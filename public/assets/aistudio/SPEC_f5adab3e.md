# `xrpld-ws-proxy` 技术规格说明书（SPEC.md）

## 1. 项目概述与技术约束

### 1.1 目标

实现一个高性能、高可用的 **`xrpld`（XRPL / rippled）纯 WebSocket 反向代理服务**。后端挂接一组公共或自建的 `xrpld` 节点，按节点能力打标签分类，根据客户端请求指令智能路由，并具备连接复用、防过载限流、故障熔断与状态隔离能力。

### 1.2 技术栈与运行环境

- **运行时**：Node.js >= 20 LTS（使用原生 ESM：`"type": "module"`）
- **核心依赖**：最小化依赖（推荐仅使用 `ws` 库处理 WebSocket 服务端与客户端通信，其余使用 Node.js 原生模块）
- **协议范围**：**仅支持 WebSocket**，不实现 HTTP JSON-RPC 转发

---

## 2. 节点分类与路由规则

### 2.1 节点角色（Roles）

每个后端节点可配置一个或多个角色标签：

- `path`：路径查询节点（算力消耗大，支持 `path_find`）
- `history`：历史全账本节点（支持完整交易与历史查询）
- `submit`：交易提交节点（低延迟、高可靠广播交易）
- `normal`：普通查询节点（默认角色）

### 2.2 指令路由映射表

代理收到客户端 JSON 消息后，读取 `payload.command` 进行分类：

| 目标角色          | 匹配条件（`command`）                                                                                                                  | 说明                                                                                                 |
| :------------ | :------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------- |
| **`path`**    | `path_find`, `ripple_path_find`                                                                                                  | 包含 `path_find` 的 `create`/`status`/`close` 订阅及单次路径询价                                               |
| **`history`** | `tx`, `account_tx`, `tx_history`, `transaction_entry`, `nft_history`, `book_changes`                                             | 历史记录类查询；可选开关：当 `ledger`/`ledger_data`/`account_info` 等携带显式 `ledger_hash` 或数字 `ledger_index` 时也路由至此 |
| **`submit`**  | `submit`, `submit_multisigned`, `sign_and_submit`                                                                                | 所有交易上链提交指令                                                                                         |
| **`normal`**  | 其余所有指令（如 `server_info`, `fee`, `account_info`, `account_lines`, `book_offers`, `ping`, `subscribe`, `unsubscribe`, `simulate` 等） | 兜底默认角色                                                                                             |

---

## 3. 核心架构：动静分离的混合连接模型

为了兼顾**公共节点对单 IP 连接数的严格限制**以及 **`xrpld` 订阅状态隔离**，代理必须采&#x7528;**“无状态池化复用 + 有状态按需独占”**&#x7684;混合架构：

```text
                        ┌─▶ [无状态多路复用池] 每节点仅维持 1~2 条常驻 WS (ID 重写映射)
                        │     ▲ normal(非订阅) / history / submit / ripple_path_find
客户端 WS ──▶ [代理路由] ┤
                        └─▶ [有状态独占通道] 1对1隔离，懒创建、用完即毁
                              ▲ path_find / subscribe / unsubscribe
```

### 3.1 无状态请求：全局长连接池 + ID 重写（Multiplexing）

适用于：`history`、`submit`、`ripple_path_find` 以及除 `subscribe`/`unsubscribe` 外的 `normal` 请求。

- **连接管理**：代理全局为每个后端节点维持固定数量（如 `1~2` 条）的常驻 WebSocket 连接，断线后带随机抖动（Jitter）自动重连，并定期发送 WS `ping` 保活。
- **ID 重写与并发复用**：
  - 不同客户端可能使用相同的 `id`（如都传 `id: 1`），因此上游共享连接**不能透传原始 `id`**。
  - 代理维护全局递增整数 `proxyId` 与映射表 `Map<proxyId, { clientWs, originalId, hasOriginalId, timer, nodeId }>`。
  - 发送给上游前，将请求的 `id` 替换为 `proxyId`；收到上游响应后，按 `proxyId` 查表还原 `originalId`（若客户端原请求未带 `id`，响应中移除 `id`），精准投递回对应的 `clientWs`。
  - 必须设置单请求超时定时器（如 `15000ms`），超时后清理映射并向客户端返回标准 XRPL 超时错误。

### 3.2 有状态请求：1 对 1 独占通道（Stateful Isolation）

适用于：`path_find`、`subscribe`、`unsubscribe`。

- **为什么不能进共享池**：
  1. `xrpld` 原生规定**单条 WebSocket 只能维持 1 个 `path_find` 订阅**，第二个 `create` 会直接覆盖第一个。
  2. `path_find` 和 `subscribe` 的异步推送消息**不包含请求 `id`**，无法通过 `id` 映射分发。
- **隔离原则**：
  - 每个客户端连接（`ClientSession`）按需为自己开辟独立的上游通道（`path` 独占通道、`subscribe` 独占通道），**严禁跨客户端共用**。
  - **懒创建与及时释放**：
    - 仅当客户端发送 `path_find`（且 `subcommand !== 'close'`）时，才建立连向 `path` 节点的专用上游 WS。
    - 当客户端发送 `path_find` 且 `subcommand === 'close'` 时，等待上游返回该 `close` 响应后（或超时后），**立即关闭该上游 `path` 连接**释放名额。
    - 当客户端 WS 断开时，立即销毁其名下的所有有状态上游连接。
  - **异常联动**：如果某个已开启订阅的上游 WS 意外断开，代理应向客户端发送关闭帧（如 Close Code `1011`），促使客户端 SDK（如 `xrpl.js`）感知断线并触发重连重建订阅。

---

## 4. 保护机制：限流、连接数控制与熔断

公共 `xrpld` 节点内置了 `ResourceManager`（按 IP 算力加权和连接数限制），代理层必须实现以下四重保护：

### 4.1 并发连接槽位限制（防 `path_find` 过多）

- **节点级配额**：每个 `path` 节点配置 `maxPathConns`（例如 `15`），每个 `normal` 节点可配置 `maxSubscribeConns`（例如 `30`）。
- **准入控制**：新建 `path_find` 通道时，优先选择当前活跃 `path` 连接数最少且未满的节点。若所有 `path` 节点均满额，**直接快速失败（Fail-Fast）**，向客户端返回错误码 `tooBusy`。
- **订阅 TTL 回收**：为每条 `path_find` 连接设置最大存活时间（如 `maxPathTtlMs: 180000`，3 分钟）或无活动超时。到期后主动发送 `close` 并释放槽位，防止前端页面挂机耗尽连接。
- **客户端入口限流**：在 WS Server 入口统计来源 IP（兼容 `X-Forwarded-For` 可选配置），限制单 IP 最大并发连接数（如 `maxConnectionsPerIp: 20`）。

### 4.2 节点级令牌桶限流（防短时间高频冲击同一节点）

- **独立令牌桶**：为每个后端节点维护一个令牌桶（`ratePerSec` 与 `burst`）。
- **按指令权重扣减**：不同指令消耗不同令牌（可配置），例如：
  - 轻量指令（`ping`, `fee`, `server_info`）：消耗 `1` 个令牌
  - 普通查询（`account_info`, `ledger`）：消耗 `2` 个令牌
  - 重量级指令（`account_tx`, `tx_history`, `book_offers`, `ripple_path_find`）：消耗 `4~5` 个令牌
- **溢出调度（Spillover）**：选取目标节点时，优先选**健康且剩余令牌最多**的节点；若首选节点令牌不足，自动尝试同角色其他有令牌的节点。
- **有界排队**：若该角色**所有**节点令牌均耗尽，允许请求在内存队列短暂等待（如最多等待 `800ms`，队列上限 `200` 条）；超时或队列满则直接返回 `slowDown` 错误，严禁无限制堆积内存。

### 4.3 自适应背压与熔断（Circuit Breaker）

- **主动嗅探 `xrpld` 负载信号**：拦截上游返回内容，若出现以下任一情况：
  - 返回 JSON 中包含 `warning: "load"`
  - 返回错误为 `tooBusy`、`slowDown`、`highLoad`
  - 连接异常断开或连续超时
- **动作**：
  1. 触发**自适应降速**：将该节点的令牌生成速率临时减半（持续一段时间后逐步恢复）。
  2. 若连续失败次数达阈值（如 3 次），将该节点标记为**熔断冷却**（如 `nodeCooldownMs: 30000`），冷却期内不分配新流量，期满自动半开探测恢复。

### 4.4 高频只读请求合并 / 微缓存（可选增强）

- 对 `fee`、`server_info`、`ledger_closed` 等全局一致的只读查询，实现 **Singleflight（同参数在途请求合并）** 或 **1~2 秒微缓存**，降低并发连接初始化时的重复请求。

---

## 5. 标准化错误响应规范

所有由代理主动拦截或上游失败产生的错误，**必须包装为符合 XRPL WebSocket 规范的 JSON 响应**，确保前端 `xrpl.js` 等 SDK 能正常捕获而不报错崩溃：

```json
{
  "id": "<原样返回客户端请求的 id>",
  "status": "error",
  "type": "response",
  "error": "<错误码: jsonInvalid | noNodesAvailable | tooBusy | slowDown | upstreamTimeout | upstreamUnavailable>",
  "error_message": "<清晰可读的错误原因描述>"
}
```

---

## 6. 建议配置结构（`src/config.js` Schema）

```javascript
export const config = {
  server: {
    host: '0.0.0.0',
    port: 8080,
    maxPayloadBytes: 1024 * 1024, // 1MB
    maxConnectionsPerIp: 20,
    heartbeatIntervalMs: 30000,
    statsLogIntervalMs: 60000, // 定期在控制台打印各节点连接数、QPS、熔断状态
  },
  limits: {
    requestTimeoutMs: 15000,
    nodeCooldownMs: 30000,
    maxPathTtlMs: 180000,       // 单条 path_find 最长存活 3 分钟
    queueMaxWaitMs: 800,
    queueMaxLength: 200,
  },
  nodes: [
    {
      id: 'xrplcluster',
      wsUrl: 'wss://xrplcluster.com/',
      roles: ['normal', 'history', 'path', 'submit'],
      poolSize: 2,              // 无状态多路复用池长连接数
      maxPathConns: 15,         // 允许同时挂起的 path_find 最大连接数
      maxSubscribeConns: 30,    // 允许同时挂起的 subscribe 最大连接数
      rateLimit: {
        ratePerSec: 15,         // 每秒补充令牌数
        burst: 30,              // 桶容量
      },
    },
    {
      id: 'ripple-s1',
      wsUrl: 'wss://s1.ripple.com/',
      roles: ['normal', 'path', 'submit'],
      poolSize: 2,
      maxPathConns: 15,
      maxSubscribeConns: 30,
      rateLimit: { ratePerSec: 15, burst: 30 },
    },
    {
      id: 'ripple-s2-history',
      wsUrl: 'wss://s2.ripple.com/',
      roles: ['history'],
      poolSize: 2,
      maxPathConns: 0,
      maxSubscribeConns: 0,
      rateLimit: { ratePerSec: 10, burst: 20 },
    },
    {
      id: 'honeycluster',
      wsUrl: 'wss://honeycluster.io/',
      roles: ['normal', 'history'],
      poolSize: 2,
      maxPathConns: 0,
      maxSubscribeConns: 20,
      rateLimit: { ratePerSec: 15, burst: 30 },
    },
  ],
};
```

---

## 7. 推荐目录结构与模块职责

```text
xrpld-ws-proxy/
├── package.json
├── SPEC.md
├── src/
│   ├── index.js             # 入口：启动 WS 服务、优雅关闭 (SIGINT/SIGTERM)、定时状态日志
│   ├── config.js            # 集中配置与指令权重/路由表
│   ├── router.js            # 解析消息、分类 role、判断 stateful/stateless、计算指令权重
│   ├── rateLimiter.js       # 令牌桶算法实现（支持加权扣减、自适应降速、短暂排队）
│   ├── nodeManager.js       # 节点健康状态、熔断冷却、槽位计数、按最优负载挑选节点
│   ├── statelessPool.js     # 无状态全局连接池 + proxyId 重写/还原映射 + 超时清理
│   ├── statefulChannel.js   # 有状态独占通道 (path_find / subscribe) + TTL 自动回收
│   ├── cache.js             # (可选) Singleflight 与微缓存
│   └── wsServer.js          # 客户端连接管理、IP 限流、心跳、分发至 Pool 或 Channel
└── test/
    ├── router.test.js       # 路由与权重单元测试
    ├── rateLimiter.test.js  # 令牌桶与排队边界测试
    └── proxy.e2e.test.js    # Mock 上游 WS 服务器的端到端集成测试（重点测并发 ID 映射与 path_find 隔离）
```

---

## 8. 关键实现避坑指南（编码需特别注意）

1. **客户端 `id` 的边界值**：客户端可能传 `id: 0`（Falsy 值）、字符串 `id: "req-1"`，甚至完全不传 `id`。判断是否存在 `id` 必须用 `payload.id !== undefined && payload.id !== null`，切忌用 `if (payload.id)`。
2. **连接尚未 `OPEN` 时的消息排队**：当独占通道刚创建处于 `CONNECTING` 状态，客户端紧接着发来的第 2 条消息（或池化连接断线重连瞬间发来的请求）必须进入缓冲队列，等 `open` 事件后再顺序发出，不能丢包或抛错。
3. **事件监听防泄漏**：重连/销毁上游 `WebSocket` 时，务必清理所有 `setTimeout` 定时器以及待响应的 `pendingMap`，避免 Node.js 内存泄漏。
4. **`path_find` 关闭语义**：当客户端发 `{ command: "path_find", subcommand: "close" }` 时，这条 `close` 请求本身必须发到刚才那条正在跑 `path_find` 的同一条上游连接上，等收到上游回复后再销毁连接；不能发去另一台节点或直接丢弃。
5. **优雅关闭（Graceful Shutdown）**：收到 `SIGINT`/`SIGTERM` 时，停止接收新请求，给客户端发 WS `1001` 关闭帧，关闭所有定时器和上游连接后安全退出进程。

---

## 9. 验收测试清单（Acceptance Criteria）

开发完成后，需使用 Node.js 内置 `node:test` 跑通以下 Mock 测试场景：

1. **指令分流准确**：分别发送 `server_info`、`account_tx`、`submit`、`path_find`，Mock 后端验证请求分别落到了对应 `roles` 的节点上。
2. **ID 复用不串包**：两个客户端同时向无状态池发送 `id: 1` 的不同查询，各自收到正确对应的结果且 `id` 均还原为 `1`。
3. **`path_find` 严格隔离**：客户端 A 和客户端 B 同时创建 `path_find`，Mock 节点收到两条独立的上游连接，各自只收到自己的推送；客户端 A 发送 `subcommand: "close"` 后，对应上游连接关闭、槽位释放，客户端 B 不受影响。
4. **槽位上限保护**：将 `maxPathConns` 设为 `1`，第 2 个客户端发起 `path_find create` 时，代理直接拦截并返回 `error: "tooBusy"`，不向后端建连。
5. **限流与溢出调度**：将节点 A 速率设为极小，连续发请求时，流量自动溢出到同角色节点 B；全部耗尽后触发本地排队与 `slowDown` 快速拒绝。
6. **故障转移与熔断**：关闭某个上游 Mock 节点，代理自动切换同角色备用节点，将该节点标记熔断并在日志中体现。
