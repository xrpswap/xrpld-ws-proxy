# `xrpld-ws-proxy` 技术规格说明书（SPEC.md）

## 1. 项目概述与技术约束

### 1.1 目标

实现一个高性能、高可用的 **`xrpld`（XRPL / rippled）纯 WebSocket 反向代理服务**。后端挂接一组公共或自建的 `xrpld` 节点，按节点能力打标签分类，根据客户端请求指令智能路由，并具备连接复用、防过载限流、故障熔断与状态隔离能力。同时提供可选的本地 Web 监控台与 WebSocket 测试工作台（`server.ts`）。

### 1.2 技术栈与运行环境

- **运行时**：Node.js >= 20 LTS（使用原生 ESM：`"type": "module"`）
- **核心依赖**：`ws`（处理 WebSocket 服务端与上游长连接客户端通信），代理核心模块其余均基于 Node.js 原生模块
- **协议范围**：代理核心**仅支持 WebSocket**，不实现 HTTP JSON-RPC 转发（Web 控制台仅用于运行态观测与测试）

---

## 2. 节点分类与路由规则

### 2.1 节点角色（Roles）

每个后端节点可配置一个或多个角色标签：

- `path`：路径查询节点（算力消耗大，支持 `path_find` / `ripple_path_find`）
- `history`：历史全账本节点（支持完整交易与历史账本查询）
- `submit`：交易提交节点（低延迟、高可靠广播交易）
- `normal`：普通查询节点（默认角色）

### 2.2 指令路由映射表

代理收到客户端 JSON 消息后，读取 `payload.command` 进行分类：

| 目标角色          | 匹配条件（`command`）                                                                                                                  | 说明                                                                                                                  |
| :------------ | :------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| **`path`**    | `path_find`, `ripple_path_find`                                                                                                  | 包含 `path_find` 的 `create`/`status`/`close` 订阅及单次路径询价                                                                |
| **`history`** | `tx`, `account_tx`, `tx_history`, `transaction_entry`, `nft_history`, `book_changes`                                             | 历史记录类查询；默认开启 `routeExplicitLedgerToHistory: true`：当非订阅请求携带显式 `ledger_hash` 或数字 `ledger_index` 时自动路由至此 |
| **`submit`**  | `submit`, `submit_multisigned`, `sign_and_submit`                                                                                | 所有交易上链提交指令                                                                                                          |
| **`normal`**  | 其余所有指令（如 `server_info`, `fee`, `account_info`, `account_lines`, `book_offers`, `ping`, `subscribe`, `unsubscribe`, `simulate` 等） | 兜底默认角色                                                                                                              |

---

## 3. 核心架构：动静分离的混合连接模型

为了兼顾**公共节点对单 IP 连接数的严格限制**以及 **`xrpld` 订阅状态隔离**，代理采用**“无状态池化复用 + 有状态按需独占”**的混合架构：

```text
                        ┌─▶ [无状态多路复用池] 每节点仅维持 1~2 条常驻 WS (ID 重写映射)
                        │     ▲ normal(非订阅) / history / submit / ripple_path_find
客户端 WS ──▶ [代理路由] ┤
                        └─▶ [有状态独占通道] 1对1隔离，懒创建、用完即毁
                              ▲ path_find / subscribe / unsubscribe
```

### 3.1 无状态请求：全局长连接池 + ID 重写（Multiplexing）

适用于：`history`、`submit`、`ripple_path_find` 以及除 `subscribe`/`unsubscribe` 外的 `normal` 请求。

- **连接管理**：代理全局为每个后端节点维持固定数量（默认 `poolSize: 2`）的常驻 WebSocket 连接，断线后带指数退避 + 随机抖动（Jitter，默认 `500ms ~ 15000ms`）自动重连，并定期发送 WS `ping` 保活（默认 `25000ms`）。
- **ID 重写与并发复用**：
  - 不同客户端可能使用相同的 `id`（如都传 `id: 1`），因此上游共享连接**不能透传原始 `id`**。
  - 代理维护全局递增整数 `proxyId` 与映射表 `Map<proxyId, { originalId, hasOriginalId, timer, nodeId, conn, resolve, reject }>`。
  - 发送给上游前，将请求的 `id` 替换为 `proxyId`；收到上游响应后，按 `proxyId` 查表还原 `originalId`（若客户端原请求未带 `id`，响应中移除 `id`），精准投递回对应的 `clientWs`。
  - 单请求超时定时器默认 `15000ms`，超时后清理映射、记录节点失败并向客户端返回标准 `upstreamTimeout` 错误。

### 3.2 有状态请求：1 对 1 独占通道（Stateful Isolation）

适用于：`path_find`、`subscribe`、`unsubscribe`。

- **隔离原则**：
  - 每个客户端连接（`ClientSession`）通过 `ClientStatefulManager` 按需开辟独立的上游通道（`pathChannel` 与 `subscribeChannel` 相互独立），**严禁跨客户端共用**。
  - **懒创建与及时释放**：
    - 仅当客户端发送 `path_find`（且 `subcommand !== 'close'`）时，才建立连向 `path` 节点的专用上游 WS。
    - 当客户端发送 `path_find` 且 `subcommand === 'close'` 时，将该 `close` 消息发往同一条活跃的 `pathChannel`，等待上游返回 `close` 响应后（或超时后）**立即关闭该上游 `path` 连接**并释放槽位。
    - 当客户端 WS 断开时，立即销毁其名下的所有有状态上游连接并回收槽位。
  - **异常联动**：如果已开启订阅的上游 WS 在非主动关闭状态下意外断开，代理向客户端发送 Close Code `1011`，促使客户端 SDK（如 `xrpl.js`）感知断线并触发重连重建订阅；若在建连尚未 `OPEN` 前失败，则向队列中的待发送请求返回 `upstreamUnavailable`。

---

## 4. 保护机制：限流、连接数控制与熔断

### 4.1 并发连接槽位限制（防 `path_find` / `subscribe` 过多）

- **节点级配额**：每个节点配置 `maxPathConns` 与 `maxSubscribeConns`（为 `0` 时不承接对应有状态通道）。
- **准入控制**：新建有状态通道时，优先选择健康且当前活跃对应连接数最少、未满额的节点；若所有同角色有配额节点均满额，**直接快速失败（Fail-Fast）**，向客户端返回错误码 `tooBusy`。
- **订阅 TTL 回收**：为每条 `path_find` 连接设置最大存活时间（默认 `maxPathTtlMs: 180000`，3 分钟），到期后主动向上游发送 `path_find close` 并关闭通道释放槽位。
- **客户端入口限流**：在 WS Server 入口统计来源 IP（支持通过 `TRUST_PROXY=true` 开启 `X-Forwarded-For` 解析），限制单 IP 最大并发连接数（默认 `maxConnectionsPerIp: 20`）。

### 4.2 节点级令牌桶限流（防短时间高频冲击同一节点）

- **独立令牌桶**：为每个后端节点维护一个加权令牌桶（`ratePerSec` 与 `burst`）。
- **按指令权重扣减**：
  - 权重 `1`：`ping`, `fee`, `server_info`, `server_state`, `ledger_closed`, `ledger_current`, `random`, `manifest`
  - 权重 `2`（含默认权重）：`account_info`, `account_objects`, `account_currencies`, `account_channels`, `ledger`, `ledger_entry`, `gateway_balances`, `noripple_check`, `subscribe`, `unsubscribe`, `submit`, `submit_multisigned`, `sign_and_submit`
  - 权重 `4`：`account_lines`, `account_offers`, `account_nfts`, `book_offers`, `ledger_data`, `nft_info`, `simulate`
  - 权重 `5`：`tx`, `account_tx`, `tx_history`, `transaction_entry`, `nft_history`, `book_changes`, `ripple_path_find`, `path_find`
- **溢出调度（Spillover）**：选取目标节点时，优先选择有活跃池连接、`CLOSED` 状态且**剩余令牌最多**的节点；首选节点不足时自动溢出至同角色其他有余量的健康节点。
- **有界排队**：若该角色所有节点令牌耗尽，请求进入 `BoundedRateQueue` 短暂等待（默认最多等待 `queueMaxWaitMs: 800`，队列上限 `queueMaxLength: 200`）；超时或队列满则直接返回 `slowDown` 错误。

### 4.3 自适应背压与熔断（Circuit Breaker）

- **主动嗅探 `xrpld` 负载信号**：拦截上游返回内容，若出现以下任一情况：
  - 返回 JSON 中包含 `warning: "load"`
  - 返回错误为 `tooBusy`、`slowDown`、`highLoad`
  - 连接异常断开或连续超时
- **动作**：
  1. 触发**自适应降速**：将该节点的令牌生成速率减半（最低降至 `0.25x`），持续 `slowDownRecoveryMs`（默认 `15000ms`）后自动恢复。
  2. 若连续失败次数达阈值（默认 `circuitBreakerThreshold: 3`），将该节点状态切换为 `OPEN`（熔断冷却 `nodeCooldownMs: 30000`），冷却期内不分配流量；期满自动进入 `HALF_OPEN` 半开探测，成功后恢复 `CLOSED`。

### 4.4 高频只读请求合并 / 微缓存

- 通过 `ReadCache`（`src/cache.js`）对 `fee`、`server_info`、`ledger_closed` 实现 **Singleflight（同参数在途请求合并）** 与 **1500ms 微缓存**。
- 缓存与合并结果自动剥离共享 `id`，并按每个调用方原始请求还原各自 `id`（含 `id: 0` 与无 `id`），且**不缓存错误响应**（`status === 'error'`）。

---

## 5. 标准化错误响应规范

所有由代理主动拦截或上游失败产生的错误，均包装为符合 XRPL WebSocket 规范的 JSON 响应：

```json
{
  "id": "<原样返回客户端请求的 id；原请求无 id 时省略该字段>",
  "status": "error",
  "type": "response",
  "error": "<错误码: jsonInvalid | noNodesAvailable | tooBusy | slowDown | upstreamTimeout | upstreamUnavailable>",
  "error_message": "<清晰可读的错误原因描述>"
}
```

---

## 6. 默认节点与配置结构（`src/config.js`）

### 6.1 默认上游节点清单

当前版本内置以下 4 个默认公共 `xrpld` 上游节点配置：

| 节点 ID | WebSocket 地址 | 角色标签 (`roles`) | 复用池 (`poolSize`) | `maxPathConns` | `maxSubscribeConns` | 令牌桶 (`ratePerSec` / `burst`) |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: |
| **`xrplcluster`** | `wss://xrplcluster.com/` | `normal`, `history`, `path`, `submit` | `2` | `15` | `30` | `15 / 30` |
| **`ripple-s1`** | `wss://s1.ripple.com/` | `normal`, `path`, `submit` | `2` | `15` | `30` | `15 / 30` |
| **`ripple-s2-history`** | `wss://s2.ripple.com/` | `history` | `2` | `0` | `0` | `10 / 20` |
| **`honeycluster`** | `wss://honeycluster.io/` | `normal`, `history` | `2` | `0` | `20` | `15 / 30` |

### 6.2 完整配置 Schema（`src/config.js`）

可通过 `PROXY_HOST`、`PROXY_PORT`、`TRUST_PROXY` 环境变量或 `createConfig(overrides)` 覆盖默认配置：

```javascript
export const config = {
  server: {
    host: process.env.PROXY_HOST || '0.0.0.0',
    port: Number(process.env.PROXY_PORT || 8080),
    maxPayloadBytes: 1024 * 1024, // 1MB
    maxConnectionsPerIp: 20,
    trustProxy: process.env.TRUST_PROXY === 'true',
    heartbeatIntervalMs: 30000,
    statsLogIntervalMs: 60000,
  },
  limits: {
    requestTimeoutMs: 15000,
    nodeCooldownMs: 30000,
    maxPathTtlMs: 180000, // 3 分钟
    queueMaxWaitMs: 800,
    queueMaxLength: 200,
    circuitBreakerThreshold: 3,
    slowDownRecoveryMs: 15000,
    upstreamPingIntervalMs: 25000,
    reconnectBaseDelayMs: 500,
    reconnectMaxDelayMs: 15000,
  },
  routing: {
    pathCommands: ['path_find', 'ripple_path_find'],
    historyCommands: [
      'tx',
      'account_tx',
      'tx_history',
      'transaction_entry',
      'nft_history',
      'book_changes',
    ],
    submitCommands: ['submit', 'submit_multisigned', 'sign_and_submit'],
    statefulCommands: ['path_find', 'subscribe', 'unsubscribe'],
    routeExplicitLedgerToHistory: true,
    defaultWeight: 2,
    commandWeights: { /* 见 4.2 权重分级 */ },
  },
  cache: {
    enabled: true,
    ttlMs: 1500,
    commands: ['fee', 'server_info', 'ledger_closed'],
  },
  nodes: [
    {
      id: 'xrplcluster',
      wsUrl: 'wss://xrplcluster.com/',
      roles: ['normal', 'history', 'path', 'submit'],
      poolSize: 2,
      maxPathConns: 15,
      maxSubscribeConns: 30,
      rateLimit: { ratePerSec: 15, burst: 30 },
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

## 7. 目录结构、运行方式与模块职责

```text
xrpld-ws-proxy/
├── package.json
├── SPEC.md
├── server.ts                # 可选 Web 控制台入口 (port 3000)：Express + Vite + /ws 代理 + 本地 Mock 集群
├── src/
│   ├── index.js             # 独立代理服务入口：启动 WS 服务 (默认 8080)、优雅关闭 (SIGINT/SIGTERM)、定时状态日志
│   ├── config.js            # 集中配置、默认节点清单、指令权重与路由表
│   ├── router.js            # 解析消息、分类 role、判断 stateful/stateless、计算指令权重、标准错误构造
│   ├── rateLimiter.js       # 令牌桶算法（加权扣减、自适应降速）与有界等待队列 (BoundedRateQueue)
│   ├── nodeManager.js       # 节点健康状态、熔断冷却、槽位计数、溢出调度选路与负载嗅探
│   ├── statelessPool.js     # 无状态全局连接池 + proxyId 重写/还原映射 + 超时清理
│   ├── statefulChannel.js   # 有状态独占通道 (path_find / subscribe) + TTL 自动回收
│   ├── cache.js             # Singleflight 与微缓存 (fee / server_info / ledger_closed)
│   ├── wsServer.js          # 客户端连接管理、IP 限流、心跳、分发至 Pool 或 Channel（支持独立端口或挂载 httpServer）
│   └── App.tsx              # Web 实时监控控制台与 WS 调试工作台
└── test/
    ├── router.test.js       # 路由、边界 ID 与权重单元测试
    ├── rateLimiter.test.js  # 令牌桶、自适应降速与有界队列测试
    └── proxy.e2e.test.js    # Mock 上游 WS 端到端集成测试（覆盖 6 大验收场景 + Singleflight/负载嗅探/1011 联动）
```

常用启动命令：
- `npm run proxy`（或 `node src/index.js`）：启动独立纯净版 `xrpld-ws-proxy`（默认监听 `0.0.0.0:8080`）。
- `npm run dev`（`tsx server.ts`）：启动带 Web 监控台与 `/ws` 本地模拟集群的调试环境（监听 `3000` 端口）。
- `npm test`（`node --test test/*.test.js`）：运行全部单元与 E2E 验收测试。

---

## 8. 关键实现避坑与监听器防泄漏设计

1. **客户端 `id` 的边界值**：客户端可能传 `id: 0`（Falsy 值）、`id: ""`、字符串 `id: "req-1"`，甚至不传 `id`。统一通过 `payload.id !== undefined && payload.id !== null` 判定是否存在 `id`，严禁使用 `if (payload.id)`。
2. **连接尚未 `OPEN` 时的消息排队**：独占通道或无状态池化连接处于 `CONNECTING` 状态期间收到的消息均进入各自 `outboundQueue` 缓冲队列，待 `open` 事件触发后按 FIFO 顺序发出，不丢包不报错。
3. **定时器与事件监听器防泄漏（含高并发/重启加固）**：
   - 重连/销毁上游 `WebSocket` 时清理所有 `setTimeout` / `setInterval` 定时器与 `pendingMap`，并在 `close` 回调执行 `ws.removeAllListeners()` 彻底释放旧 socket 闭包引用。
   - `ProxyServer.start()` 具备幂等防重入保护；客户端断开时 `cleanupSession` 显式解绑 `pong`、`message`、`close`、`error` 监听器；`shutdown()` 完成后清理 `wss.removeAllListeners()` 并从挂载的 `httpServer` 解绑 `upgrade` 处理器。
   - `ClientStatefulManager` 的 `onClosed` 回调采用实例引用比对（`if (this.pathChannel === ch)` / `if (this.subscribeChannel === ch)`），防止旧通道延迟关闭时误清空新建通道的引用。
   - `server.ts` 中的 Vite 中间件配置 `hmr: false, ws: false`，避免额外创建冗余的内部 WebSocket Server；`shutdown()` 具备 `isShuttingDown` 防重入锁，避免重复信号在 `httpServer` 上累积 `close` 回调。
4. **`path_find` 关闭语义**：收到 `{ command: "path_find", subcommand: "close" }` 时，必须路由到当前活跃的同一条上游 `pathChannel` 连接上，等待上游返回 `close` 响应（或超时）后再销毁该连接并释放槽位。
5. **优雅关闭（Graceful Shutdown）**：收到 `SIGINT`/`SIGTERM` 时停止接收新请求，向所有客户端发送 WS `1001` 关闭帧，清理全部定时器、等待队列及上游连接后安全退出。

---

## 9. 验收测试清单（Acceptance Criteria）

使用 Node.js 内置 `node:test` 与 `node --trace-warnings --test test/*.test.js` 验证以下全部场景（当前 **14/14** 测试通过且无警告）：

1. **指令分流准确**：分别发送 `server_info`、`account_tx`、`submit`、`ripple_path_find`、`path_find`，验证请求准确落入对应 `roles` 的节点（含 `id: 0` 与无 `id` 边界）。
2. **ID 复用不串包**：多个客户端同时向无状态池发送 `id: 1` 的不同查询，各自收到对应结果且 `id` 均准确还原。
3. **`path_find` 严格隔离**：客户端 A 与 B 同时创建 `path_find`，各自持有独立上游连接与推送流；A 发送 `subcommand: "close"` 后仅释放 A 的上游连接与槽位，B 不受影响。
4. **槽位上限保护**：设置 `maxPathConns` 上限后，超限客户端发起 `path_find create` 时代理直接快速失败返回 `error: "tooBusy"`，不向后端建连。
5. **限流与溢出调度**：节点 A 令牌耗尽后流量自动溢出至同角色节点 B；全部耗尽后触发有界排队与 `slowDown` 快速拒绝。
6. **故障转移与熔断**：关闭某个上游节点后，代理自动切换至同角色健康节点，将故障节点标记为 `OPEN` 熔断冷却并输出对应日志。
7. **增强机制验证**：`ReadCache` Singleflight 合并并发 `fee` 请求并正确还原调用方各自 `id`；上游返回 `warning: "load"` 时令牌补充速率减半；订阅上游异常断开时向客户端发送 `1011` 关闭帧。

---

## 10. Web 管理控制台与调试接口（`server.ts` + `src/App.tsx`）

除独立 CLI 代理入口（`src/index.js`）外，项目还提供了一套运行于 `3000` 端口的 **Web 实时监控控制台与交互调试工作台**（通过 `npm run dev` 或 `npm start` 启动），便于在无外网或沙盒环境下直接验证代理路由、熔断、限流与有状态隔离能力。

### 10.1 服务端运行架构（`server.ts`）

- **HTTP + Vite 中间件**：使用 Express 与 `http.createServer` 监听 `0.0.0.0:3000`；开发模式挂载 Vite 中间件（`middlewareMode: true, hmr: false, ws: false`），生产模式（`NODE_ENV=production`）静态托管 `dist/`。
- **内置本地模拟节点集群**：启动时自动在 `127.0.0.1` 随机空闲端口拉起 4 个模拟 `xrpld` WebSocket 节点，角色与配额完全镜像 `src/config.js` 的默认节点（`xrplcluster`、`ripple-s1`、`ripple-s2-history`、`honeycluster`），支持：
  - `server_info`、`fee`、`ledger_closed`、`account_tx`、`submit` 等标准指令响应；
  - `path_find`（`create` / `close`）与 `subscribe` / `unsubscribe` 定时异步推送流；
  - 单次注入 `warning: "load"` 负载信号模拟与节点在线/离线启停控制。
- **挂载 `/ws` 代理端点**：将核心 `ProxyServer` 实例通过 `{ httpServer, path: '/ws' }` 模式挂载到同一 `3000` 端口的 `/ws` 路径上。

### 10.2 管理控制台 HTTP API

| 方法与路径 | 说明 | 请求体 / 返回内容 |
| :--- | :--- | :--- |
| `GET /api/stats` | 获取实时运行态指标 | 返回客户端连接数、IP 数、全局复用池 `pendingRequests`/`nextProxyId`、微缓存命中/合并次数、各节点状态/槽位/令牌桶/QPS，以及当前 `routing` 与 `limits` 配置 |
| `POST /api/nodes/:id/toggle-online` | 模拟上游节点宕机或恢复 | 停止或重启对应 Mock `xrpld` 节点；重启后自动将该节点熔断器复位为 `CLOSED` |
| `POST /api/nodes/:id/circuit` | 手动切换节点熔断状态 | 请求体 `{ "state": "OPEN" | "CLOSED" }`，强制触发熔断或手动恢复 |
| `POST /api/nodes/:id/load-warning` | 注入过载警告信号 | 标记对应 Mock 节点在下次响应中携带 `warning: "load"`，用于测试自适应降速 |
| `POST /api/run-tests` | 在线运行验收测试套件 | 调用 `node --test test/router.test.js test/rateLimiter.test.js test/proxy.e2e.test.js`，返回 `{ ok, output }` |

### 10.3 前端页面模块（`src/App.tsx`）

界面采用侧边栏导航 + 顶部指标概览条（每 `1500ms` 自动轮询 `/api/stats`），包含 4 个功能视图：

1. **Node Topology（节点拓扑与池监控）**
   - 展示汇总 KPI：健康节点数、活跃客户端连接与总 QPS、复用池在途请求/排队长度、微缓存命中与 Singleflight 合并数。
   - 节点明细表实时显示每个节点的角色标签、熔断器状态（`CLOSED` / `OPEN` / `HALF_OPEN` / `OFFLINE` 及冷却剩余秒数）、连接池 WS 数、`path_find` 与 `subscribe` 槽位占用、当前令牌数与自适应速率、总请求/错误/QPS。
   - 每行提供故障演练按钮：**Stop / Start**（模拟节点断网）、**Trip / Reset**（手动熔断/复位）、**Load**（注入 `warning: "load"`）。
2. **WS Workbench（WebSocket 交互调试台）**
   - 直接在浏览器连接 `/ws` 代理端点，内置 11 种预设 XRPL 指令模板（覆盖 `server_info`、`fee`、`account_tx`、`submit`、`ripple_path_find`、`path_find create/close`、`subscribe/unsubscribe`、`id: 0` 边界及显式 `ledger_index` 切入 `history`）。
   - 提供 **Test Concurrent `id: 1`**（同时开启两条客户端 WS 发送相同 `id: 1` 请求验证不串包）与 **Burst 12x**（连发 12 条高权重 `book_offers` 演示令牌溢出调度）一键压测按钮。
   - 右侧帧检视器实时记录所有 `OUTBOUND` / `INBOUND` / `SYSTEM` 帧内容及端到端往返耗时（ms）。
3. **Routing & Config（路由规则与保护参数）**
   - 只读展示当前生效的角色指令映射列表（`path` / `history` / `submit` / `stateful`）、`routeExplicitLedgerToHistory` 开关状态及 `limits` 全量限流/超时/熔断阈值。
4. **Acceptance Suite（验收测试执行器）**
   - 一键触发后端 `/api/run-tests`，实时执行完整 `node --test` 套件并展示 TAP 通过状态与原始输出日志。

