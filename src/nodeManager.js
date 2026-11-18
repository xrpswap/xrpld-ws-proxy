import { config as defaultConfig } from './config.js';
import { TokenBucket, BoundedRateQueue } from './rateLimiter.js';

const OVERLOAD_ERRORS = new Set(['tooBusy', 'slowDown', 'highLoad']);

/**
 * Manages backend xrpld node health, circuit breakers, stateful connection slots,
 * token-bucket rate limiters, spillover selection, and rolling QPS telemetry.
 */
export class NodeManager {
  /**
   * @param {ReturnType<typeof import('./config.js').createConfig>} [cfg]
   * @param {{ logger?: Console }} [options]
   */
  constructor(cfg = defaultConfig, options = {}) {
    this.config = cfg;
    this.logger = options.logger || console;
    this.limits = cfg.limits || defaultConfig.limits;
    this.nodes = new Map();
    this.rateQueue = new BoundedRateQueue(
      {
        maxWaitMs: this.limits.queueMaxWaitMs ?? 800,
        maxLength: this.limits.queueMaxLength ?? 200,
      },
      (role, weight) => this.trySelectStatelessNode(role, weight),
    );

    for (const nodeCfg of cfg.nodes || []) {
      this.nodes.set(nodeCfg.id, {
        id: nodeCfg.id,
        wsUrl: nodeCfg.wsUrl,
        roles: new Set(nodeCfg.roles || ['normal']),
        poolSize: nodeCfg.poolSize ?? 2,
        maxPathConns: nodeCfg.maxPathConns ?? 0,
        maxSubscribeConns: nodeCfg.maxSubscribeConns ?? 0,
        tokenBucket: new TokenBucket({
          ratePerSec: nodeCfg.rateLimit?.ratePerSec ?? 15,
          burst: nodeCfg.rateLimit?.burst ?? 30,
        }),
        activePathConns: 0,
        activeSubscribeConns: 0,
        poolConnectedCount: 0,
        state: 'CLOSED', // 'CLOSED' | 'OPEN' | 'HALF_OPEN'
        consecutiveFailures: 0,
        cooldownUntil: 0,
        cooldownTimer: null,
        totalRequests: 0,
        totalErrors: 0,
        totalTimeouts: 0,
        loadWarnings: 0,
        recentTimestamps: [],
        lastError: null,
      });
    }
  }

  getNode(nodeId) {
    return this.nodes.get(nodeId);
  }

  getAllNodes() {
    return Array.from(this.nodes.values());
  }

  setPoolConnectedCount(nodeId, count) {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.poolConnectedCount = Math.max(0, count);
  }

  /**
   * Evaluate and potentially transition OPEN -> HALF_OPEN when cooldown expires.
   * @param {any} node
   * @param {number} [now]
   */
  isNodeHealthy(node, now = Date.now()) {
    if (node.state === 'OPEN') {
      if (now >= node.cooldownUntil) {
        node.state = 'HALF_OPEN';
        this.logger.info?.(`[NodeManager] Circuit breaker HALF_OPEN for node "${node.id}"`);
        return true;
      }
      return false;
    }
    return true;
  }

  /**
   * Non-blocking attempt to pick the best healthy node with enough tokens for `role`.
   * Implements spillover across same-role nodes by picking highest remaining tokens.
   * @param {string} role
   * @param {number} weight
   * @returns {{ node?: any, waitNeeded?: boolean, noNodes?: boolean }}
   */
  trySelectStatelessNode(role, weight = 1) {
    const now = Date.now();
    const roleNodes = [];
    for (const node of this.nodes.values()) {
      if (node.roles.has(role)) {
        roleNodes.push(node);
      }
    }

    if (roleNodes.length === 0) {
      return { noNodes: true };
    }

    const healthyNodes = roleNodes.filter((n) => this.isNodeHealthy(n, now));
    if (healthyNodes.length === 0) {
      return { noNodes: true };
    }

    // Prefer nodes that have active open pool connections; fall back to any healthy node only if none are connected yet.
    const connectedHealthy = healthyNodes.filter((n) => n.poolConnectedCount > 0);
    const candidatePool = connectedHealthy.length > 0 ? connectedHealthy : healthyNodes;

    // Sort candidates: CLOSED before HALF_OPEN, then highest available tokens.
    const withTokens = candidatePool
      .map((n) => ({
        node: n,
        tokens: n.tokenBucket.getAvailableTokens(now),
        canConsume: n.tokenBucket.canConsume(weight, now),
      }))
      .filter((item) => item.canConsume)
      .sort((a, b) => {
        if (a.node.state !== b.node.state) {
          return a.node.state === 'CLOSED' ? -1 : 1;
        }
        return b.tokens - a.tokens;
      });

    if (withTokens.length > 0) {
      const chosen = withTokens[0].node;
      if (chosen.tokenBucket.tryConsume(weight, now)) {
        this.recordRequestTimestamp(chosen, now);
        return { node: chosen };
      }
    }

    // All healthy candidates have exhausted their token buckets.
    return { waitNeeded: true };
  }

  /**
   * Select a node for a stateless request, waiting in bounded queue if all tokens are exhausted.
   * @param {string} role
   * @param {number} weight
   */
  async acquireStatelessNode(role, weight = 1) {
    const attempt = this.trySelectStatelessNode(role, weight);
    if (attempt.node) {
      return attempt.node;
    }
    if (attempt.noNodes) {
      throw Object.assign(
        new Error(`No healthy upstream nodes available for role "${role}"`),
        { code: 'noNodesAvailable' },
      );
    }
    return this.rateQueue.enqueue(role, weight);
  }

  /**
   * Reserve a 1-to-1 stateful channel slot (`path_find` or `subscribe`).
   * Picks healthy node with fewest active slots for that channel type.
   * Fail-fast with `tooBusy` if all eligible nodes are at max capacity.
   * @param {'path' | 'normal'} role
   * @param {'path_find' | 'subscribe'} channelType
   */
  acquireStatefulSlot(role, channelType) {
    const now = Date.now();
    const isPath = channelType === 'path_find';
    const maxKey = isPath ? 'maxPathConns' : 'maxSubscribeConns';
    const activeKey = isPath ? 'activePathConns' : 'activeSubscribeConns';

    const roleNodes = [];
    for (const node of this.nodes.values()) {
      if (node.roles.has(role) && node[maxKey] > 0) {
        roleNodes.push(node);
      }
    }

    if (roleNodes.length === 0) {
      return {
        ok: false,
        errorCode: 'noNodesAvailable',
        errorMessage: `No nodes configured with role "${role}" and ${maxKey} > 0.`,
      };
    }

    const healthyNodes = roleNodes.filter((n) => this.isNodeHealthy(n, now));
    if (healthyNodes.length === 0) {
      return {
        ok: false,
        errorCode: 'noNodesAvailable',
        errorMessage: `All "${role}" nodes are currently in circuit-breaker cooldown.`,
      };
    }

    const available = healthyNodes
      .filter((n) => n[activeKey] < n[maxKey])
      .sort((a, b) => {
        if (a[activeKey] !== b[activeKey]) {
          return a[activeKey] - b[activeKey];
        }
        return b.tokenBucket.getAvailableTokens(now) - a.tokenBucket.getAvailableTokens(now);
      });

    if (available.length === 0) {
      return {
        ok: false,
        errorCode: 'tooBusy',
        errorMessage: `All "${role}" nodes reached maximum concurrent ${channelType} connections.`,
      };
    }

    const chosen = available[0];
    chosen[activeKey] += 1;

    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      chosen[activeKey] = Math.max(0, chosen[activeKey] - 1);
    };

    return {
      ok: true,
      node: chosen,
      release,
    };
  }

  /**
   * Sniff upstream JSON responses for xrpld ResourceManager load signals.
   * @param {string} nodeId
   * @param {Record<string, any>} msg
   */
  inspectUpstreamResponse(nodeId, msg) {
    const node = this.nodes.get(nodeId);
    if (!node || !msg || typeof msg !== 'object') return;

    const isLoadWarning = msg.warning === 'load';
    const isOverloadError = typeof msg.error === 'string' && OVERLOAD_ERRORS.has(msg.error);

    if (isLoadWarning || isOverloadError) {
      node.loadWarnings += 1;
      const recoveryMs = this.limits.slowDownRecoveryMs ?? 15000;
      node.tokenBucket.triggerSlowdown(recoveryMs);
      this.logger.warn?.(
        `[NodeManager] Load signal from "${nodeId}" (${msg.warning || msg.error}); throttling token refill rate to ${node.tokenBucket.getEffectiveRate().toFixed(2)}/s`,
      );
      if (isOverloadError) {
        this.recordFailure(nodeId, `upstream_${msg.error}`);
      }
    }
  }

  recordRequestTimestamp(node, now = Date.now()) {
    node.totalRequests += 1;
    node.recentTimestamps.push(now);
    const cutoff = now - 10000;
    while (node.recentTimestamps.length > 0 && node.recentTimestamps[0] < cutoff) {
      node.recentTimestamps.shift();
    }
  }

  recordSuccess(nodeId) {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.consecutiveFailures = 0;
    if (node.state === 'HALF_OPEN') {
      node.state = 'CLOSED';
      this.logger.info?.(`[NodeManager] Circuit breaker CLOSED (recovered) for node "${nodeId}"`);
    }
  }

  recordFailure(nodeId, reason = 'error') {
    const node = this.nodes.get(nodeId);
    if (!node) return;
    node.totalErrors += 1;
    if (reason.includes('timeout')) {
      node.totalTimeouts += 1;
    }
    node.lastError = reason;
    node.consecutiveFailures += 1;

    const threshold = this.limits.circuitBreakerThreshold ?? 3;
    if (node.state === 'HALF_OPEN' || node.consecutiveFailures >= threshold) {
      this.tripCircuitBreaker(node, reason);
    }
  }

  tripCircuitBreaker(node, reason = 'failure_threshold') {
    const cooldownMs = this.limits.nodeCooldownMs ?? 30000;
    const wasOpen = node.state === 'OPEN';
    node.state = 'OPEN';
    node.cooldownUntil = Date.now() + cooldownMs;

    if (node.cooldownTimer) {
      clearTimeout(node.cooldownTimer);
    }

    node.cooldownTimer = setTimeout(() => {
      node.cooldownTimer = null;
      if (node.state === 'OPEN') {
        node.state = 'HALF_OPEN';
        this.logger.info?.(`[NodeManager] Circuit breaker HALF_OPEN for node "${node.id}"`);
      }
    }, cooldownMs);

    if (typeof node.cooldownTimer.unref === 'function') {
      node.cooldownTimer.unref();
    }

    if (!wasOpen) {
      this.logger.warn?.(
        `[NodeManager] Circuit breaker OPEN for node "${node.id}" (reason=${reason}, cooldown=${cooldownMs}ms)`,
      );
    }
  }

  /**
   * Manually trip or restore a node circuit breaker (useful for diagnostics/testing).
   */
  forceNodeState(nodeId, targetState) {
    const node = this.nodes.get(nodeId);
    if (!node) return false;
    if (node.cooldownTimer) {
      clearTimeout(node.cooldownTimer);
      node.cooldownTimer = null;
    }
    if (targetState === 'OPEN') {
      this.tripCircuitBreaker(node, 'manual_trip');
    } else {
      node.state = 'CLOSED';
      node.consecutiveFailures = 0;
      node.cooldownUntil = 0;
      node.lastError = null;
    }
    return true;
  }

  getStats() {
    const now = Date.now();
    const cutoff = now - 10000;
    const nodes = [];

    for (const node of this.nodes.values()) {
      this.isNodeHealthy(node, now);
      while (node.recentTimestamps.length > 0 && node.recentTimestamps[0] < cutoff) {
        node.recentTimestamps.shift();
      }
      const qps = Number((node.recentTimestamps.length / 10).toFixed(2));
      nodes.push({
        id: node.id,
        wsUrl: node.wsUrl,
        roles: Array.from(node.roles),
        state: node.state,
        poolConnectedCount: node.poolConnectedCount,
        poolSize: node.poolSize,
        activePathConns: node.activePathConns,
        maxPathConns: node.maxPathConns,
        activeSubscribeConns: node.activeSubscribeConns,
        maxSubscribeConns: node.maxSubscribeConns,
        tokens: Number(node.tokenBucket.getAvailableTokens(now).toFixed(2)),
        burst: node.tokenBucket.burst,
        effectiveRatePerSec: Number(node.tokenBucket.getEffectiveRate().toFixed(2)),
        baseRatePerSec: node.tokenBucket.baseRatePerSec,
        qps,
        totalRequests: node.totalRequests,
        totalErrors: node.totalErrors,
        totalTimeouts: node.totalTimeouts,
        loadWarnings: node.loadWarnings,
        consecutiveFailures: node.consecutiveFailures,
        cooldownRemainingMs: node.state === 'OPEN' ? Math.max(0, node.cooldownUntil - now) : 0,
        lastError: node.lastError,
      });
    }

    return {
      queueLength: this.rateQueue.length,
      nodes,
    };
  }

  close() {
    this.rateQueue.closeAll();
    for (const node of this.nodes.values()) {
      if (node.cooldownTimer) {
        clearTimeout(node.cooldownTimer);
        node.cooldownTimer = null;
      }
    }
  }
}
