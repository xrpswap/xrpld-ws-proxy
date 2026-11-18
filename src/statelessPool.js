import WebSocket from 'ws';
import { buildErrorResponse } from './router.js';

/**
 * Single persistent upstream WebSocket connection inside a node's stateless pool.
 */
class UpstreamPoolConnection {
  /**
   * @param {StatelessPool} pool
   * @param {any} node
   * @param {number} index
   */
  constructor(pool, node, index) {
    this.pool = pool;
    this.node = node;
    this.index = index;
    /** @type {WebSocket | null} */
    this.ws = null;
    /** @type {string[]} */
    this.outboundQueue = [];
    /** @type {Set<number>} */
    this.inFlightProxyIds = new Set();
    this.reconnectAttempts = 0;
    this.reconnectTimer = null;
    this.pingTimer = null;
    this.awaitingPong = false;
    this.closed = false;
  }

  get isOpen() {
    return this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  get isConnecting() {
    return this.ws !== null && this.ws.readyState === WebSocket.CONNECTING;
  }

  connect() {
    if (this.closed) return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    const ws = new WebSocket(this.node.wsUrl);
    this.ws = ws;
    this.awaitingPong = false;

    ws.on('open', () => {
      if (this.closed || this.ws !== ws) {
        try { ws.close(); } catch {}
        return;
      }
      this.reconnectAttempts = 0;
      this.pool.updateNodeConnectedCount(this.node.id);
      this.startHeartbeat();
      this.flushOutboundQueue();
    });

    ws.on('pong', () => {
      this.awaitingPong = false;
    });

    ws.on('message', (data) => {
      this.pool.handleUpstreamMessage(this.node.id, data);
    });

    ws.on('error', (err) => {
      if (this.closed || this.ws !== ws) return;
      this.pool.nodeManager.recordFailure(this.node.id, `ws_error:${err.message || 'unknown'}`);
    });

    ws.on('close', (code) => {
      if (this.ws === ws) {
        this.ws = null;
      }
      this.stopHeartbeat();
      this.pool.updateNodeConnectedCount(this.node.id);

      // Fail any in-flight requests bound to this specific connection immediately.
      if (this.inFlightProxyIds.size > 0) {
        const ids = Array.from(this.inFlightProxyIds);
        this.inFlightProxyIds.clear();
        for (const proxyId of ids) {
          this.pool.failPendingRequest(
            proxyId,
            'upstreamUnavailable',
            `Upstream node "${this.node.id}" connection closed (code ${code}).`,
          );
        }
      }

      if (!this.closed) {
        if (code !== 1000) {
          this.pool.nodeManager.recordFailure(this.node.id, `ws_close_${code}`);
        }
        this.scheduleReconnect();
      }
    });
  }

  startHeartbeat() {
    this.stopHeartbeat();
    const intervalMs = this.pool.limits.upstreamPingIntervalMs ?? 25000;
    if (intervalMs <= 0) return;

    this.pingTimer = setInterval(() => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      if (this.awaitingPong) {
        this.ws.terminate();
        return;
      }
      this.awaitingPong = true;
      try {
        this.ws.ping();
      } catch {
        this.ws.terminate();
      }
    }, intervalMs);

    if (typeof this.pingTimer.unref === 'function') {
      this.pingTimer.unref();
    }
  }

  stopHeartbeat() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    this.awaitingPong = false;
  }

  scheduleReconnect() {
    if (this.closed || this.reconnectTimer) return;
    const base = this.pool.limits.reconnectBaseDelayMs ?? 500;
    const max = this.pool.limits.reconnectMaxDelayMs ?? 15000;
    const exp = Math.min(max, base * Math.pow(2, Math.min(this.reconnectAttempts, 6)));
    const jitter = Math.floor(Math.random() * Math.min(1000, exp * 0.4));
    const delay = exp + jitter;
    this.reconnectAttempts += 1;

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);

    if (typeof this.reconnectTimer.unref === 'function') {
      this.reconnectTimer.unref();
    }
  }

  flushOutboundQueue() {
    if (!this.isOpen || this.outboundQueue.length === 0) return;
    while (this.outboundQueue.length > 0 && this.isOpen) {
      const msg = this.outboundQueue.shift();
      if (msg !== undefined) {
        this.ws.send(msg);
      }
    }
  }

  /**
   * Send serialized JSON frame, buffering if socket is still CONNECTING.
   * @param {string} serialized
   * @param {number} proxyId
   */
  send(serialized, proxyId) {
    if (this.closed) return false;
    this.inFlightProxyIds.add(proxyId);

    if (this.isOpen) {
      this.ws.send(serialized);
      return true;
    }

    if (this.isConnecting) {
      this.outboundQueue.push(serialized);
      return true;
    }

    // Socket is closed/reconnecting: buffer and kick connect immediately.
    this.outboundQueue.push(serialized);
    this.connect();
    return true;
  }

  releaseProxyId(proxyId) {
    this.inFlightProxyIds.delete(proxyId);
  }

  close() {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopHeartbeat();
    this.outboundQueue.length = 0;

    if (this.ws) {
      const ws = this.ws;
      this.ws = null;
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close(1000, 'Proxy shutting down');
        }
      } catch {
        try { ws.terminate(); } catch {}
      }
    }
  }
}

/**
 * Global stateless WebSocket connection pool with `proxyId` multiplexing,
 * request timeout timers, and response `id` restoration.
 */
export class StatelessPool {
  /**
   * @param {import('./nodeManager.js').NodeManager} nodeManager
   * @param {any} [limits]
   * @param {{ logger?: Console }} [options]
   */
  constructor(nodeManager, limits = nodeManager.limits, options = {}) {
    this.nodeManager = nodeManager;
    this.limits = limits || {};
    this.logger = options.logger || console;
    this.nextProxyId = 1;
    /** @type {Map<string, UpstreamPoolConnection[]>} */
    this.nodePools = new Map();
    /** @type {Map<string, number>} */
    this.nodeRoundRobin = new Map();
    /**
     * @type {Map<number, {
     *   originalId: unknown,
     *   hasOriginalId: boolean,
     *   timer: NodeJS.Timeout,
     *   nodeId: string,
     *   conn: UpstreamPoolConnection,
     *   resolve: (res: Record<string, any>) => void,
     *   reject: (err: Error) => void,
     * }>}
     */
    this.pendingMap = new Map();
    this.closed = false;

    for (const node of this.nodeManager.getAllNodes()) {
      const size = Math.max(0, node.poolSize ?? 2);
      const conns = [];
      for (let i = 0; i < size; i += 1) {
        conns.push(new UpstreamPoolConnection(this, node, i));
      }
      this.nodePools.set(node.id, conns);
      this.nodeRoundRobin.set(node.id, 0);
    }
  }

  start() {
    for (const conns of this.nodePools.values()) {
      for (const conn of conns) {
        conn.connect();
      }
    }
  }

  updateNodeConnectedCount(nodeId) {
    const conns = this.nodePools.get(nodeId) || [];
    const openCount = conns.filter((c) => c.isOpen).length;
    this.nodeManager.setPoolConnectedCount(nodeId, openCount);
  }

  /**
   * Pick an upstream connection for `nodeId`, preferring OPEN sockets round-robin.
   * @param {string} nodeId
   */
  pickConnection(nodeId) {
    const conns = this.nodePools.get(nodeId) || [];
    if (conns.length === 0) return null;

    const openConns = conns.filter((c) => c.isOpen);
    const pool = openConns.length > 0 ? openConns : conns;
    const cursor = this.nodeRoundRobin.get(nodeId) || 0;
    const chosen = pool[cursor % pool.length];
    this.nodeRoundRobin.set(nodeId, (cursor + 1) % pool.length);
    return chosen;
  }

  /**
   * Send a stateless XRPL request through the multiplexed pool and resolve with the
   * response object (with original `id` restored or omitted).
   * @param {any} node
   * @param {Record<string, any>} payload
   * @param {{ hasId: boolean, id: unknown }} idInfo
   * @returns {Promise<Record<string, any>>}
   */
  sendRequest(node, payload, idInfo) {
    if (this.closed) {
      return Promise.resolve(
        buildErrorResponse(idInfo, 'upstreamUnavailable', 'Proxy pool is shutting down.'),
      );
    }

    const conn = this.pickConnection(node.id);
    if (!conn) {
      return Promise.resolve(
        buildErrorResponse(idInfo, 'upstreamUnavailable', `Node "${node.id}" has no pool connections.`),
      );
    }

    const proxyId = this.nextProxyId++;
    const upstreamPayload = { ...payload, id: proxyId };
    const serialized = JSON.stringify(upstreamPayload);
    const timeoutMs = this.limits.requestTimeoutMs ?? 15000;

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        const entry = this.pendingMap.get(proxyId);
        if (!entry) return;
        this.pendingMap.delete(proxyId);
        entry.conn.releaseProxyId(proxyId);
        this.nodeManager.recordFailure(entry.nodeId, 'request_timeout');
        resolve(
          buildErrorResponse(
            { hasId: entry.hasOriginalId, id: entry.originalId },
            'upstreamTimeout',
            `Upstream node "${entry.nodeId}" did not respond within ${timeoutMs}ms.`,
          ),
        );
      }, timeoutMs);

      if (typeof timer.unref === 'function') {
        timer.unref();
      }

      this.pendingMap.set(proxyId, {
        originalId: idInfo.id,
        hasOriginalId: idInfo.hasId,
        timer,
        nodeId: node.id,
        conn,
        resolve,
        reject: () => {},
      });

      const sent = conn.send(serialized, proxyId);
      if (!sent) {
        clearTimeout(timer);
        this.pendingMap.delete(proxyId);
        conn.releaseProxyId(proxyId);
        resolve(
          buildErrorResponse(idInfo, 'upstreamUnavailable', `Unable to queue message to "${node.id}".`),
        );
      }
    });
  }

  /**
   * Handle an incoming JSON message from an upstream pool socket.
   * @param {string} nodeId
   * @param {import('ws').RawData} rawData
   */
  handleUpstreamMessage(nodeId, rawData) {
    let msg;
    try {
      const text = typeof rawData === 'string' ? rawData : rawData.toString('utf8');
      msg = JSON.parse(text);
    } catch {
      return;
    }

    if (!msg || typeof msg !== 'object') return;
    if (typeof msg.id !== 'number') return;

    const entry = this.pendingMap.get(msg.id);
    if (!entry) return;

    this.pendingMap.delete(msg.id);
    clearTimeout(entry.timer);
    entry.conn.releaseProxyId(msg.id);

    this.nodeManager.inspectUpstreamResponse(nodeId, msg);

    if (msg.status !== 'error') {
      this.nodeManager.recordSuccess(nodeId);
    }

    if (entry.hasOriginalId) {
      msg.id = entry.originalId;
    } else {
      delete msg.id;
    }

    entry.resolve(msg);
  }

  failPendingRequest(proxyId, errorCode, errorMessage) {
    const entry = this.pendingMap.get(proxyId);
    if (!entry) return;
    this.pendingMap.delete(proxyId);
    clearTimeout(entry.timer);
    entry.conn.releaseProxyId(proxyId);
    entry.resolve(
      buildErrorResponse(
        { hasId: entry.hasOriginalId, id: entry.originalId },
        errorCode,
        errorMessage,
      ),
    );
  }

  getStats() {
    return {
      pendingRequests: this.pendingMap.size,
      nextProxyId: this.nextProxyId,
    };
  }

  closeAll() {
    this.closed = true;
    for (const [proxyId, entry] of this.pendingMap.entries()) {
      clearTimeout(entry.timer);
      entry.conn.releaseProxyId(proxyId);
      entry.resolve(
        buildErrorResponse(
          { hasId: entry.hasOriginalId, id: entry.originalId },
          'upstreamUnavailable',
          'Proxy is shutting down.',
        ),
      );
    }
    this.pendingMap.clear();

    for (const conns of this.nodePools.values()) {
      for (const conn of conns) {
        conn.close();
      }
    }
  }
}
