import { WebSocketServer, WebSocket } from 'ws';
import { config as defaultConfig } from './config.js';
import { parseMessage, classifyRoute, buildErrorResponse } from './router.js';
import { NodeManager } from './nodeManager.js';
import { StatelessPool } from './statelessPool.js';
import { ClientStatefulManager } from './statefulChannel.js';
import { ReadCache } from './cache.js';

/**
 * Extract client IP address with optional X-Forwarded-For trust.
 * @param {import('http').IncomingMessage} req
 * @param {boolean} trustProxy
 */
function getClientIp(req, trustProxy) {
  if (trustProxy) {
    const xff = req.headers['x-forwarded-for'];
    if (typeof xff === 'string' && xff.trim()) {
      return xff.split(',')[0].trim();
    }
  }
  return req.socket?.remoteAddress || 'unknown';
}

/**
 * Main WebSocket reverse proxy server orchestrating IP admission limits,
 * heartbeat ping/pong, routing, stateless multiplexing, micro-cache, and stateful channels.
 */
export class ProxyServer {
  /**
   * @param {ReturnType<typeof import('./config.js').createConfig>} [cfg]
   * @param {{
   *   httpServer?: import('http').Server,
   *   path?: string,
   *   logger?: Console,
   * }} [options]
   */
  constructor(cfg = defaultConfig, options = {}) {
    this.config = cfg;
    this.logger = options.logger || console;
    this.httpServer = options.httpServer || null;
    this.path = options.path;

    this.nodeManager = new NodeManager(cfg, { logger: this.logger });
    this.statelessPool = new StatelessPool(this.nodeManager, cfg.limits, { logger: this.logger });
    this.cache = new ReadCache(cfg.cache);

    /** @type {Set<{ ws: WebSocket, ip: string, stateful: ClientStatefulManager, isAlive: boolean }>} */
    this.sessions = new Set();
    /** @type {Map<string, number>} */
    this.ipConnectionCounts = new Map();
    /** @type {WebSocketServer | null} */
    this.wss = null;
    this.heartbeatTimer = null;
    this.shuttingDown = false;
  }

  /**
   * Start upstream pools and WebSocket server.
   * @returns {Promise<{ host: string, port: number }>}
   */
  async start() {
    this.statelessPool.start();

    const wssOptions = {
      maxPayload: this.config.server.maxPayloadBytes ?? 1024 * 1024,
      ...(this.httpServer
        ? { noServer: true }
        : {
            host: this.config.server.host ?? '0.0.0.0',
            port: this.config.server.port ?? 8080,
          }),
    };

    this.wss = new WebSocketServer(wssOptions);

    if (this.httpServer) {
      this.upgradeHandler = (req, socket, head) => {
        const reqPath = (req.url || '/').split('?')[0];
        if (this.path && reqPath !== this.path) {
          return;
        }
        if (this.shuttingDown) {
          socket.write('HTTP/1.1 503 Service Unavailable\r\n\r\n');
          socket.destroy();
          return;
        }
        const ip = getClientIp(req, Boolean(this.config.server.trustProxy));
        const maxPerIp = this.config.server.maxConnectionsPerIp ?? 20;
        const currentCount = this.ipConnectionCounts.get(ip) || 0;
        if (maxPerIp > 0 && currentCount >= maxPerIp) {
          socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n');
          socket.destroy();
          return;
        }
        this.wss.handleUpgrade(req, socket, head, (ws) => {
          this.wss.emit('connection', ws, req);
        });
      };
      this.httpServer.on('upgrade', this.upgradeHandler);
    }

    this.wss.on('connection', (ws, req) => this.handleClientConnection(ws, req));
    this.startClientHeartbeat();

    if (this.httpServer) {
      const addr = this.httpServer.address();
      if (addr && typeof addr === 'object') {
        return { host: addr.address, port: addr.port };
      }
      return { host: this.config.server.host ?? '0.0.0.0', port: this.config.server.port ?? 3000 };
    }

    return new Promise((resolve, reject) => {
      this.wss.once('listening', () => {
        const addr = this.wss.address();
        if (addr && typeof addr === 'object') {
          resolve({ host: addr.address, port: addr.port });
        } else {
          resolve({ host: this.config.server.host ?? '0.0.0.0', port: this.config.server.port ?? 8080 });
        }
      });
      this.wss.once('error', reject);
    });
  }

  handleClientConnection(ws, req) {
    if (this.shuttingDown) {
      ws.close(1001, 'Server shutting down');
      return;
    }

    const ip = getClientIp(req, Boolean(this.config.server.trustProxy));
    const maxPerIp = this.config.server.maxConnectionsPerIp ?? 20;
    const currentCount = this.ipConnectionCounts.get(ip) || 0;
    if (maxPerIp > 0 && currentCount >= maxPerIp) {
      ws.close(1008, 'Too many concurrent connections from IP');
      return;
    }

    this.ipConnectionCounts.set(ip, currentCount + 1);

    const session = {
      ws,
      ip,
      isAlive: true,
      stateful: new ClientStatefulManager(ws, this.nodeManager, this.config.limits),
    };
    this.sessions.add(session);

    ws.on('pong', () => {
      session.isAlive = true;
    });

    ws.on('message', (data) => {
      if (this.shuttingDown) return;
      this.handleClientMessage(session, data);
    });

    const cleanupSession = () => {
      if (!this.sessions.has(session)) return;
      this.sessions.delete(session);
      const count = (this.ipConnectionCounts.get(ip) || 1) - 1;
      if (count <= 0) {
        this.ipConnectionCounts.delete(ip);
      } else {
        this.ipConnectionCounts.set(ip, count);
      }
      session.stateful.destroyAll();
    };

    ws.on('close', cleanupSession);
    ws.on('error', cleanupSession);
  }

  /**
   * Parse, route, rate-limit, and dispatch a single client message.
   * @param {{ ws: WebSocket, stateful: ClientStatefulManager }} session
   * @param {import('ws').RawData} rawData
   */
  async handleClientMessage(session, rawData) {
    const parsed = parseMessage(rawData);
    if (!parsed.ok) {
      this.sendJson(
        session.ws,
        buildErrorResponse(parsed.idInfo, parsed.errorCode, parsed.errorMessage),
      );
      return;
    }

    const { payload, idInfo } = parsed;
    const route = classifyRoute(payload, this.config.routing);

    if (route.isStateful) {
      session.stateful.handleCommand(payload, idInfo, route);
      return;
    }

    try {
      const response = await this.cache.getOrFetch(payload, async () => {
        const node = await this.nodeManager.acquireStatelessNode(route.role, route.weight);
        return this.statelessPool.sendRequest(node, payload, idInfo);
      });

      // Restore caller's exact `id` even when served from shared cache / singleflight.
      if (response && typeof response === 'object') {
        if (idInfo.hasId) {
          response.id = idInfo.id;
        } else {
          delete response.id;
        }
      }

      this.sendJson(session.ws, response);
    } catch (err) {
      const errorCode = err?.code || 'upstreamUnavailable';
      const errorMessage = err?.message || 'Unable to route request to upstream xrpld node.';
      this.sendJson(session.ws, buildErrorResponse(idInfo, errorCode, errorMessage));
    }
  }

  sendJson(ws, obj) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
    }
  }

  startClientHeartbeat() {
    const intervalMs = this.config.server.heartbeatIntervalMs ?? 30000;
    if (intervalMs <= 0) return;

    this.heartbeatTimer = setInterval(() => {
      for (const session of this.sessions) {
        if (!session.isAlive) {
          session.ws.terminate();
          continue;
        }
        session.isAlive = false;
        try {
          session.ws.ping();
        } catch {
          session.ws.terminate();
        }
      }
    }, intervalMs);

    if (typeof this.heartbeatTimer.unref === 'function') {
      this.heartbeatTimer.unref();
    }
  }

  getStats() {
    return {
      clientConnections: this.sessions.size,
      uniqueClientIps: this.ipConnectionCounts.size,
      pool: this.statelessPool.getStats(),
      cache: this.cache.getStats(),
      ...this.nodeManager.getStats(),
    };
  }

  async shutdown() {
    if (this.shuttingDown) return;
    this.shuttingDown = true;

    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    if (this.httpServer && this.upgradeHandler) {
      this.httpServer.off('upgrade', this.upgradeHandler);
      this.upgradeHandler = null;
    }

    for (const session of Array.from(this.sessions)) {
      session.stateful.destroyAll();
      try {
        if (
          session.ws.readyState === WebSocket.OPEN ||
          session.ws.readyState === WebSocket.CONNECTING
        ) {
          session.ws.close(1001, 'Proxy server shutting down');
        }
      } catch {
        try { session.ws.terminate(); } catch {}
      }
    }
    this.sessions.clear();
    this.ipConnectionCounts.clear();

    this.statelessPool.closeAll();
    this.nodeManager.close();
    this.cache.clear();

    if (this.wss) {
      await new Promise((resolve) => {
        this.wss.close(() => resolve(undefined));
      });
      this.wss = null;
    }
  }
}
