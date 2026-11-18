import WebSocket from 'ws';
import { buildErrorResponse, extractRequestId } from './router.js';

/**
 * Isolated 1-to-1 upstream WebSocket channel bound to a single client session.
 * Handles lazy creation, CONNECTING buffering, path_find close/TTL lifecycle,
 * and upstream crash propagation (close code 1011).
 */
class DedicatedUpstreamChannel {
  /**
   * @param {{
   *   channelType: 'path_find' | 'subscribe',
   *   node: any,
   *   releaseSlot: () => void,
   *   clientWs: WebSocket,
   *   nodeManager: import('./nodeManager.js').NodeManager,
   *   limits: any,
   *   onClosed: () => void,
   * }} opts
   */
  constructor(opts) {
    this.channelType = opts.channelType;
    this.node = opts.node;
    this.releaseSlot = opts.releaseSlot;
    this.clientWs = opts.clientWs;
    this.nodeManager = opts.nodeManager;
    this.limits = opts.limits || {};
    this.onClosed = opts.onClosed;

    /** @type {WebSocket | null} */
    this.ws = null;
    /** @type {Array<{ serialized: string, idInfo: { hasId: boolean, id: unknown } }>} */
    this.outboundQueue = [];
    this.hasOpened = false;
    this.intentionalClose = false;
    this.closingAfterPathClose = false;
    this.pendingPathCloseId = null;
    this.pathCloseTimeout = null;
    this.ttlTimer = null;
    this.destroyed = false;

    this.connect();
    if (this.channelType === 'path_find') {
      this.startPathTtlTimer();
    }
  }

  connect() {
    if (this.destroyed) return;
    const ws = new WebSocket(this.node.wsUrl);
    this.ws = ws;

    ws.on('open', () => {
      if (this.destroyed || this.ws !== ws) {
        try { ws.close(); } catch {}
        return;
      }
      this.hasOpened = true;
      this.nodeManager.recordSuccess(this.node.id);
      this.flushQueue();
    });

    ws.on('message', (data, isBinary) => {
      if (this.destroyed) return;
      this.handleUpstreamMessage(data, isBinary);
    });

    ws.on('error', (err) => {
      if (this.destroyed || this.ws !== ws) return;
      this.nodeManager.recordFailure(this.node.id, `stateful_ws_error:${err.message || 'unknown'}`);
    });

    ws.on('close', () => {
      if (this.ws === ws) {
        this.ws = null;
      }
      const wasIntentional = this.intentionalClose || this.destroyed;
      const hadOpened = this.hasOpened;
      const queued = [...this.outboundQueue];
      this.outboundQueue.length = 0;

      this.cleanup();

      if (!wasIntentional) {
        if (!hadOpened) {
          this.nodeManager.recordFailure(this.node.id, 'stateful_connect_failed');
          for (const item of queued) {
            this.sendToClient(
              buildErrorResponse(
                item.idInfo,
                'upstreamUnavailable',
                `Unable to establish stateful channel to "${this.node.id}".`,
              ),
            );
          }
        } else {
          // Unexpected drop on an active subscription channel -> notify client SDK via 1011.
          if (this.clientWs.readyState === WebSocket.OPEN) {
            this.clientWs.close(1011, `Upstream ${this.channelType} channel closed unexpectedly`);
          }
        }
      }
    });
  }

  startPathTtlTimer() {
    const ttlMs = this.limits.maxPathTtlMs ?? 180000;
    if (ttlMs <= 0) return;

    this.ttlTimer = setTimeout(() => {
      this.ttlTimer = null;
      if (this.destroyed) return;
      // Gracefully ask upstream to close path_find before tearing down socket & slot.
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          this.ws.send(JSON.stringify({ command: 'path_find', subcommand: 'close' }));
        } catch {}
      }
      this.intentionalClose = true;
      setTimeout(() => {
        this.destroy();
      }, 250).unref?.();
    }, ttlMs);

    if (typeof this.ttlTimer.unref === 'function') {
      this.ttlTimer.unref();
    }
  }

  flushQueue() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    while (this.outboundQueue.length > 0 && this.ws.readyState === WebSocket.OPEN) {
      const item = this.outboundQueue.shift();
      if (item) {
        this.ws.send(item.serialized);
      }
    }
  }

  /**
   * Send payload over this dedicated upstream channel, buffering if still CONNECTING.
   * @param {Record<string, any>} payload
   * @param {{ hasId: boolean, id: unknown }} idInfo
   * @param {{ isPathClose?: boolean }} [opts]
   */
  send(payload, idInfo, opts = {}) {
    if (this.destroyed) {
      this.sendToClient(
        buildErrorResponse(idInfo, 'upstreamUnavailable', 'Stateful channel is closed.'),
      );
      return;
    }

    if (opts.isPathClose) {
      this.closingAfterPathClose = true;
      this.pendingPathCloseId = idInfo.hasId ? idInfo.id : Symbol('any-close-response');
      const closeTimeoutMs = Math.min(5000, this.limits.requestTimeoutMs ?? 15000);
      if (this.pathCloseTimeout) {
        clearTimeout(this.pathCloseTimeout);
      }
      this.pathCloseTimeout = setTimeout(() => {
        this.pathCloseTimeout = null;
        this.intentionalClose = true;
        this.destroy();
      }, closeTimeoutMs);
      if (typeof this.pathCloseTimeout.unref === 'function') {
        this.pathCloseTimeout.unref();
      }
    }

    const serialized = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(serialized);
    } else if (this.ws && this.ws.readyState === WebSocket.CONNECTING) {
      this.outboundQueue.push({ serialized, idInfo });
    } else {
      this.sendToClient(
        buildErrorResponse(idInfo, 'upstreamUnavailable', 'Stateful channel socket is unavailable.'),
      );
    }
  }

  handleUpstreamMessage(data, isBinary) {
    if (isBinary) {
      if (this.clientWs.readyState === WebSocket.OPEN) {
        this.clientWs.send(data, { binary: true });
      }
      return;
    }

    const text = typeof data === 'string' ? data : data.toString('utf8');
    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      if (this.clientWs.readyState === WebSocket.OPEN) {
        this.clientWs.send(text);
      }
      return;
    }

    if (parsed && typeof parsed === 'object') {
      this.nodeManager.inspectUpstreamResponse(this.node.id, parsed);
    }

    if (this.clientWs.readyState === WebSocket.OPEN) {
      this.clientWs.send(text);
    }

    // If waiting for path_find subcommand="close" response, tear down channel immediately after forwarding.
    if (this.closingAfterPathClose && parsed && parsed.type === 'response') {
      const respId = extractRequestId(parsed);
      const matched =
        typeof this.pendingPathCloseId === 'symbol' ||
        (respId.hasId && respId.id === this.pendingPathCloseId);

      if (matched) {
        this.intentionalClose = true;
        this.destroy();
      }
    }
  }

  sendToClient(obj) {
    if (this.clientWs.readyState === WebSocket.OPEN) {
      this.clientWs.send(JSON.stringify(obj));
    }
  }

  cleanup() {
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }
    if (this.pathCloseTimeout) {
      clearTimeout(this.pathCloseTimeout);
      this.pathCloseTimeout = null;
    }
    this.releaseSlot();
    this.onClosed();
  }

  destroy() {
    if (this.destroyed) return;
    this.destroyed = true;
    this.intentionalClose = true;
    this.outboundQueue.length = 0;

    if (this.ws) {
      const ws = this.ws;
      this.ws = null;
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close(1000, 'Channel closed');
        }
      } catch {
        try { ws.terminate(); } catch {}
      }
    }
    this.cleanup();
  }
}

/**
 * Per-client stateful channel coordinator (keeps `path_find` and `subscribe` strictly
 * isolated per client session, lazily created, and torn down on close/disconnect).
 */
export class ClientStatefulManager {
  /**
   * @param {WebSocket} clientWs
   * @param {import('./nodeManager.js').NodeManager} nodeManager
   * @param {any} limits
   */
  constructor(clientWs, nodeManager, limits) {
    this.clientWs = clientWs;
    this.nodeManager = nodeManager;
    this.limits = limits;
    /** @type {DedicatedUpstreamChannel | null} */
    this.pathChannel = null;
    /** @type {DedicatedUpstreamChannel | null} */
    this.subscribeChannel = null;
  }

  /**
   * Route a stateful command (`path_find`, `subscribe`, `unsubscribe`).
   * @param {Record<string, any>} payload
   * @param {{ hasId: boolean, id: unknown }} idInfo
   * @param {{ channelType: 'path_find' | 'subscribe', subcommand?: string }} route
   */
  handleCommand(payload, idInfo, route) {
    if (route.channelType === 'path_find') {
      const isClose = route.subcommand === 'close';

      if (isClose) {
        if (!this.pathChannel) {
          // No active path_find channel to close; return clean success response without leaking slots.
          const ack = {
            status: 'success',
            type: 'response',
            result: {},
          };
          if (idInfo.hasId) ack.id = idInfo.id;
          if (this.clientWs.readyState === WebSocket.OPEN) {
            this.clientWs.send(JSON.stringify(ack));
          }
          return;
        }
        // Must go to the exact active pathChannel and destroy after response!
        this.pathChannel.send(payload, idInfo, { isPathClose: true });
        return;
      }

      if (!this.pathChannel) {
        const slot = this.nodeManager.acquireStatefulSlot('path', 'path_find');
        if (!slot.ok) {
          if (this.clientWs.readyState === WebSocket.OPEN) {
            this.clientWs.send(
              JSON.stringify(buildErrorResponse(idInfo, slot.errorCode, slot.errorMessage)),
            );
          }
          return;
        }

        this.pathChannel = new DedicatedUpstreamChannel({
          channelType: 'path_find',
          node: slot.node,
          releaseSlot: slot.release,
          clientWs: this.clientWs,
          nodeManager: this.nodeManager,
          limits: this.limits,
          onClosed: () => {
            this.pathChannel = null;
          },
        });
      }

      this.pathChannel.send(payload, idInfo);
      return;
    }

    // subscribe / unsubscribe
    if (!this.subscribeChannel) {
      if (payload.command === 'unsubscribe') {
        const ack = { status: 'success', type: 'response', result: {} };
        if (idInfo.hasId) ack.id = idInfo.id;
        if (this.clientWs.readyState === WebSocket.OPEN) {
          this.clientWs.send(JSON.stringify(ack));
        }
        return;
      }

      const slot = this.nodeManager.acquireStatefulSlot('normal', 'subscribe');
      if (!slot.ok) {
        if (this.clientWs.readyState === WebSocket.OPEN) {
          this.clientWs.send(
            JSON.stringify(buildErrorResponse(idInfo, slot.errorCode, slot.errorMessage)),
          );
        }
        return;
      }

      this.subscribeChannel = new DedicatedUpstreamChannel({
        channelType: 'subscribe',
        node: slot.node,
        releaseSlot: slot.release,
        clientWs: this.clientWs,
        nodeManager: this.nodeManager,
        limits: this.limits,
        onClosed: () => {
          this.subscribeChannel = null;
        },
      });
    }

    this.subscribeChannel.send(payload, idInfo);
  }

  destroyAll() {
    if (this.pathChannel) {
      const ch = this.pathChannel;
      this.pathChannel = null;
      ch.destroy();
    }
    if (this.subscribeChannel) {
      const ch = this.subscribeChannel;
      this.subscribeChannel = null;
      ch.destroy();
    }
  }
}
