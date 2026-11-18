import http from 'node:http';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config as baseConfig, createConfig } from './src/config.js';
import { ProxyServer } from './src/wsServer.js';

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));

interface MockNodeHandle {
  id: string;
  roles: string[];
  wsUrl: string;
  online: boolean;
  wss: WebSocketServer;
  activeSockets: Set<WebSocket>;
  simulateLoadWarning: boolean;
  stop: () => Promise<void>;
  restart: () => Promise<void>;
}

async function createMockNode(nodeCfg: (typeof baseConfig.nodes)[number]): Promise<MockNodeHandle> {
  let wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>((resolve) => wss.once('listening', resolve));
  const addr = wss.address();
  const port = typeof addr === 'object' && addr ? addr.port : 0;
  const wsUrl = `ws://127.0.0.1:${port}`;
  const activeSockets = new Set<WebSocket>();
  let ledgerSeq = 91842100;

  const handle: MockNodeHandle = {
    id: nodeCfg.id,
    roles: [...nodeCfg.roles],
    wsUrl,
    online: true,
    wss,
    activeSockets,
    simulateLoadWarning: false,
    async stop() {
      handle.online = false;
      for (const s of activeSockets) {
        try {
          s.terminate();
        } catch {}
      }
      activeSockets.clear();
      await new Promise<void>((resolve) => handle.wss.close(() => resolve()));
    },
    async restart() {
      if (handle.online) return;
      const nextWss = new WebSocketServer({ host: '127.0.0.1', port });
      await new Promise<void>((resolve) => nextWss.once('listening', resolve));
      handle.wss = nextWss;
      attachHandlers(nextWss);
      handle.online = true;
    },
  };

  function attachHandlers(server: WebSocketServer) {
    server.on('connection', (ws) => {
      activeSockets.add(ws);
      let pathTimer: NodeJS.Timeout | null = null;
      let subTimer: NodeJS.Timeout | null = null;

      ws.on('message', (raw) => {
        let msg: Record<string, any>;
        try {
          msg = JSON.parse(raw.toString('utf8'));
        } catch {
          return;
        }

        const extraWarning = handle.simulateLoadWarning ? { warning: 'load' } : {};
        if (handle.simulateLoadWarning) {
          handle.simulateLoadWarning = false;
        }

        if (msg.command === 'path_find') {
          if (msg.subcommand === 'close') {
            if (pathTimer) {
              clearInterval(pathTimer);
              pathTimer = null;
            }
            ws.send(
              JSON.stringify({
                id: msg.id,
                status: 'success',
                type: 'response',
                ...extraWarning,
                result: { closed: true, served_by: nodeCfg.id },
              }),
            );
            return;
          }

          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              ...extraWarning,
              result: {
                alternatives: [
                  {
                    paths_computed: [[{ currency: 'USD', issuer: 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B' }]],
                    source_amount: '1000000',
                  },
                ],
                destination_amount: msg.destination_amount || '100',
                served_by: nodeCfg.id,
              },
            }),
          );

          if (!pathTimer) {
            pathTimer = setInterval(() => {
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(
                  JSON.stringify({
                    type: 'path_find',
                    source_account: msg.source_account || 'rSourceAccount',
                    destination_account: msg.destination_account || 'rDestAccount',
                    alternatives: [{ source_amount: String(990000 + Math.floor(Math.random() * 20000)) }],
                    served_by: nodeCfg.id,
                  }),
                );
              }
            }, 3000);
          }
          return;
        }

        if (msg.command === 'subscribe') {
          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              ...extraWarning,
              result: {
                ledger_index: ledgerSeq,
                validated_ledgers: `32570-${ledgerSeq}`,
                served_by: nodeCfg.id,
              },
            }),
          );
          if (!subTimer) {
            subTimer = setInterval(() => {
              ledgerSeq += 1;
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(
                  JSON.stringify({
                    type: 'ledgerClosed',
                    ledger_index: ledgerSeq,
                    txn_count: Math.floor(Math.random() * 45) + 5,
                    served_by: nodeCfg.id,
                  }),
                );
              }
            }, 3500);
          }
          return;
        }

        if (msg.command === 'unsubscribe') {
          if (subTimer) {
            clearInterval(subTimer);
            subTimer = null;
          }
          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              result: { unsubscribed: true, served_by: nodeCfg.id },
            }),
          );
          return;
        }

        const resultByCommand: Record<string, any> = {
          server_info: {
            info: {
              build_version: '2.2.3',
              complete_ledgers: `32570-${ledgerSeq}`,
              load_factor: 1,
              validated_ledger: { seq: ledgerSeq, base_fee_xrp: 0.00001 },
            },
            served_by: nodeCfg.id,
          },
          fee: {
            current_ledger_size: '28',
            drops: { base_fee: '10', median_fee: '5000', open_ledger_fee: '10' },
            served_by: nodeCfg.id,
          },
          ledger_closed: {
            ledger_index: ledgerSeq,
            ledger_hash: '8B4F2A19E0C54A1D99C8E4B5227F6A3F112233445566778899AABBCCDDEEFF00',
            served_by: nodeCfg.id,
          },
          account_tx: {
            account: msg.account || 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe',
            ledger_index_min: ledgerSeq - 1000,
            ledger_index_max: ledgerSeq,
            transactions: [{ validated: true, tx: { TransactionType: 'Payment', Amount: '1000000' } }],
            served_by: nodeCfg.id,
          },
          submit: {
            engine_result: 'tesSUCCESS',
            engine_result_code: 0,
            engine_result_message: 'The transaction was applied. Only final in a validated ledger.',
            served_by: nodeCfg.id,
          },
        };

        ws.send(
          JSON.stringify({
            id: msg.id,
            status: 'success',
            type: 'response',
            ...extraWarning,
            result: resultByCommand[msg.command] || {
              command: msg.command,
              ledger_index: ledgerSeq,
              served_by: nodeCfg.id,
            },
          }),
        );
      });

      ws.on('close', () => {
        activeSockets.delete(ws);
        if (pathTimer) clearInterval(pathTimer);
        if (subTimer) clearInterval(subTimer);
      });
    });
  }

  attachHandlers(wss);
  return handle;
}

async function startServer() {
  const app = express();
  app.use(express.json());

  const httpServer = http.createServer(app);

  // Boot local mock xrpld cluster matching src/config.js roles & quotas
  const mockNodes = await Promise.all(baseConfig.nodes.map((n) => createMockNode(n)));
  const mockNodeMap = new Map(mockNodes.map((m) => [m.id, m]));

  const proxyConfig = createConfig({
    server: {
      host: '0.0.0.0',
      port: 3000,
      statsLogIntervalMs: 0,
    },
    nodes: baseConfig.nodes.map((n, idx) => ({
      ...n,
      wsUrl: mockNodes[idx].wsUrl,
    })),
  });

  const proxy = new ProxyServer(proxyConfig, {
    httpServer,
    path: '/ws',
  });
  await proxy.start();

  app.get('/api/stats', (_req, res) => {
    const stats = proxy.getStats();
    const enrichedNodes = stats.nodes.map((n: any) => ({
      ...n,
      mockOnline: mockNodeMap.get(n.id)?.online ?? true,
    }));
    res.json({
      ...stats,
      nodes: enrichedNodes,
      routing: proxyConfig.routing,
      limits: proxyConfig.limits,
    });
  });

  app.post('/api/nodes/:id/toggle-online', async (req, res) => {
    const mock = mockNodeMap.get(req.params.id);
    if (!mock) {
      res.status(404).json({ error: 'Node not found' });
      return;
    }
    if (mock.online) {
      await mock.stop();
    } else {
      await mock.restart();
      proxy.nodeManager.forceNodeState(mock.id, 'CLOSED');
    }
    res.json({ id: mock.id, online: mock.online });
  });

  app.post('/api/nodes/:id/circuit', (req, res) => {
    const target = req.body?.state === 'OPEN' ? 'OPEN' : 'CLOSED';
    const ok = proxy.nodeManager.forceNodeState(req.params.id, target);
    if (!ok) {
      res.status(404).json({ error: 'Node not found' });
      return;
    }
    res.json({ id: req.params.id, state: target });
  });

  app.post('/api/nodes/:id/load-warning', (req, res) => {
    const mock = mockNodeMap.get(req.params.id);
    if (!mock) {
      res.status(404).json({ error: 'Node not found' });
      return;
    }
    mock.simulateLoadWarning = true;
    res.json({ id: mock.id, queuedLoadWarning: true });
  });

  app.post('/api/run-tests', async (_req, res) => {
    try {
      const { stdout, stderr } = await execFileAsync(
        process.execPath,
        ['--test', 'test/router.test.js', 'test/rateLimiter.test.js', 'test/proxy.e2e.test.js'],
        { cwd: __dirname, timeout: 30000 },
      );
      res.json({ ok: true, output: stdout + (stderr ? `\n${stderr}` : '') });
    } catch (err: any) {
      res.json({
        ok: false,
        output: `${err.stdout || ''}\n${err.stderr || ''}\n${err.message || ''}`.trim(),
      });
    }
  });

  if (process.env.NODE_ENV === 'production') {
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (_req, res) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  } else {
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: false,
        ws: false,
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  }

  const PORT = 3000;
  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`[xrpld-ws-proxy] Web Console + WS Proxy (/ws) running on http://0.0.0.0:${PORT}`);
  });

  let isShuttingDown = false;
  const shutdown = async () => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    await proxy.shutdown();
    await Promise.all(mockNodes.map((n) => n.stop()));
    httpServer.close(() => process.exit(0));
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

startServer().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});
