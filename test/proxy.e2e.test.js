import test from 'node:test';
import assert from 'node:assert/strict';
import { WebSocketServer, WebSocket } from 'ws';
import { createConfig } from '../src/config.js';
import { ProxyServer } from '../src/wsServer.js';

/**
 * Spin up an in-memory mock xrpld WebSocket server on an ephemeral localhost port.
 */
async function startMockXrpldNode(nodeName, options = {}) {
  const wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve) => wss.once('listening', resolve));
  const addr = wss.address();
  const wsUrl = `ws://127.0.0.1:${addr.port}`;

  const received = [];
  const activeSockets = new Set();

  wss.on('connection', (ws) => {
    activeSockets.add(ws);
    let pathPushTimer = null;

    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString('utf8'));
      received.push(msg);

      if (options.onMessage) {
        const handled = options.onMessage({ ws, msg, nodeName, activeSockets });
        if (handled) return;
      }

      if (msg.command === 'path_find') {
        if (msg.subcommand === 'create') {
          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              result: { alternatives: [], node: nodeName, source: msg.source_account },
            }),
          );
          pathPushTimer = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(
                JSON.stringify({
                  type: 'path_find',
                  source_account: msg.source_account,
                  destination_account: msg.destination_account,
                  node: nodeName,
                }),
              );
            }
          }, 40);
          return;
        }
        if (msg.subcommand === 'close') {
          if (pathPushTimer) {
            clearInterval(pathPushTimer);
            pathPushTimer = null;
          }
          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              result: { closed: true, node: nodeName },
            }),
          );
          return;
        }
      }

      ws.send(
        JSON.stringify({
          id: msg.id,
          status: 'success',
          type: 'response',
          result: {
            echo_command: msg.command,
            echo_tag: msg.tag ?? null,
            served_by: nodeName,
          },
        }),
      );
    });

    ws.on('close', () => {
      activeSockets.delete(ws);
      if (pathPushTimer) {
        clearInterval(pathPushTimer);
        pathPushTimer = null;
      }
    });
  });

  return {
    nodeName,
    wsUrl,
    wss,
    received,
    activeSockets,
    async close() {
      for (const s of activeSockets) {
        try { s.terminate(); } catch {}
      }
      await new Promise((resolve) => wss.close(resolve));
    },
  };
}

function connectClient(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const messages = [];
    const waiters = [];

    ws.on('message', (raw) => {
      const parsed = JSON.parse(raw.toString('utf8'));
      if (waiters.length > 0) {
        const next = waiters.shift();
        next(parsed);
      } else {
        messages.push(parsed);
      }
    });

    ws.nextMessage = (timeoutMs = 3000) => {
      if (messages.length > 0) {
        return Promise.resolve(messages.shift());
      }
      return new Promise((res, rej) => {
        const timer = setTimeout(() => {
          const idx = waiters.indexOf(onMsg);
          if (idx !== -1) waiters.splice(idx, 1);
          rej(new Error(`Timed out waiting for client WS message after ${timeoutMs}ms`));
        }, timeoutMs);
        const onMsg = (msg) => {
          clearTimeout(timer);
          res(msg);
        };
        waiters.push(onMsg);
      });
    };

    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

async function waitFor(predicate, timeoutMs = 2000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error(`Condition not met within ${timeoutMs}ms`);
}

test('E2E Acceptance 1 & 2: Role-based routing and stateless ID multiplexing without collision (including id: 0 and missing id)', async () => {
  const normalNode = await startMockXrpldNode('normal-node');
  const historyNode = await startMockXrpldNode('history-node');
  const submitNode = await startMockXrpldNode('submit-node');
  const pathNode = await startMockXrpldNode('path-node');

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    cache: { enabled: false },
    nodes: [
      { id: 'normal-node', wsUrl: normalNode.wsUrl, roles: ['normal'], poolSize: 2, maxPathConns: 0, maxSubscribeConns: 10, rateLimit: { ratePerSec: 50, burst: 50 } },
      { id: 'history-node', wsUrl: historyNode.wsUrl, roles: ['history'], poolSize: 2, maxPathConns: 0, maxSubscribeConns: 0, rateLimit: { ratePerSec: 50, burst: 50 } },
      { id: 'submit-node', wsUrl: submitNode.wsUrl, roles: ['submit'], poolSize: 2, maxPathConns: 0, maxSubscribeConns: 0, rateLimit: { ratePerSec: 50, burst: 50 } },
      { id: 'path-node', wsUrl: pathNode.wsUrl, roles: ['path'], poolSize: 2, maxPathConns: 5, maxSubscribeConns: 0, rateLimit: { ratePerSec: 50, burst: 50 } },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  const proxyUrl = `ws://127.0.0.1:${addr.port}`;

  await waitFor(() => proxy.nodeManager.getNode('normal-node').poolConnectedCount === 2);

  const clientA = await connectClient(proxyUrl);
  const clientB = await connectClient(proxyUrl);

  // Acceptance 1: verify each role lands on its dedicated node
  clientA.send(JSON.stringify({ id: 1, command: 'server_info', tag: 'A-normal' }));
  clientB.send(JSON.stringify({ id: 1, command: 'server_info', tag: 'B-normal' }));
  const [resA1, resB1] = await Promise.all([clientA.nextMessage(), clientB.nextMessage()]);

  assert.equal(resA1.id, 1);
  assert.equal(resA1.result.served_by, 'normal-node');
  assert.equal(resA1.result.echo_tag, 'A-normal');

  assert.equal(resB1.id, 1);
  assert.equal(resB1.result.served_by, 'normal-node');
  assert.equal(resB1.result.echo_tag, 'B-normal');

  // Verify history, submit, and ripple_path_find routing
  clientA.send(JSON.stringify({ id: 0, command: 'account_tx', tag: 'hist-zero-id' }));
  const histRes = await clientA.nextMessage();
  assert.equal(histRes.id, 0);
  assert.equal(histRes.result.served_by, 'history-node');

  clientA.send(JSON.stringify({ command: 'submit', tx_blob: '00FF', tag: 'no-id-submit' }));
  const subRes = await clientA.nextMessage();
  assert.equal('id' in subRes, false);
  assert.equal(subRes.result.served_by, 'submit-node');

  clientA.send(JSON.stringify({ id: 'pf-1', command: 'ripple_path_find' }));
  const rpfRes = await clientA.nextMessage();
  assert.equal(rpfRes.id, 'pf-1');
  assert.equal(rpfRes.result.served_by, 'path-node');

  clientA.close();
  clientB.close();
  await proxy.shutdown();
  await Promise.all([normalNode.close(), historyNode.close(), submitNode.close(), pathNode.close()]);
});

test('E2E Acceptance 3 & 4: Strict path_find 1-to-1 isolation, close lifecycle, and maxPathConns tooBusy guard', async () => {
  const pathNode = await startMockXrpldNode('path-node');

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    nodes: [
      { id: 'path-node', wsUrl: pathNode.wsUrl, roles: ['path', 'normal'], poolSize: 1, maxPathConns: 2, maxSubscribeConns: 5, rateLimit: { ratePerSec: 50, burst: 50 } },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  const proxyUrl = `ws://127.0.0.1:${addr.port}`;

  await waitFor(() => proxy.nodeManager.getNode('path-node').poolConnectedCount === 1);

  const clientA = await connectClient(proxyUrl);
  const clientB = await connectClient(proxyUrl);
  const clientC = await connectClient(proxyUrl);

  // Client A & B create isolated path_find subscriptions
  clientA.send(JSON.stringify({ id: 10, command: 'path_find', subcommand: 'create', source_account: 'rAlice', destination_account: 'rBob' }));
  clientB.send(JSON.stringify({ id: 10, command: 'path_find', subcommand: 'create', source_account: 'rCarol', destination_account: 'rDave' }));

  const ackA = await clientA.nextMessage();
  const ackB = await clientB.nextMessage();
  assert.equal(ackA.id, 10);
  assert.equal(ackA.result.source, 'rAlice');
  assert.equal(ackB.id, 10);
  assert.equal(ackB.result.source, 'rCarol');

  // Verify 1 pool conn + 2 dedicated path_find conns = 3 sockets on mock pathNode
  assert.equal(proxy.nodeManager.getNode('path-node').activePathConns, 2);
  assert.equal(pathNode.activeSockets.size, 3);

  // Acceptance 4: 3rd client exceeds maxPathConns=2 -> fail-fast tooBusy without opening a 4th socket
  clientC.send(JSON.stringify({ id: 99, command: 'path_find', subcommand: 'create', source_account: 'rEve', destination_account: 'rFrank' }));
  const errC = await clientC.nextMessage();
  assert.equal(errC.id, 99);
  assert.equal(errC.status, 'error');
  assert.equal(errC.error, 'tooBusy');
  assert.equal(pathNode.activeSockets.size, 3);

  // Verify isolation: clientA receives pushes only for rAlice
  const pushA = await clientA.nextMessage();
  assert.equal(pushA.type, 'path_find');
  assert.equal(pushA.source_account, 'rAlice');

  // Client A closes path_find -> upstream close response forwarded, then upstream socket closes & slot freed
  clientA.send(JSON.stringify({ id: 11, command: 'path_find', subcommand: 'close' }));
  let closeAckA = null;
  for (let i = 0; i < 10; i += 1) {
    const m = await clientA.nextMessage();
    if (m.type === 'response' && m.id === 11) {
      closeAckA = m;
      break;
    }
  }
  assert.ok(closeAckA);
  assert.equal(closeAckA.result.closed, true);

  await waitFor(() => proxy.nodeManager.getNode('path-node').activePathConns === 1);
  await waitFor(() => pathNode.activeSockets.size === 2);

  // Client B still receives its own pushes uninterrupted
  const pushB = await clientB.nextMessage();
  assert.equal(pushB.type, 'path_find');
  assert.equal(pushB.source_account, 'rCarol');

  // Now Client C can acquire the freed slot!
  clientC.send(JSON.stringify({ id: 100, command: 'path_find', subcommand: 'create', source_account: 'rEve', destination_account: 'rFrank' }));
  const ackC = await clientC.nextMessage();
  assert.equal(ackC.id, 100);
  assert.equal(ackC.result.source, 'rEve');
  assert.equal(proxy.nodeManager.getNode('path-node').activePathConns, 2);

  clientA.close();
  clientB.close();
  clientC.close();
  await proxy.shutdown();
  await pathNode.close();
});

test('E2E Acceptance 5: Rate limit spillover across same-role nodes and slowDown when all exhausted', async () => {
  const nodeA = await startMockXrpldNode('node-a');
  const nodeB = await startMockXrpldNode('node-b');

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    limits: { queueMaxWaitMs: 120, queueMaxLength: 2, requestTimeoutMs: 5000 },
    cache: { enabled: false },
    nodes: [
      { id: 'node-a', wsUrl: nodeA.wsUrl, roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 0.01, burst: 1 } },
      { id: 'node-b', wsUrl: nodeB.wsUrl, roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 0.01, burst: 2 } },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  await waitFor(
    () =>
      proxy.nodeManager.getNode('node-a').poolConnectedCount === 1 &&
      proxy.nodeManager.getNode('node-b').poolConnectedCount === 1,
  );

  const client = await connectClient(`ws://127.0.0.1:${addr.port}`);

  // Send 3 requests (burst total = 1 + 2 = 3): traffic should hit node-b first (highest tokens=2), then spill to node-a!
  const responses = [];
  for (let i = 1; i <= 3; i += 1) {
    client.send(JSON.stringify({ id: i, command: 'ping' }));
    responses.push(await client.nextMessage());
  }
  const servedBy = responses.map((r) => r.result.served_by);
  assert.equal(servedBy.filter((s) => s === 'node-b').length, 2);
  assert.equal(servedBy.filter((s) => s === 'node-a').length, 1);

  // Now all buckets are exhausted; 3 concurrent requests -> 2 queued (timeout slowDown), 3rd hits queueMaxLength (immediate slowDown)
  client.send(JSON.stringify({ id: 4, command: 'ping' }));
  client.send(JSON.stringify({ id: 5, command: 'ping' }));
  client.send(JSON.stringify({ id: 6, command: 'ping' }));

  const errs = await Promise.all([client.nextMessage(), client.nextMessage(), client.nextMessage()]);
  for (const err of errs) {
    assert.equal(err.status, 'error');
    assert.equal(err.error, 'slowDown');
  }

  client.close();
  await proxy.shutdown();
  await Promise.all([nodeA.close(), nodeB.close()]);
});

test('E2E Acceptance 6: Upstream failover and circuit breaker trip + recovery', async () => {
  const nodeA = await startMockXrpldNode('node-a');
  const nodeB = await startMockXrpldNode('node-b');

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    limits: {
      circuitBreakerThreshold: 2,
      nodeCooldownMs: 30000,
      reconnectBaseDelayMs: 50,
      reconnectMaxDelayMs: 100,
    },
    cache: { enabled: false },
    nodes: [
      { id: 'node-a', wsUrl: nodeA.wsUrl, roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 50, burst: 50 } },
      { id: 'node-b', wsUrl: nodeB.wsUrl, roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 50, burst: 20 } },
    ],
  });

  const logs = [];
  const captureLogger = {
    info: (m) => logs.push(m),
    warn: (m) => logs.push(m),
    error: (m) => logs.push(m),
  };

  const proxy = new ProxyServer(cfg, { logger: captureLogger });
  const addr = await proxy.start();
  await waitFor(
    () =>
      proxy.nodeManager.getNode('node-a').poolConnectedCount === 1 &&
      proxy.nodeManager.getNode('node-b').poolConnectedCount === 1,
  );

  // Shut down nodeA completely
  await nodeA.close();

  // Wait until nodeA trips circuit breaker OPEN from reconnect/close failures
  await waitFor(() => proxy.nodeManager.getNode('node-a').state === 'OPEN', 3000);
  assert.ok(logs.some((line) => line.includes('Circuit breaker OPEN') && line.includes('node-a')));

  // Subsequent client requests seamlessly route to healthy node-b
  const client = await connectClient(`ws://127.0.0.1:${addr.port}`);
  client.send(JSON.stringify({ id: 77, command: 'account_info', account: 'rTest' }));
  const res = await client.nextMessage();
  assert.equal(res.id, 77);
  assert.equal(res.status, 'success');
  assert.equal(res.result.served_by, 'node-b');

  client.close();
  await proxy.shutdown();
  await nodeB.close();
});

test('E2E Extra: ReadCache Singleflight coalesces concurrent fee requests while restoring distinct client IDs', async () => {
  let upstreamHitCount = 0;
  const normalNode = await startMockXrpldNode('normal-node', {
    onMessage: ({ ws, msg }) => {
      if (msg.command === 'fee') {
        upstreamHitCount += 1;
        setTimeout(() => {
          ws.send(
            JSON.stringify({
              id: msg.id,
              status: 'success',
              type: 'response',
              result: { drops: { base_fee: '10' } },
            }),
          );
        }, 40);
        return true;
      }
      return false;
    },
  });

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    cache: { enabled: true, ttlMs: 1000, commands: ['fee', 'server_info', 'ledger_closed'] },
    nodes: [
      { id: 'normal-node', wsUrl: normalNode.wsUrl, roles: ['normal'], poolSize: 1, rateLimit: { ratePerSec: 50, burst: 50 } },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  await waitFor(() => proxy.nodeManager.getNode('normal-node').poolConnectedCount === 1);

  const clientA = await connectClient(`ws://127.0.0.1:${addr.port}`);
  const clientB = await connectClient(`ws://127.0.0.1:${addr.port}`);

  clientA.send(JSON.stringify({ id: 'alpha', command: 'fee' }));
  clientB.send(JSON.stringify({ id: 0, command: 'fee' }));

  const [resA, resB] = await Promise.all([clientA.nextMessage(), clientB.nextMessage()]);
  assert.equal(resA.id, 'alpha');
  assert.equal(resB.id, 0);
  assert.equal(upstreamHitCount, 1);

  clientA.close();
  clientB.close();
  await proxy.shutdown();
  await normalNode.close();
});

test('E2E Extra: Upstream warning:"load" halves token rate and subscribe crash closes client with 1011', async () => {
  let subSocket = null;
  const normalNode = await startMockXrpldNode('normal-node', {
    onMessage: ({ ws, msg }) => {
      if (msg.command === 'ping') {
        ws.send(
          JSON.stringify({
            id: msg.id,
            status: 'success',
            type: 'response',
            warning: 'load',
            result: {},
          }),
        );
        return true;
      }
      if (msg.command === 'subscribe') {
        subSocket = ws;
        ws.send(JSON.stringify({ id: msg.id, status: 'success', type: 'response', result: {} }));
        return true;
      }
      return false;
    },
  });

  const cfg = createConfig({
    server: { host: '127.0.0.1', port: 0, statsLogIntervalMs: 0, heartbeatIntervalMs: 0 },
    limits: { slowDownRecoveryMs: 5000 },
    cache: { enabled: false },
    nodes: [
      { id: 'normal-node', wsUrl: normalNode.wsUrl, roles: ['normal'], poolSize: 1, maxSubscribeConns: 5, rateLimit: { ratePerSec: 20, burst: 20 } },
    ],
  });

  const proxy = new ProxyServer(cfg);
  const addr = await proxy.start();
  await waitFor(() => proxy.nodeManager.getNode('normal-node').poolConnectedCount === 1);

  const client = await connectClient(`ws://127.0.0.1:${addr.port}`);
  client.send(JSON.stringify({ id: 1, command: 'ping' }));
  const pingRes = await client.nextMessage();
  assert.equal(pingRes.warning, 'load');
  assert.equal(proxy.nodeManager.getNode('normal-node').tokenBucket.getEffectiveRate(), 10);

  client.send(JSON.stringify({ id: 2, command: 'subscribe', streams: ['ledger'] }));
  const subRes = await client.nextMessage();
  assert.equal(subRes.id, 2);
  assert.ok(subSocket);

  const closePromise = new Promise((resolve) => {
    client.once('close', (code) => resolve(code));
  });
  subSocket.terminate();
  const closeCode = await closePromise;
  assert.equal(closeCode, 1011);

  await proxy.shutdown();
  await normalNode.close();
});

