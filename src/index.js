import { config } from './config.js';
import { ProxyServer } from './wsServer.js';

const proxy = new ProxyServer(config);

let statsTimer = null;

function formatStatsLine(stats) {
  const nodeSummary = stats.nodes
    .map(
      (n) =>
        `${n.id}[${n.state} pool=${n.poolConnectedCount}/${n.poolSize} path=${n.activePathConns}/${n.maxPathConns} sub=${n.activeSubscribeConns}/${n.maxSubscribeConns} tok=${n.tokens}/${n.burst} qps=${n.qps}]`,
    )
    .join(' | ');
  return `[Stats] clients=${stats.clientConnections} queue=${stats.queueLength} pending=${stats.pool.pendingRequests} | ${nodeSummary}`;
}

async function main() {
  const addr = await proxy.start();
  console.log(`[xrpld-ws-proxy] Listening on ws://${addr.host}:${addr.port}`);

  const statsIntervalMs = config.server.statsLogIntervalMs ?? 60000;
  if (statsIntervalMs > 0) {
    statsTimer = setInterval(() => {
      console.log(formatStatsLine(proxy.getStats()));
    }, statsIntervalMs);
    if (typeof statsTimer.unref === 'function') {
      statsTimer.unref();
    }
  }
}

let isExiting = false;
async function handleSignal(signal) {
  if (isExiting) return;
  isExiting = true;
  console.log(`[xrpld-ws-proxy] Received ${signal}, shutting down gracefully...`);
  if (statsTimer) {
    clearInterval(statsTimer);
    statsTimer = null;
  }
  try {
    await proxy.shutdown();
    console.log('[xrpld-ws-proxy] Shutdown complete.');
    process.exit(0);
  } catch (err) {
    console.error('[xrpld-ws-proxy] Error during shutdown:', err);
    process.exit(1);
  }
}

process.on('SIGINT', () => handleSignal('SIGINT'));
process.on('SIGTERM', () => handleSignal('SIGTERM'));

main().catch((err) => {
  console.error('[xrpld-ws-proxy] Fatal startup error:', err);
  process.exit(1);
});
