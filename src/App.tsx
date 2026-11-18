import { useEffect, useRef, useState, useCallback } from 'react';
import {
  Activity,
  Terminal,
  GitBranch,
  CheckSquare,
  Play,
  RefreshCw,
  Power,
  AlertTriangle,
  Send,
  Trash2,
  Zap,
} from 'lucide-react';

type TabId = 'topology' | 'workbench' | 'routing' | 'tests';

interface NodeStat {
  id: string;
  wsUrl: string;
  roles: string[];
  state: 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  mockOnline: boolean;
  poolConnectedCount: number;
  poolSize: number;
  activePathConns: number;
  maxPathConns: number;
  activeSubscribeConns: number;
  maxSubscribeConns: number;
  tokens: number;
  burst: number;
  effectiveRatePerSec: number;
  baseRatePerSec: number;
  qps: number;
  totalRequests: number;
  totalErrors: number;
  totalTimeouts: number;
  loadWarnings: number;
  consecutiveFailures: number;
  cooldownRemainingMs: number;
  lastError: string | null;
}

interface ProxyStats {
  clientConnections: number;
  uniqueClientIps: number;
  queueLength: number;
  pool: {
    pendingRequests: number;
    nextProxyId: number;
  };
  cache: {
    enabled: boolean;
    entries: number;
    inFlight: number;
    hits: number;
    coalesced: number;
  };
  nodes: NodeStat[];
  routing?: {
    pathCommands: string[];
    historyCommands: string[];
    submitCommands: string[];
    statefulCommands: string[];
    routeExplicitLedgerToHistory: boolean;
    defaultWeight: number;
    commandWeights: Record<string, number>;
  };
  limits?: Record<string, number>;
}

interface WsLogEntry {
  seq: number;
  timestamp: string;
  direction: 'outbound' | 'inbound' | 'system';
  clientLabel: string;
  payload: string;
  latencyMs?: number;
}

const PRESET_COMMANDS: Array<{ label: string; payload: Record<string, unknown> }> = [
  { label: 'server_info (normal, cached)', payload: { id: 1, command: 'server_info' } },
  { label: 'fee (normal, cached)', payload: { id: 2, command: 'fee' } },
  { label: 'account_tx (history)', payload: { id: 3, command: 'account_tx', account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe' } },
  { label: 'submit (submit)', payload: { id: 4, command: 'submit', tx_blob: '12000022800000002400000001' } },
  { label: 'ripple_path_find (path stateless)', payload: { id: 5, command: 'ripple_path_find', source_account: 'rAlice', destination_account: 'rBob' } },
  { label: 'path_find create (path stateful)', payload: { id: 6, command: 'path_find', subcommand: 'create', source_account: 'rAlice', destination_account: 'rBob', destination_amount: '1000000' } },
  { label: 'path_find close (path release)', payload: { id: 7, command: 'path_find', subcommand: 'close' } },
  { label: 'subscribe ledger (stateful)', payload: { id: 8, command: 'subscribe', streams: ['ledger'] } },
  { label: 'unsubscribe ledger', payload: { id: 9, command: 'unsubscribe', streams: ['ledger'] } },
  { label: 'id: 0 boundary test', payload: { id: 0, command: 'account_info', account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe' } },
  { label: 'explicit ledger_index -> history', payload: { id: 10, command: 'account_info', account: 'rPT1Sjq2YGrBMTttX4GZHjKu9dyfzbpAYe', ledger_index: 91842000 } },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>('topology');
  const [stats, setStats] = useState<ProxyStats | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);
  const [loadingStats, setLoadingStats] = useState(true);

  // WebSocket Workbench state
  const [wsConnected, setWsConnected] = useState(false);
  const [commandJson, setCommandJson] = useState(
    JSON.stringify(PRESET_COMMANDS[0].payload, null, 2),
  );
  const [wsLogs, setWsLogs] = useState<WsLogEntry[]>([]);
  const wsRef = useRef<WebSocket | null>(null);
  const logSeqRef = useRef(1);
  const sentAtRef = useRef<Map<string, number>>(new Map());

  // Acceptance suite runner state
  const [testRunning, setTestRunning] = useState(false);
  const [testResult, setTestResult] = useState<{ ok: boolean; output: string } | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch('/api/stats');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setStats(data);
      setStatsError(null);
    } catch (err) {
      setStatsError(err instanceof Error ? err.message : 'Failed to load proxy metrics');
    } finally {
      setLoadingStats(false);
    }
  }, []);

  useEffect(() => {
    fetchStats();
    const id = setInterval(fetchStats, 1500);
    return () => clearInterval(id);
  }, [fetchStats]);

  const appendLog = useCallback(
    (direction: WsLogEntry['direction'], clientLabel: string, payload: string, latencyMs?: number) => {
      const entry: WsLogEntry = {
        seq: logSeqRef.current++,
        timestamp: new Date().toLocaleTimeString('en-US', { hour12: false }),
        direction,
        clientLabel,
        payload,
        latencyMs,
      };
      setWsLogs((prev) => [entry, ...prev].slice(0, 80));
    },
    [],
  );

  const getProxyWsUrl = () => {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${proto}//${window.location.host}/ws`;
  };

  const connectPrimaryWs = useCallback(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.close();
      return;
    }
    const ws = new WebSocket(getProxyWsUrl());
    wsRef.current = ws;

    ws.onopen = () => {
      setWsConnected(true);
      appendLog('system', 'client-1', 'Connected to /ws proxy endpoint');
    };

    ws.onmessage = (ev) => {
      let latency: number | undefined;
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed.id !== undefined && parsed.id !== null) {
          const key = String(parsed.id);
          const t0 = sentAtRef.current.get(key);
          if (t0 !== undefined) {
            latency = Math.round(performance.now() - t0);
            sentAtRef.current.delete(key);
          }
        }
      } catch {}
      appendLog('inbound', 'client-1', ev.data, latency);
      fetchStats();
    };

    ws.onclose = (ev) => {
      setWsConnected(false);
      wsRef.current = null;
      appendLog('system', 'client-1', `Socket closed (code=${ev.code}${ev.reason ? `, reason=${ev.reason}` : ''})`);
    };

    ws.onerror = () => {
      appendLog('system', 'client-1', 'WebSocket transport error');
    };
  }, [appendLog, fetchStats]);

  useEffect(() => {
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, []);

  const sendRawCommand = () => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      appendLog('system', 'client-1', 'Not connected. Click "Connect WS" first.');
      return;
    }
    try {
      const parsed = JSON.parse(commandJson);
      if (parsed && typeof parsed === 'object' && parsed.id !== undefined && parsed.id !== null) {
        sentAtRef.current.set(String(parsed.id), performance.now());
      }
    } catch {}
    wsRef.current.send(commandJson);
    appendLog('outbound', 'client-1', commandJson);
  };

  const runMultiplexCollisionDemo = async () => {
    const url = getProxyWsUrl();
    const c1 = new WebSocket(url);
    const c2 = new WebSocket(url);

    await Promise.all([
      new Promise<void>((res) => (c1.onopen = () => res())),
      new Promise<void>((res) => (c2.onopen = () => res())),
    ]);

    const p1 = JSON.stringify({ id: 1, command: 'account_info', account: 'rClientOneAccount' });
    const p2 = JSON.stringify({ id: 1, command: 'account_tx', account: 'rClientTwoHistory' });

    c1.onmessage = (ev) => {
      appendLog('inbound', 'concurrent-A (id:1)', ev.data);
      c1.close();
    };
    c2.onmessage = (ev) => {
      appendLog('inbound', 'concurrent-B (id:1)', ev.data);
      c2.close();
    };

    appendLog('outbound', 'concurrent-A (id:1)', p1);
    appendLog('outbound', 'concurrent-B (id:1)', p2);
    c1.send(p1);
    c2.send(p2);
  };

  const runBurstSpilloverDemo = () => {
    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      appendLog('system', 'client-1', 'Connect WS first to run burst spillover test.');
      return;
    }
    for (let i = 1; i <= 12; i += 1) {
      const payload = JSON.stringify({ id: `burst-${i}`, command: 'book_offers', taker_gets: { currency: 'XRP' } });
      wsRef.current.send(payload);
    }
    appendLog('outbound', 'client-1', 'Sent 12 concurrent book_offers (weight=4, total 48 tokens) to demonstrate spillover');
  };

  const handleToggleOnline = async (nodeId: string) => {
    await fetch(`/api/nodes/${encodeURIComponent(nodeId)}/toggle-online`, { method: 'POST' });
    fetchStats();
  };

  const handleToggleCircuit = async (node: NodeStat) => {
    const nextState = node.state === 'OPEN' ? 'CLOSED' : 'OPEN';
    await fetch(`/api/nodes/${encodeURIComponent(node.id)}/circuit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state: nextState }),
    });
    fetchStats();
  };

  const handleInjectLoadWarning = async (nodeId: string) => {
    await fetch(`/api/nodes/${encodeURIComponent(nodeId)}/load-warning`, { method: 'POST' });
    fetchStats();
  };

  const handleRunAcceptanceTests = async () => {
    setActiveTab('tests');
    setTestRunning(true);
    try {
      const res = await fetch('/api/run-tests', { method: 'POST' });
      const data = await res.json();
      setTestResult(data);
    } catch (err) {
      setTestResult({
        ok: false,
        output: err instanceof Error ? err.message : 'Failed to execute test suite',
      });
    } finally {
      setTestRunning(false);
    }
  };

  const navItems: Array<{ id: TabId; label: string; icon: typeof Activity }> = [
    { id: 'topology', label: 'Node Topology', icon: Activity },
    { id: 'workbench', label: 'WS Workbench', icon: Terminal },
    { id: 'routing', label: 'Routing & Config', icon: GitBranch },
    { id: 'tests', label: 'Acceptance Suite', icon: CheckSquare },
  ];

  const totalQps = stats ? stats.nodes.reduce((acc, n) => acc + n.qps, 0).toFixed(1) : '0.0';
  const healthyNodes = stats ? stats.nodes.filter((n) => n.state !== 'OPEN' && n.mockOnline).length : 0;

  return (
    <div className="min-h-screen flex bg-slate-50 text-slate-900">
      {/* Sidebar */}
      <aside className="w-60 shrink-0 border-r border-slate-200 bg-white flex flex-col">
        <div className="h-14 px-5 flex items-center border-b border-slate-200">
          <span className="text-base font-bold tracking-tight text-slate-900 whitespace-nowrap">
            xrpld-ws-proxy
          </span>
        </div>

        <nav className="p-3 space-y-1 flex-1" aria-label="Primary navigation">
          {navItems.map((item) => {
            const Icon = item.icon;
            const active = activeTab === item.id;
            return (
              <button
                key={item.id}
                type="button"
                onClick={() => setActiveTab(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap ${
                  active
                    ? 'bg-slate-900 text-white'
                    : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
                }`}
              >
                <Icon className="w-4 h-4 shrink-0" />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>

        <div className="p-4 border-t border-slate-200 text-xs text-slate-500 space-y-1.5">
          <div className="flex justify-between">
            <span>WS Endpoint</span>
            <span className="font-mono text-slate-800">/ws</span>
          </div>
          <div className="flex justify-between">
            <span>Standalone Entry</span>
            <span className="font-mono text-slate-800">src/index.js</span>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex-1 min-w-0 flex flex-col">
        {/* Top Bar Contract */}
        <header className="h-14 px-6 bg-white border-b border-slate-200 flex items-center justify-between gap-8">
          <div className="text-sm text-slate-500 whitespace-nowrap truncate">
            <span className="text-slate-900 font-semibold">xrpld-ws-proxy</span>
            <span className="mx-2" aria-hidden="true">/</span>
            <span>
              {activeTab === 'topology' && 'Node Topology & Pool Telemetry'}
              {activeTab === 'workbench' && 'Live WebSocket Proxy Workbench'}
              {activeTab === 'routing' && 'Command Routing & Token Weights'}
              {activeTab === 'tests' && 'Node.js Acceptance Test Suite'}
            </span>
          </div>

          <div className="flex items-center gap-3 shrink-0">
            <button
              type="button"
              onClick={handleRunAcceptanceTests}
              disabled={testRunning}
              className="px-4 py-2 text-xs font-semibold text-white bg-blue-700 hover:bg-blue-800 disabled:opacity-50 rounded-md transition-colors whitespace-nowrap shrink-0 flex items-center gap-2"
            >
              <Play className="w-3.5 h-3.5" />
              <span>{testRunning ? 'Running Suite...' : 'Run Acceptance Suite'}</span>
            </button>
          </div>
        </header>

        <main className="flex-1 p-6 max-w-[1200px] w-full mx-auto space-y-6">
          {statsError && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-md text-sm text-red-800 flex items-center justify-between">
              <span>Telemetry connection error: {statsError}</span>
              <button
                type="button"
                onClick={fetchStats}
                className="px-3 py-1.5 text-xs font-medium bg-white border border-red-200 rounded hover:bg-red-100"
              >
                Retry
              </button>
            </div>
          )}

          {/* KPI Strip */}
          <section className="grid grid-cols-2 lg:grid-cols-4 gap-4" aria-label="Proxy summary metrics">
            <div className="bg-white border border-slate-200 rounded-md p-4">
              <div className="text-xs text-slate-500">Healthy Nodes</div>
              <div className="mt-1 text-2xl font-semibold font-mono tabular-nums text-slate-900">
                {stats ? `${healthyNodes} / ${stats.nodes.length}` : '--'}
              </div>
              <div className="mt-1 text-xs text-slate-500">Circuit-closed &amp; reachable</div>
            </div>

            <div className="bg-white border border-slate-200 rounded-md p-4">
              <div className="text-xs text-slate-500">Active Clients · Aggregate QPS</div>
              <div className="mt-1 text-2xl font-semibold font-mono tabular-nums text-slate-900">
                {stats ? `${stats.clientConnections} · ${totalQps}/s` : '--'}
              </div>
              <div className="mt-1 text-xs text-slate-500 font-mono tabular-nums">
                {stats ? `${stats.uniqueClientIps} unique IPs` : 'Waiting for telemetry'}
              </div>
            </div>

            <div className="bg-white border border-slate-200 rounded-md p-4">
              <div className="text-xs text-slate-500">Multiplexer · Rate Queue</div>
              <div className="mt-1 text-2xl font-semibold font-mono tabular-nums text-slate-900">
                {stats ? `${stats.pool.pendingRequests} · ${stats.queueLength}` : '--'}
              </div>
              <div className="mt-1 text-xs text-slate-500 font-mono tabular-nums">
                {stats ? `Next proxyId #${stats.pool.nextProxyId}` : 'In-flight / queued'}
              </div>
            </div>

            <div className="bg-white border border-slate-200 rounded-md p-4">
              <div className="text-xs text-slate-500">Read Cache · Singleflight</div>
              <div className="mt-1 text-2xl font-semibold font-mono tabular-nums text-slate-900">
                {stats ? `${stats.cache.hits} hits · ${stats.cache.coalesced}` : '--'}
              </div>
              <div className="mt-1 text-xs text-slate-500 font-mono tabular-nums">
                {stats ? `${stats.cache.entries} cached keys` : '1500ms TTL micro-cache'}
              </div>
            </div>
          </section>

          {/* TAB 1: NODE TOPOLOGY */}
          {activeTab === 'topology' && (
            <section className="bg-white border border-slate-200 rounded-md overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
                <div>
                  <h2 className="text-base font-semibold text-slate-900">Backend xrpld Node Pool</h2>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Real-time stateless connection pools, stateful path_find/subscribe slots, token buckets, and circuit breakers.
                  </p>
                </div>
                <button
                  type="button"
                  onClick={fetchStats}
                  className="px-3 py-1.5 text-xs font-medium text-slate-700 bg-slate-100 hover:bg-slate-200 rounded-md flex items-center gap-1.5 whitespace-nowrap"
                >
                  <RefreshCw className="w-3.5 h-3.5" />
                  <span>Refresh</span>
                </button>
              </div>

              {loadingStats && !stats ? (
                <div className="p-8 text-sm text-slate-500">Loading node telemetry...</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-left border-collapse">
                    <thead>
                      <tr className="border-b border-slate-200 bg-slate-50 text-xs font-medium text-slate-500">
                        <th className="py-3 px-4">Node &amp; Roles</th>
                        <th className="py-3 px-4">Breaker State</th>
                        <th className="py-3 px-4 text-right">Pool WS</th>
                        <th className="py-3 px-4 text-right">Path Slots</th>
                        <th className="py-3 px-4 text-right">Sub Slots</th>
                        <th className="py-3 px-4 text-right">Tokens / Rate</th>
                        <th className="py-3 px-4 text-right">Reqs / Errs</th>
                        <th className="py-3 px-4 text-right">Fault Controls</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-200 text-sm">
                      {stats?.nodes.map((node) => {
                        const statusColor =
                          !node.mockOnline || node.state === 'OPEN'
                            ? 'text-red-700'
                            : node.state === 'HALF_OPEN'
                            ? 'text-amber-700'
                            : 'text-emerald-700';

                        return (
                          <tr key={node.id} className="hover:bg-slate-50 transition-colors">
                            <td className="py-3 px-4">
                              <div className="font-semibold text-slate-900">{node.id}</div>
                              <div className="text-xs text-slate-500 mt-0.5">
                                {node.roles.join(' · ')}
                              </div>
                            </td>
                            <td className="py-3 px-4">
                              <div className={`text-xs font-semibold font-mono ${statusColor}`}>
                                {node.mockOnline ? node.state : 'OFFLINE'}
                              </div>
                              <div className="text-xs text-slate-500 font-mono tabular-nums mt-0.5">
                                {node.state === 'OPEN'
                                  ? `cooldown ${Math.ceil(node.cooldownRemainingMs / 1000)}s`
                                  : `failures=${node.consecutiveFailures} · warn=${node.loadWarnings}`}
                              </div>
                            </td>
                            <td className="py-3 px-4 text-right font-mono tabular-nums text-xs">
                              {node.poolConnectedCount} / {node.poolSize}
                            </td>
                            <td className="py-3 px-4 text-right font-mono tabular-nums text-xs">
                              {node.activePathConns} / {node.maxPathConns}
                            </td>
                            <td className="py-3 px-4 text-right font-mono tabular-nums text-xs">
                              {node.activeSubscribeConns} / {node.maxSubscribeConns}
                            </td>
                            <td className="py-3 px-4 text-right font-mono tabular-nums text-xs">
                              <div>{node.tokens.toFixed(1)} / {node.burst}</div>
                              <div className="text-slate-500">{node.effectiveRatePerSec}/s</div>
                            </td>
                            <td className="py-3 px-4 text-right font-mono tabular-nums text-xs">
                              <div>{node.totalRequests} req</div>
                              <div className="text-slate-500">{node.totalErrors} err · {node.qps} qps</div>
                            </td>
                            <td className="py-3 px-4 text-right">
                              <div className="inline-flex items-center gap-1.5">
                                <button
                                  type="button"
                                  onClick={() => handleToggleOnline(node.id)}
                                  className="px-2.5 py-1 text-xs font-medium border border-slate-200 rounded bg-white hover:bg-slate-100 text-slate-700 whitespace-nowrap flex items-center gap-1"
                                  title="Simulate upstream node disconnect/recovery"
                                >
                                  <Power className="w-3 h-3" />
                                  <span>{node.mockOnline ? 'Stop' : 'Start'}</span>
                                </button>
                                <button
                                  type="button"
                                  onClick={() => handleToggleCircuit(node)}
                                  className="px-2.5 py-1 text-xs font-medium border border-slate-200 rounded bg-white hover:bg-slate-100 text-slate-700 whitespace-nowrap"
                                >
                                  {node.state === 'OPEN' ? 'Reset' : 'Trip'}
                                </button>
                                <button
                                  type="button"
                                  onClick={() => handleInjectLoadWarning(node.id)}
                                  className="px-2.5 py-1 text-xs font-medium border border-slate-200 rounded bg-white hover:bg-slate-100 text-slate-700 whitespace-nowrap flex items-center gap-1"
                                  title="Inject warning:load on next response"
                                >
                                  <AlertTriangle className="w-3 h-3" />
                                  <span>Load</span>
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </section>
          )}

          {/* TAB 2: WS WORKBENCH */}
          {activeTab === 'workbench' && (
            <section className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-stretch">
              <div className="lg:col-span-5 bg-white border border-slate-200 rounded-md p-5 flex flex-col space-y-4">
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <h2 className="text-base font-semibold text-slate-900">Client WebSocket</h2>
                    <p className="text-xs text-slate-500 font-mono mt-0.5">
                      {wsConnected ? 'Connected · /ws' : 'Disconnected · /ws'}
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={connectPrimaryWs}
                    className={`px-3.5 py-2 text-xs font-semibold rounded-md whitespace-nowrap transition-colors ${
                      wsConnected
                        ? 'bg-slate-200 text-slate-800 hover:bg-slate-300'
                        : 'bg-blue-700 text-white hover:bg-blue-800'
                    }`}
                  >
                    {wsConnected ? 'Disconnect WS' : 'Connect WS'}
                  </button>
                </div>

                <div>
                  <label htmlFor="preset-select" className="block text-xs font-medium text-slate-700 mb-1.5">
                    Command Preset
                  </label>
                  <select
                    id="preset-select"
                    onChange={(e) => {
                      const idx = Number(e.target.value);
                      if (!Number.isNaN(idx) && PRESET_COMMANDS[idx]) {
                        setCommandJson(JSON.stringify(PRESET_COMMANDS[idx].payload, null, 2));
                      }
                    }}
                    className="w-full px-3 py-2 text-sm bg-white border border-slate-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-600"
                  >
                    {PRESET_COMMANDS.map((preset, idx) => (
                      <option key={preset.label} value={idx}>
                        {preset.label}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="flex-1 flex flex-col">
                  <label htmlFor="payload-editor" className="block text-xs font-medium text-slate-700 mb-1.5">
                    JSON Request Frame
                  </label>
                  <textarea
                    id="payload-editor"
                    value={commandJson}
                    onChange={(e) => setCommandJson(e.target.value)}
                    rows={10}
                    className="w-full flex-1 p-3 font-mono text-xs bg-slate-900 text-slate-100 rounded-md border border-slate-800 focus:outline-none focus:ring-2 focus:ring-blue-600"
                  />
                </div>

                <div className="flex flex-wrap items-center gap-2 pt-1">
                  <button
                    type="button"
                    onClick={sendRawCommand}
                    className="px-4 py-2 text-xs font-semibold text-white bg-blue-700 hover:bg-blue-800 rounded-md flex items-center gap-1.5 whitespace-nowrap"
                  >
                    <Send className="w-3.5 h-3.5" />
                    <span>Send Frame</span>
                  </button>
                  <button
                    type="button"
                    onClick={runMultiplexCollisionDemo}
                    className="px-3 py-2 text-xs font-medium text-slate-700 bg-slate-100 hover:bg-slate-200 rounded-md whitespace-nowrap"
                  >
                    Test Concurrent id:1
                  </button>
                  <button
                    type="button"
                    onClick={runBurstSpilloverDemo}
                    className="px-3 py-2 text-xs font-medium text-slate-700 bg-slate-100 hover:bg-slate-200 rounded-md flex items-center gap-1 whitespace-nowrap"
                  >
                    <Zap className="w-3.5 h-3.5" />
                    <span>Burst 12x</span>
                  </button>
                </div>
              </div>

              <div className="lg:col-span-7 bg-white border border-slate-200 rounded-md flex flex-col overflow-hidden">
                <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between">
                  <div>
                    <h2 className="text-base font-semibold text-slate-900">Wire Frame Inspector</h2>
                    <p className="text-xs text-slate-500 mt-0.5">
                      Real-time outbound requests, multiplexed responses, and asynchronous path_find / ledger pushes.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => setWsLogs([])}
                    className="px-3 py-1.5 text-xs font-medium text-slate-600 hover:text-slate-900 bg-slate-100 hover:bg-slate-200 rounded-md flex items-center gap-1.5 whitespace-nowrap"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                    <span>Clear</span>
                  </button>
                </div>

                <div className="flex-1 min-h-[380px] max-h-[540px] overflow-y-auto divide-y divide-slate-100">
                  {wsLogs.length === 0 ? (
                    <div className="p-8 text-sm text-slate-500">
                      Connect WebSocket and send an XRPL command to inspect proxy routing, ID restoration, and stream isolation.
                    </div>
                  ) : (
                    wsLogs.map((log) => (
                      <div key={log.seq} className="px-4 py-3 text-xs space-y-1">
                        <div className="flex items-center justify-between text-slate-500 font-mono">
                          <span>
                            <span
                              className={`font-semibold ${
                                log.direction === 'outbound'
                                  ? 'text-blue-700'
                                  : log.direction === 'inbound'
                                  ? 'text-emerald-700'
                                  : 'text-amber-700'
                              }`}
                            >
                              {log.direction.toUpperCase()}
                            </span>
                            <span className="mx-2">·</span>
                            <span>{log.clientLabel}</span>
                            {log.latencyMs !== undefined && (
                              <>
                                <span className="mx-2">·</span>
                                <span>{log.latencyMs}ms</span>
                              </>
                            )}
                          </span>
                          <span>{log.timestamp}</span>
                        </div>
                        <pre className="p-2.5 bg-slate-50 border border-slate-200 rounded font-mono text-[11px] text-slate-800 overflow-x-auto">
                          {log.payload}
                        </pre>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </section>
          )}

          {/* TAB 3: ROUTING & CONFIG */}
          {activeTab === 'routing' && (
            <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-white border border-slate-200 rounded-md p-5 space-y-4">
                <div>
                  <h2 className="text-base font-semibold text-slate-900">Role Routing Rules</h2>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Static vs stateful separation and command classification from src/router.js.
                  </p>
                </div>

                <div className="divide-y divide-slate-200 text-sm">
                  <div className="py-3 flex justify-between gap-4">
                    <span className="font-semibold text-slate-900">path (stateful + stateless)</span>
                    <span className="font-mono text-xs text-slate-600 text-right">
                      {stats?.routing?.pathCommands.join(', ')}
                    </span>
                  </div>
                  <div className="py-3 flex justify-between gap-4">
                    <span className="font-semibold text-slate-900">history (stateless)</span>
                    <span className="font-mono text-xs text-slate-600 text-right">
                      {stats?.routing?.historyCommands.join(', ')}
                    </span>
                  </div>
                  <div className="py-3 flex justify-between gap-4">
                    <span className="font-semibold text-slate-900">submit (stateless)</span>
                    <span className="font-mono text-xs text-slate-600 text-right">
                      {stats?.routing?.submitCommands.join(', ')}
                    </span>
                  </div>
                  <div className="py-3 flex justify-between gap-4">
                    <span className="font-semibold text-slate-900">stateful 1-to-1 channels</span>
                    <span className="font-mono text-xs text-slate-600 text-right">
                      {stats?.routing?.statefulCommands.join(', ')}
                    </span>
                  </div>
                  <div className="py-3 flex justify-between gap-4">
                    <span className="font-semibold text-slate-900">Explicit ledger_hash / index</span>
                    <span className="font-mono text-xs text-slate-600">
                      {stats?.routing?.routeExplicitLedgerToHistory ? 'Routes to history' : 'Disabled'}
                    </span>
                  </div>
                </div>
              </div>

              <div className="bg-white border border-slate-200 rounded-md p-5 space-y-4">
                <div>
                  <h2 className="text-base font-semibold text-slate-900">Protection Limits &amp; Token Weights</h2>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Active thresholds enforced by NodeManager, StatelessPool, and StatefulChannel.
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-3 text-xs">
                  {stats?.limits &&
                    Object.entries(stats.limits).map(([k, v]) => (
                      <div key={k} className="p-2.5 bg-slate-50 border border-slate-200 rounded flex justify-between gap-2">
                        <span className="text-slate-600 truncate">{k}</span>
                        <span className="font-mono font-semibold text-slate-900 tabular-nums">{v}</span>
                      </div>
                    ))}
                </div>
              </div>
            </section>
          )}

          {/* TAB 4: ACCEPTANCE SUITE */}
          {activeTab === 'tests' && (
            <section className="bg-white border border-slate-200 rounded-md overflow-hidden">
              <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-4">
                <div>
                  <h2 className="text-base font-semibold text-slate-900">Node.js Built-in Test Runner Output</h2>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Executes <code className="font-mono">node --test test/router.test.js test/rateLimiter.test.js test/proxy.e2e.test.js</code>
                  </p>
                </div>
                <button
                  type="button"
                  onClick={handleRunAcceptanceTests}
                  disabled={testRunning}
                  className="px-3.5 py-2 text-xs font-semibold text-white bg-blue-700 hover:bg-blue-800 disabled:opacity-50 rounded-md whitespace-nowrap"
                >
                  {testRunning ? 'Executing...' : 'Re-run Tests'}
                </button>
              </div>

              <div className="p-5">
                {!testResult && !testRunning && (
                  <div className="text-sm text-slate-500">
                    Click &ldquo;Run Acceptance Suite&rdquo; to verify all 6 SPEC.md acceptance criteria against live Mock xrpld servers.
                  </div>
                )}
                {testRunning && (
                  <div className="text-sm text-slate-600 font-mono">
                    Running node --test suite...
                  </div>
                )}
                {testResult && !testRunning && (
                  <div className="space-y-3">
                    <div
                      className={`text-xs font-semibold font-mono ${
                        testResult.ok ? 'text-emerald-700' : 'text-red-700'
                      }`}
                    >
                      {testResult.ok ? 'PASSED · All unit and E2E acceptance tests succeeded' : 'FAILED · Review output below'}
                    </div>
                    <pre className="p-4 bg-slate-900 text-slate-100 rounded-md text-xs font-mono overflow-x-auto max-h-[480px] overflow-y-auto whitespace-pre-wrap">
                      {testResult.output}
                    </pre>
                  </div>
                )}
              </div>
            </section>
          )}
        </main>
      </div>
    </div>
  );
}
