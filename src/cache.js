import { config as defaultConfig } from './config.js';

/**
 * Singleflight + short-TTL micro-cache for global read-only XRPL commands
 * (e.g. `fee`, `server_info`, `ledger_closed`).
 */
export class ReadCache {
  /**
   * @param {typeof defaultConfig.cache} [cacheCfg]
   */
  constructor(cacheCfg = defaultConfig.cache) {
    this.enabled = Boolean(cacheCfg?.enabled);
    this.ttlMs = Number(cacheCfg?.ttlMs ?? 1500);
    this.commands = new Set(cacheCfg?.commands || ['fee', 'server_info', 'ledger_closed']);
    /** @type {Map<string, { value: any, expiresAt: number }>} */
    this.store = new Map();
    /** @type {Map<string, Promise<any>>} */
    this.inFlight = new Map();
    this.hits = 0;
    this.coalesced = 0;
  }

  /**
   * Determine if a payload is eligible for micro-cache / singleflight.
   * @param {Record<string, any>} payload
   */
  isEligible(payload) {
    if (!this.enabled) return false;
    if (!payload || typeof payload.command !== 'string') return false;
    return this.commands.has(payload.command);
  }

  /**
   * Compute canonical cache key ignoring request `id`.
   * @param {Record<string, any>} payload
   */
  makeKey(payload) {
    const entries = Object.entries(payload)
      .filter(([k]) => k !== 'id')
      .sort(([a], [b]) => a.localeCompare(b));
    return JSON.stringify(entries);
  }

  /**
   * Resolve cached/singleflight value or run fetcher once for concurrent identical callers.
   * Never caches error responses (`status === 'error'`).
   * @param {Record<string, any>} payload
   * @param {() => Promise<any>} fetcher
   */
  async getOrFetch(payload, fetcher) {
    if (!this.isEligible(payload)) {
      return fetcher();
    }

    const key = this.makeKey(payload);
    const now = Date.now();
    const cached = this.store.get(key);
    if (cached && cached.expiresAt > now) {
      this.hits += 1;
      return structuredClone(cached.value);
    }
    if (cached) {
      this.store.delete(key);
    }

    const pending = this.inFlight.get(key);
    if (pending) {
      this.coalesced += 1;
      const result = await pending;
      return structuredClone(result);
    }

    const promise = (async () => {
      try {
        const result = await fetcher();
        if (result && result.status !== 'error' && !result.error) {
          const cleanCopy = structuredClone(result);
          delete cleanCopy.id;
          this.store.set(key, {
            value: cleanCopy,
            expiresAt: Date.now() + this.ttlMs,
          });
        }
        return result;
      } finally {
        this.inFlight.delete(key);
      }
    })();

    this.inFlight.set(key, promise);
    const res = await promise;
    return structuredClone(res);
  }

  clear() {
    this.store.clear();
    this.inFlight.clear();
  }

  getStats() {
    return {
      enabled: this.enabled,
      entries: this.store.size,
      inFlight: this.inFlight.size,
      hits: this.hits,
      coalesced: this.coalesced,
    };
  }
}
