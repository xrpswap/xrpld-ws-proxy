/**
 * Per-node Token Bucket with weighted deduction and adaptive backpressure slowdown.
 */
export class TokenBucket {
  /**
   * @param {{ ratePerSec: number, burst: number }} options
   */
  constructor({ ratePerSec, burst }) {
    this.baseRatePerSec = Math.max(0, Number(ratePerSec) || 0);
    this.burst = Math.max(0, Number(burst) || 0);
    this.tokens = this.burst;
    this.lastRefillAt = Date.now();
    this.slowdownFactor = 1;
    this.slowdownUntil = 0;
  }

  /**
   * Refill tokens based on elapsed time and current slowdown state.
   * @param {number} [now]
   */
  refill(now = Date.now()) {
    if (this.slowdownFactor < 1 && now >= this.slowdownUntil) {
      this.slowdownFactor = 1;
      this.slowdownUntil = 0;
    }

    const elapsedSec = Math.max(0, (now - this.lastRefillAt) / 1000);
    if (elapsedSec > 0) {
      const effectiveRate = this.baseRatePerSec * this.slowdownFactor;
      this.tokens = Math.min(this.burst, this.tokens + elapsedSec * effectiveRate);
      this.lastRefillAt = now;
    }
    return this.tokens;
  }

  /**
   * Effective cost clamped to burst when burst > 0 so valid requests never deadlock on a full bucket.
   * @param {number} weight
   */
  effectiveWeight(weight) {
    const w = Math.max(1, Number(weight) || 1);
    if (this.burst <= 0) return w;
    return Math.min(w, this.burst);
  }

  /**
   * Get available tokens after refilling.
   * @param {number} [now]
   */
  getAvailableTokens(now = Date.now()) {
    return this.refill(now);
  }

  /**
   * Check whether a request of given weight can currently be served.
   * @param {number} weight
   * @param {number} [now]
   */
  canConsume(weight, now = Date.now()) {
    if (this.burst <= 0) return false;
    this.refill(now);
    return this.tokens >= this.effectiveWeight(weight);
  }

  /**
   * Attempt to consume weighted tokens.
   * @param {number} weight
   * @param {number} [now]
   * @returns {boolean}
   */
  tryConsume(weight, now = Date.now()) {
    if (this.burst <= 0) return false;
    this.refill(now);
    const cost = this.effectiveWeight(weight);
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }

  /**
   * Trigger adaptive backpressure: halve effective token refill rate temporarily.
   * @param {number} durationMs
   * @param {number} [now]
   */
  triggerSlowdown(durationMs = 15000, now = Date.now()) {
    this.refill(now);
    this.slowdownFactor = Math.max(0.25, this.slowdownFactor * 0.5);
    this.slowdownUntil = Math.max(this.slowdownUntil, now + durationMs);
  }

  /**
   * Effective refill rate per second.
   */
  getEffectiveRate() {
    this.refill();
    return this.baseRatePerSec * this.slowdownFactor;
  }
}

/**
 * Bounded waiting queue for rate-limited requests when all nodes for a role are exhausted.
 */
export class BoundedRateQueue {
  /**
   * @param {{ maxWaitMs: number, maxLength: number }} options
   * @param {(role: string, weight: number) => any} tryAcquireFn
   */
  constructor({ maxWaitMs = 800, maxLength = 200 }, tryAcquireFn) {
    this.maxWaitMs = maxWaitMs;
    this.maxLength = maxLength;
    this.tryAcquireFn = tryAcquireFn;
    /** @type {Array<{ role: string, weight: number, resolve: Function, reject: Function, timer: NodeJS.Timeout, settled: boolean }>} */
    this.queue = [];
    this.drainTimer = null;
    this.closed = false;
  }

  get length() {
    return this.queue.length;
  }

  /**
   * Enqueue a waiting acquisition request until tokens refill or timeout/full.
   * @param {string} role
   * @param {number} weight
   * @returns {Promise<any>}
   */
  enqueue(role, weight) {
    if (this.closed) {
      return Promise.reject(Object.assign(new Error('Rate queue is closed'), { code: 'slowDown' }));
    }

    if (this.queue.length >= this.maxLength) {
      return Promise.reject(
        Object.assign(new Error(`Rate limiter queue full (${this.maxLength})`), { code: 'slowDown' }),
      );
    }

    return new Promise((resolve, reject) => {
      const entry = {
        role,
        weight,
        resolve,
        reject,
        settled: false,
        timer: null,
      };

      entry.timer = setTimeout(() => {
        if (entry.settled) return;
        entry.settled = true;
        this.removeEntry(entry);
        reject(
          Object.assign(
            new Error(`Rate limit wait timeout exceeded (${this.maxWaitMs}ms)`),
            { code: 'slowDown' },
          ),
        );
      }, this.maxWaitMs);

      this.queue.push(entry);
      this.ensureDrainLoop();
    });
  }

  removeEntry(entry) {
    const idx = this.queue.indexOf(entry);
    if (idx !== -1) {
      this.queue.splice(idx, 1);
    }
    if (this.queue.length === 0 && this.drainTimer) {
      clearInterval(this.drainTimer);
      this.drainTimer = null;
    }
  }

  ensureDrainLoop() {
    if (this.drainTimer || this.closed) return;
    this.drainTimer = setInterval(() => {
      this.flush();
    }, 25);
    if (typeof this.drainTimer.unref === 'function') {
      this.drainTimer.unref();
    }
  }

  flush() {
    if (this.queue.length === 0) {
      if (this.drainTimer) {
        clearInterval(this.drainTimer);
        this.drainTimer = null;
      }
      return;
    }

    // Iterate FIFO; entries that can be served are resolved immediately.
    let i = 0;
    while (i < this.queue.length) {
      const entry = this.queue[i];
      if (entry.settled) {
        this.queue.splice(i, 1);
        continue;
      }

      const result = this.tryAcquireFn(entry.role, entry.weight);
      if (result && result.node) {
        entry.settled = true;
        clearTimeout(entry.timer);
        this.queue.splice(i, 1);
        entry.resolve(result.node);
      } else if (result && result.noNodes) {
        entry.settled = true;
        clearTimeout(entry.timer);
        this.queue.splice(i, 1);
        entry.reject(
          Object.assign(new Error(`No healthy nodes available for role "${entry.role}"`), {
            code: 'noNodesAvailable',
          }),
        );
      } else {
        i += 1;
      }
    }

    if (this.queue.length === 0 && this.drainTimer) {
      clearInterval(this.drainTimer);
      this.drainTimer = null;
    }
  }

  closeAll() {
    this.closed = true;
    if (this.drainTimer) {
      clearInterval(this.drainTimer);
      this.drainTimer = null;
    }
    while (this.queue.length > 0) {
      const entry = this.queue.shift();
      if (entry && !entry.settled) {
        entry.settled = true;
        clearTimeout(entry.timer);
        entry.reject(Object.assign(new Error('Proxy shutting down'), { code: 'upstreamUnavailable' }));
      }
    }
  }
}
