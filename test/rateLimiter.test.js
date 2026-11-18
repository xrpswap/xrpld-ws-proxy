import test from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucket, BoundedRateQueue } from '../src/rateLimiter.js';

test('TokenBucket supports weighted deduction and refill over time', async () => {
  const bucket = new TokenBucket({ ratePerSec: 10, burst: 10 });
  assert.equal(bucket.tryConsume(4), true);
  assert.equal(bucket.tryConsume(4), true);
  // Only ~2 tokens left, weight 4 should fail
  assert.equal(bucket.tryConsume(4), false);
  // Weight 2 should succeed
  assert.equal(bucket.tryConsume(2), true);
  assert.equal(bucket.tryConsume(1), false);

  // Wait 250ms -> ~2.5 tokens refilled
  await new Promise((r) => setTimeout(r, 260));
  assert.equal(bucket.tryConsume(2), true);
});

test('TokenBucket adaptive slowdown halves refill rate temporarily and recovers', async () => {
  const bucket = new TokenBucket({ ratePerSec: 20, burst: 20 });
  assert.equal(bucket.getEffectiveRate(), 20);

  bucket.triggerSlowdown(120);
  assert.equal(bucket.getEffectiveRate(), 10);

  await new Promise((r) => setTimeout(r, 150));
  assert.equal(bucket.getEffectiveRate(), 20);
});

test('BoundedRateQueue resolves when tokens refill and rejects with slowDown on timeout or full queue', async () => {
  let available = false;
  const queue = new BoundedRateQueue({ maxWaitMs: 120, maxLength: 2 }, () => {
    if (available) {
      available = false;
      return { node: { id: 'node-a' } };
    }
    return { waitNeeded: true };
  });

  // 1. Resolves once tokens become available before maxWaitMs
  const p1 = queue.enqueue('normal', 1);
  setTimeout(() => {
    available = true;
  }, 40);
  const res1 = await p1;
  assert.equal(res1.id, 'node-a');

  // 2. Rejects with slowDown when queue hits maxLength (2)
  const p2 = queue.enqueue('normal', 1);
  const p3 = queue.enqueue('normal', 1);
  await assert.rejects(
    queue.enqueue('normal', 1),
    (err) => err.code === 'slowDown',
  );

  // 3. p2 and p3 time out with slowDown
  await assert.rejects(p2, (err) => err.code === 'slowDown');
  await assert.rejects(p3, (err) => err.code === 'slowDown');
  queue.closeAll();
});
