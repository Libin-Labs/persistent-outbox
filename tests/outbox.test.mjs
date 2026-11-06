import test from 'node:test';
import assert from 'node:assert/strict';
import { createPersistentOutbox, OutboxError } from '../dist/esm/index.js';
import { fileAdapter } from './file-adapter.mjs';
const key = (lease) => ({ id: lease.id, token: lease.token, owner: lease.owner });

test('file-backed enqueue and ack survive adapter and library recreation', async () => {
  const backend = await fileAdapter('restart');
  const first = createPersistentOutbox(backend.adapter, { clock: () => 0 });
  assert.equal(await first.enqueue('operation-1', { amount: 2 }), 'enqueued');
  const reopened = createPersistentOutbox(backend.reopen(), { clock: () => 1 });
  const [lease] = await reopened.claim('worker');
  assert.deepEqual(lease, {
    id: 'operation-1',
    payload: { amount: 2 },
    owner: 'worker',
    token: '1',
    expiresAt: 30001,
    attempt: 1,
  });
  await reopened.ack(lease);
  const third = createPersistentOutbox(backend.reopen(), { clock: () => 2 });
  assert.equal((await third.inspect())[0].status, 'done');
  assert.equal(await third.enqueue('operation-1', { amount: 2 }), 'duplicate');
});
test('racing enqueue dedupes atomically across instances', async () => {
  const backend = await fileAdapter('enqueue-race');
  const a = createPersistentOutbox(backend.adapter);
  const b = createPersistentOutbox(backend.reopen());
  assert.deepEqual((await Promise.all([a.enqueue('same', 1), b.enqueue('same', 1)])).sort(), [
    'duplicate',
    'enqueued',
  ]);
  await assert.rejects(b.enqueue('same', 2), { code: 'conflict' });
  assert.equal((await a.inspect()).length, 1);
});
test('racing claims give only one worker the active lease', async () => {
  const backend = await fileAdapter('claim-race');
  const a = createPersistentOutbox(backend.adapter, { clock: () => 0 });
  const b = createPersistentOutbox(backend.reopen(), { clock: () => 0 });
  await a.enqueue('x', 1);
  const leases = await Promise.all([a.claim('a'), b.claim('b')]);
  assert.equal(leases.flat().length, 1);
  assert.equal((await a.inspect())[0].attempts, 1);
});
test('expired leases can be reclaimed and old fencing keys cannot ack fail or renew', async () => {
  const backend = await fileAdapter('expiry');
  let time = 0;
  const box = createPersistentOutbox(backend.adapter, { clock: () => time, leaseMs: 10 });
  await box.enqueue('x', 1);
  const [old] = await box.claim('a');
  time = 10;
  const [next] = await box.claim('b');
  assert.equal(next.token, '2');
  assert.equal(next.attempt, 2);
  for (const operation of [() => box.ack(old), () => box.fail(old, 'old'), () => box.renew(old)])
    await assert.rejects(operation(), { code: 'lease' });
  await box.ack(next);
});
test('ack at exact expiry rejects even before another worker claims', async () => {
  const backend = await fileAdapter('exact');
  let time = 0;
  const box = createPersistentOutbox(backend.adapter, { clock: () => time, leaseMs: 1 });
  await box.enqueue('x', 1);
  const [lease] = await box.claim('a');
  time = 1;
  await assert.rejects(box.ack(key(lease)), { code: 'lease' });
  assert.equal((await box.inspect())[0].status, 'leased');
});
test('renewal extends same fenced token without an extra attempt', async () => {
  const backend = await fileAdapter('renew');
  let time = 0;
  const box = createPersistentOutbox(backend.adapter, { clock: () => time, leaseMs: 10 });
  await box.enqueue('x', 1);
  const [lease] = await box.claim('a');
  time = 5;
  assert.deepEqual(await box.renew(lease), { ...lease, expiresAt: 15 });
  time = 10;
  assert.deepEqual(await box.claim('b'), []);
  await box.ack(lease);
});
test('retry delays exponentially increase and cap, then become dead', async () => {
  const backend = await fileAdapter('retry');
  let time = 0;
  const box = createPersistentOutbox(backend.adapter, {
    clock: () => time,
    retryMs: 10,
    maxRetryMs: 15,
    maxAttempts: 3,
  });
  await box.enqueue('x', 1);
  let [lease] = await box.claim('a');
  assert.equal(await box.fail(lease, 'first'), 'pending');
  time = 9;
  assert.deepEqual(await box.claim('a'), []);
  time = 10;
  [lease] = await box.claim('a');
  await box.fail(lease, 'second');
  assert.equal((await box.inspect())[0].availableAt, 25);
  time = 25;
  [lease] = await box.claim('a');
  assert.equal(await box.fail(lease, 'third'), 'dead');
  assert.equal((await box.inspect())[0].error, 'third');
  assert.deepEqual(await box.claim('a'), []);
});
test('exhausted crashed leases become dead, explicit retry resets attempts', async () => {
  const backend = await fileAdapter('crash');
  let time = 0;
  const box = createPersistentOutbox(backend.adapter, {
    clock: () => time,
    leaseMs: 1,
    maxAttempts: 1,
  });
  await box.enqueue('x', 1);
  await box.claim('a');
  time = 1;
  assert.deepEqual(await box.claim('b'), []);
  assert.equal((await box.inspect())[0].status, 'dead');
  assert.equal(await box.retryDead('x'), true);
  assert.equal((await box.claim('b'))[0].attempt, 1);
});
test('done receipts consume capacity until explicitly forgotten', async () => {
  const backend = await fileAdapter('capacity');
  const box = createPersistentOutbox(backend.adapter, { maxEntries: 1, clock: () => 0 });
  await box.enqueue('x', 1);
  await assert.rejects(box.forget('x'), { code: 'conflict' });
  await box.ack((await box.claim('a'))[0]);
  await assert.rejects(box.enqueue('y', 2), { code: 'capacity' });
  assert.equal(await box.forget('x'), true);
  assert.equal(await box.forget('x'), false);
  assert.equal(await box.retryDead('missing'), false);
  assert.equal(await box.enqueue('y', 2), 'enqueued');
});
test('payload and inspection ownership cannot mutate backend state', async () => {
  const backend = await fileAdapter('ownership');
  const box = createPersistentOutbox(backend.adapter, { clock: () => 0 });
  const payload = { values: [1] };
  const pending = box.enqueue('x', payload);
  payload.values.push(2);
  await pending;
  const rows = await box.inspect();
  rows[0].payload.values.push(3);
  const [lease] = await box.claim('a');
  lease.payload.values.push(4);
  assert.deepEqual((await box.inspect())[0].payload, { values: [1] });
});
test('transaction validation error aborts without partial writes', async () => {
  const backend = await fileAdapter('abort');
  const box = createPersistentOutbox(backend.adapter);
  await box.enqueue('x', 1);
  await assert.rejects(box.enqueue('x', 2), OutboxError);
  assert.deepEqual(
    (await box.inspect()).map((row) => row.payload),
    [1],
  );
});
test('backend errors and nonexecuting adapters remain explicit', async () => {
  const error = new Error('disk full');
  const broken = createPersistentOutbox({
    transact: async () => {
      throw error;
    },
  });
  await assert.rejects(broken.enqueue('x', 1), (e) => e === error);
  const liar = createPersistentOutbox({ transact: async () => null });
  await assert.rejects(liar.inspect(), { code: 'adapter' });
});
test('malformed persisted state and unsafe getters reject', async () => {
  const bad = createPersistentOutbox({
    transact: async (update) => update({ version: 2, sequence: 0, entries: [] }).result,
  });
  await assert.rejects(bad.inspect(), { code: 'adapter' });
  let invoked = false;
  const payload = Object.defineProperty({}, 'x', {
    enumerable: true,
    get() {
      invoked = true;
    },
  });
  assert.throws(() => bad.enqueue('x', payload), OutboxError);
  assert.equal(invoked, false);
  assert.throws(() => bad.enqueue('__proto__', 1), OutboxError);
});
test('clock rollback overflow and invalid limits reject without enqueue', async () => {
  const backend = await fileAdapter('clock');
  let time = 10;
  const box = createPersistentOutbox(backend.adapter, { clock: () => time });
  await box.enqueue('x', 1);
  time = 9;
  assert.throws(() => box.claim('a'), { code: 'clock' });
  time = Number.MAX_SAFE_INTEGER;
  assert.throws(() => box.claim('a'), { code: 'clock' });
  for (const options of [
    { maxEntries: 0 },
    { maxAttempts: 0 },
    { leaseMs: 0 },
    { retryMs: 5, maxRetryMs: 4 },
    { clock: 1 },
  ])
    assert.throws(() => createPersistentOutbox(backend.adapter, options), OutboxError);
});
test('transaction-time lease validation rejects an ack delayed past expiry', async () => {
  const backend = await fileAdapter('transaction-time');
  let time = 0;
  let delay = false;
  const adapter = {
    transact: async (update) => {
      if (delay) {
        await Promise.resolve();
        time = 10;
      }
      return backend.adapter.transact(update);
    },
  };
  const box = createPersistentOutbox(adapter, { clock: () => time, leaseMs: 10 });
  await box.enqueue('x', 1);
  const [lease] = await box.claim('a');
  delay = true;
  await assert.rejects(box.ack(lease), { code: 'lease' });
  assert.equal((await box.inspect())[0].status, 'leased');
});
test('malformed fencing token and corrupted adapter result cannot be accepted', async () => {
  const backend = await fileAdapter('bad-token', {
    version: 1,
    sequence: 1,
    entries: [
      {
        id: 'x',
        payload: 1,
        status: 'leased',
        attempts: 1,
        availableAt: 0,
        lease: { token: '01', owner: 'a', expiresAt: 10 },
        error: null,
      },
    ],
  });
  await assert.rejects(createPersistentOutbox(backend.adapter).inspect(), OutboxError);
  const liar = createPersistentOutbox({
    transact: async (update) => {
      update(null);
      return 'invented';
    },
  });
  await assert.rejects(liar.enqueue('x', 1), { code: 'adapter' });
});
test('fencing sequence exhaustion atomically aborts claim', async () => {
  const backend = await fileAdapter('sequence-overflow', {
    version: 1,
    sequence: Number.MAX_SAFE_INTEGER,
    entries: [
      {
        id: 'x',
        payload: 1,
        status: 'pending',
        attempts: 0,
        availableAt: 0,
        lease: null,
        error: null,
      },
    ],
  });
  const box = createPersistentOutbox(backend.adapter, { clock: () => 0 });
  await assert.rejects(box.claim('a'), OutboxError);
  assert.equal((await box.inspect())[0].attempts, 0);
});
test('null numeric options and oversized failure strings reject explicitly', async () => {
  const backend = await fileAdapter('null-options');
  assert.throws(() => createPersistentOutbox(backend.adapter, { maxEntries: null }), OutboxError);
  assert.throws(() => createPersistentOutbox(backend.adapter, { clock: null }), OutboxError);
  const box = createPersistentOutbox(backend.adapter, { clock: () => 0 });
  await box.enqueue('x', 1);
  const [lease] = await box.claim('a');
  assert.throws(() => box.fail(lease, 'x'.repeat(1025)), OutboxError);
  assert.equal((await box.inspect())[0].status, 'leased');
});
