import {
  createPersistentOutbox,
  OutboxError,
  type AtomicOutboxAdapter,
  type OutboxLease,
  type OutboxState,
  type Json,
} from '@lp-labs/persistent-outbox';
const adapter: AtomicOutboxAdapter = { transact: async (update) => update(null).result };
const box = createPersistentOutbox(adapter, { maxAttempts: 3 });
const value: Json = { n: 1 };
void box.enqueue('id', value);
async function work() {
  const leases: OutboxLease[] = await box.claim('worker');
  for (const lease of leases) {
    await box.renew(lease);
    await box.fail(lease, 'failure');
    await box.ack(lease);
  }
}
void work;
const state: OutboxState = { version: 1, sequence: 0, entries: [] };
void state;
void new OutboxError('lease', 'Expired.');
// @ts-expect-error lease needs a fencing token
box.ack({ id: 'id', owner: 'worker' });
// @ts-expect-error runtime data is JSON only
box.enqueue('id', new Date());
