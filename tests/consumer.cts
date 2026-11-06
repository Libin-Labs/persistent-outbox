import {
  createPersistentOutbox,
  type AtomicOutboxAdapter,
  type LeaseKey,
} from '@lp-labs/persistent-outbox';
const adapter: AtomicOutboxAdapter = { transact: async (update) => update(null).result };
const key: LeaseKey = { id: 'id', token: '1', owner: 'worker' };
const box = createPersistentOutbox(adapter);
void box.ack(key);
void box.retryDead('id');
void box.forget('id');
void box.inspect();
// @ts-expect-error failure text must be a string
box.fail(key, 1);
