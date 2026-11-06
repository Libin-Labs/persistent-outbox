# @lp-labs/persistent-outbox

Original bounded adapter-backed outbox: atomic enqueue/dedupe, fenced expiring leases,
exponential retry, retained done receipts and dead letters. Version 0.1.0. Zero runtime/peer
dependencies; strict TypeScript NodeNext ESM and CommonJS declarations; Node 18+.

**The package does not implement a storage backend. Persistence and atomicity come from an
injected real backend, not from a Map or this package's name.**

## Install and runnable file example

```sh
npm install @lp-labs/persistent-outbox
```

The example uses actual file storage, flushes file data and atomically replaces one file.
It serializes operations through **one adapter instance in one process only**; it is not a
multi-process database and does not promise power-loss durability of the directory rename.
It resets only its explicitly named demo file on each run. Production adapters must satisfy
the transaction contract below. Tests also use real file backends and retain their final snapshots.

```js
import assert from 'node:assert/strict';
import { open, readFile, rename, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { createPersistentOutbox, OutboxError } from '@lp-labs/persistent-outbox';

async function replaceFile(source, destination) {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      await rename(source, destination);
      return;
    } catch (error) {
      if (
        process.platform !== 'win32' ||
        !['EPERM', 'EACCES', 'EBUSY'].includes(error.code) ||
        attempt === 7
      )
        throw error;
      await delay(Math.min(25 * 2 ** attempt, 200));
    }
  }
}

const path = new URL('./outbox-example.json', import.meta.url);
const staging = new URL('./outbox-example.next', import.meta.url);
await writeFile(path, 'null'); // Destructive demo initialization, not a production startup step.
let gate = Promise.resolve();
const adapter = {
  transact(update) {
    const operation = gate.then(async () => {
      const commit = update(JSON.parse(await readFile(path, 'utf8')));
      const result = structuredClone(commit.result);
      const file = await open(staging, 'w');
      try {
        await file.writeFile(JSON.stringify(commit.state));
        await file.sync();
      } finally {
        await file.close();
      }
      await replaceFile(staging, path);
      return result;
    });
    gate = operation.catch(() => undefined);
    return operation;
  },
};
const box = createPersistentOutbox(adapter, { clock: () => 0, maxAttempts: 1 });
assert.equal(await box.enqueue('order-1', { amount: 12 }), 'enqueued');
assert.equal(await box.enqueue('order-1', { amount: 12 }), 'duplicate');
const [first] = await box.claim('worker-a');
assert.equal(await box.fail(first, 'Destination unavailable'), 'dead');
assert.equal(await box.retryDead('order-1'), true);
const [second] = await box.claim('worker-b');
await box.renew(second);
// Perform the remote side effect with idempotency key second.id before acknowledgment.
await box.ack(second);
assert.equal((await box.inspect())[0].status, 'done');
console.log((await box.inspect())[0].attempts); // 1 (explicit dead-letter retry reset attempts)
assert.equal(await box.forget('order-1'), true);

const error = new OutboxError('lease', 'A stale worker is fenced.');
assert.equal(error.code, 'lease');
```

CommonJS: `const { createPersistentOutbox, OutboxError } = require('@lp-labs/persistent-outbox')`.

On Windows, replacement retries only `EPERM`, `EACCES`, and `EBUSY`, at most eight rename
attempts with seven waits totaling at most 975 ms. These codes can also indicate permanent
permissions problems: the final original error is rethrown; other errors/platforms fail immediately.
Each attempt uses same-directory rename without deleting the destination, keeping the previous
snapshot until replacement succeeds. Persistent locks still fail; syscall duration is not bounded
by the wait budget. This does not add cross-process locking, directory fsync, or a power-loss guarantee.

## Atomic backend contract

`AtomicOutboxAdapter.transact(update) -> PromiseLike<Json>` must:

1. Read the latest `OutboxState | null` while holding a serializable exclusive transaction/lock
   shared by **all** writers to that outbox. Null means uninitialized.
2. Invoke the synchronous callback with an isolated snapshot. It returns
   `OutboxCommit = { state: OutboxState, result: Json }`.
3. If the callback throws, commit nothing and reject. Otherwise atomically replace the state
   and resolve with exactly its result **only after successful commit**.
4. Never publish callback-owned references or commit after reporting a definite failure.
   Transaction retries may re-invoke the callback against the latest snapshot.

The library validates snapshots, output state, JSON ownership, callback invocation and returned
result equality. It cannot prove that a backend really committed, locked, flushed, or survived a
crash. Backend exceptions propagate unchanged. An ambiguous commit failure can mean the backend
committed; inspect/reconcile before blindly repeating operations.

Use a durable transactional database or equivalent atomic persistent backend. Independent
wrappers need a shared lock/CAS transaction, not separate process-local queues. Never transact
by asynchronously loading and later overwriting without concurrency control. All instances
sharing state must use compatible limits, clocks and retry configuration.

`OutboxState` is `{ version: 1, sequence: number, entries: OutboxEntry[] }`. Sequence is a
persisted global safe-integer fencing counter, never reset on forgetting receipts.
`OutboxEntry` contains `id`, `payload: Json`, `status` (`pending`, `leased`, `done`, `dead`),
`attempts`, `availableAt`, `lease` (null or `{ owner, token, expiresAt }`), and `error` (null
or bounded string). Lease tokens are unique positive decimal sequence values.

## API, types and customization

`createPersistentOutbox(adapter, options?) -> PersistentOutbox` is synchronous.
No runtime memory backend, timers, worker loop, transport, scheduler or side effect is hidden.

| Option        | Default    | Bounds                                                           |
| ------------- | ---------- | ---------------------------------------------------------------- |
| `maxEntries`  | 1000       | Integer 1..10,000; includes pending, leased, done, dead          |
| `maxAttempts` | 5          | 1..1000; attempt is counted when claimed, even if worker crashes |
| `leaseMs`     | 30,000     | 1..86,400,000                                                    |
| `retryMs`     | 1000       | 0..86,400,000                                                    |
| `maxRetryMs`  | 60,000     | `retryMs`..86,400,000                                            |
| `clock`       | `Date.now` | Nondecreasing, nonnegative safe-integer milliseconds             |

Lease decisions use time sampled **inside the atomic callback**, not just at request submission.
The clock must be sufficiently synchronized across workers; process-local monotonic validation
does not solve distributed clock skew. Timestamp/counter overflow rejects rather than wraps.

`PersistentOutbox` methods:

- `enqueue(id, payload) -> Promise<'enqueued' | 'duplicate'>`: atomically append a pending
  entry. Retained same-id structurally equal payload is duplicate, regardless of status.
  Same id with different payload throws `conflict`. Object key order is canonicalized.
- `claim(owner, limit = 1) -> Promise<OutboxLease[]>`: scan in enqueue order for due pending
  or expired leased records. Limit is 1..min(1000, maxEntries). Increment attempts and global
  token per claim. Active leases are excluded. Ready exhausted entries become dead even if
  the requested claim limit was already met.
- `ack(lease: LeaseKey) -> Promise<void>`: with an unexpired matching owner/token, mark done
  and retain payload for dedupe. It is **not** an idempotent acknowledgment: repeating ack
  after completion throws `lease`.
- `fail(lease, message) -> Promise<'pending' | 'dead'>`: fence identically; store at most
  1024 characters of caller-supplied error text. If attempts exhausted, dead; otherwise
  pending at `now + min(maxRetryMs, retryMs * 2^min(attempts - 1, 30))`. No jitter.
- `renew(lease) -> Promise<OutboxLease>`: extend active lease to transaction-time plus
  `leaseMs`, keeping token/attempt. A previously returned matching key still works.
- `retryDead(id) -> Promise<boolean>`: explicitly reset a dead entry to pending with zero
  attempts and no error; false if absent/not dead.
- `forget(id) -> Promise<boolean>`: only done/dead records; removes dedupe history and frees
  capacity. False if absent; pending/leased throws `conflict`. Never automatically evicts.
- `inspect() -> Promise<OutboxEntry[]>`: validated owned snapshot. Implemented through the
  same transaction interface; a backend may commit an unchanged snapshot.

`LeaseKey = { id, owner, token }`; a complete `OutboxLease` is accepted wherever a key is
needed and also contains `payload`, `expiresAt`, `attempt`. Altering payload in a returned
lease cannot alter the stored entry. Only the key fences acknowledgments. A lease is valid
strictly before expiry (`expiresAt <= now` rejects).

## Idempotency, delivery and durability limitations

Delivery is **at least once while eligible for retry**, not exactly once. A worker may complete
the destination side effect and crash before ack. Expiry/retry then repeats the side effect.
Use the outbox id as a stable remote idempotency key, with payload consistency and sufficient
remote receipt retention. Fencing protects **outbox state**, not the remote side effect; an
expired worker can still send unless the destination separately fences it.

Done/dead entries consume capacity. Forgetting destroys dedupe guarantees; never reuse operation
ids while old deliveries or remote receipts might exist. Dead-letter retry intentionally permits
additional deliveries. No tombstone/receipt TTL, automatic purging, exactly-once claim, or
storage-backed durability is asserted for an in-memory adapter. Backend durability governs
survival; successful enqueue means committed only if the adapter contract is honored.

## Errors, guards and performance

`OutboxError` extends `Error`, `name = 'OutboxError'`; codes are `invalid`, `capacity`,
`conflict`, `adapter`, `lease`, `clock`. Argument validation may throw synchronously even on
methods returning promises. Transaction/backend failures reject; backend errors preserve identity.
No failure is silently treated as success.

Safe identifiers contain 1..256 characters and exclude prototype-dangerous names. Every JSON
clone is bounded to 10,000 nodes, depth 32, 1,000,000 key/string characters, at most 10,000
own object keys and 256 characters per key. **The persisted state and claim result each also
have aggregate JSON bounds**, so payload size may fill the outbox before maxEntries.
Plain/null-prototype objects, finite numbers and dense arrays only. Accessors, symbols, holes,
classes, cycles, undefined and keys `__proto__`, `constructor`, `prototype` reject; no getter
is invoked. Hostile proxies are outside the contract.

All transaction paths clone/validate the whole bounded state; scans are O(entries), plus JSON
traversal and canonical key sorting. Each claim is bounded; no automatic endless drain.
This is for modest outboxes, not millions of indexed jobs. For higher throughput, use a backend
with a domain-specific indexed queue rather than this snapshot transaction contract.

## Development

```sh
npm ci --offline
npm run build
npm run typecheck
npm test
npm run format:check
```

`build` emits ESM/CJS declarations; `typecheck` verifies bare `.mts`/`.cts` consumers; `test`
runs standalone built-output tests including real-file restart and multi-instance races.
`format` writes Prettier; `prepack` builds. TypeScript ^5.9.3 and Prettier ^3.6.2 are dev-only.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
