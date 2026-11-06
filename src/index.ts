import { copy, fields, integer, text, OutboxError, type Json } from './data.js';
export { OutboxError } from './data.js';
export type { Json } from './data.js';

export interface LeaseKey {
  id: string;
  token: string;
  owner: string;
}
export interface OutboxLease extends LeaseKey {
  expiresAt: number;
  attempt: number;
  payload: Json;
}
export interface OutboxEntry {
  id: string;
  payload: Json;
  status: 'pending' | 'leased' | 'done' | 'dead';
  attempts: number;
  availableAt: number;
  lease: { token: string; owner: string; expiresAt: number } | null;
  error: string | null;
}
export interface OutboxState {
  version: 1;
  sequence: number;
  entries: OutboxEntry[];
}
export interface OutboxCommit {
  state: OutboxState;
  result: Json;
}
export interface AtomicOutboxAdapter {
  transact(update: (state: OutboxState | null) => OutboxCommit): PromiseLike<Json>;
}
export interface OutboxOptions {
  maxEntries?: number;
  maxAttempts?: number;
  leaseMs?: number;
  retryMs?: number;
  maxRetryMs?: number;
  clock?: () => number;
}
export interface PersistentOutbox {
  enqueue(id: string, payload: Json): Promise<'enqueued' | 'duplicate'>;
  claim(owner: string, limit?: number): Promise<OutboxLease[]>;
  ack(lease: LeaseKey): Promise<void>;
  fail(lease: LeaseKey, message: string): Promise<'pending' | 'dead'>;
  renew(lease: LeaseKey): Promise<OutboxLease>;
  retryDead(id: string): Promise<boolean>;
  forget(id: string): Promise<boolean>;
  inspect(): Promise<OutboxEntry[]>;
}
export function createPersistentOutbox(
  adapter: AtomicOutboxAdapter,
  options: OutboxOptions = {},
): PersistentOutbox {
  if (!adapter || typeof adapter.transact !== 'function')
    throw new OutboxError('invalid', 'An atomic transaction adapter is required.');
  fields(options as unknown, [
    'maxEntries',
    'maxAttempts',
    'leaseMs',
    'retryMs',
    'maxRetryMs',
    'clock',
  ]);
  for (const name of ['maxEntries', 'maxAttempts', 'leaseMs', 'retryMs', 'maxRetryMs'] as const)
    if (options[name] !== undefined && typeof options[name] !== 'number')
      throw new OutboxError('invalid', 'Numeric options must be numbers.');
  if (options.clock !== undefined && typeof options.clock !== 'function')
    throw new OutboxError('invalid', 'clock must be a function.');
  const maxEntries = integer(options.maxEntries ?? 1000, 1, 10_000);
  const maxAttempts = integer(options.maxAttempts ?? 5, 1, 1000);
  const leaseMs = integer(options.leaseMs ?? 30_000, 1, 86_400_000);
  const retryMs = integer(options.retryMs ?? 1000, 0, 86_400_000);
  const maxRetryMs = integer(options.maxRetryMs ?? 60_000, retryMs, 86_400_000);
  const clock = options.clock ?? Date.now;
  if (typeof clock !== 'function') throw new OutboxError('invalid', 'clock must be a function.');
  let lastTime = 0;
  const now = (): number => {
    const time = clock();
    if (!Number.isSafeInteger(time) || time < 0 || time < lastTime)
      throw new OutboxError('clock', 'Clock must return nondecreasing nonnegative safe integers.');
    lastTime = time;
    return time;
  };
  const add = (time: number, amount: number): number => {
    if (!Number.isSafeInteger(time + amount)) throw new OutboxError('clock', 'Timestamp overflow.');
    return time + amount;
  };
  const validate = (input: OutboxState | null): OutboxState => {
    if (input === null) return { version: 1, sequence: 0, entries: [] };
    const state = copy(input) as unknown as OutboxState;
    fields(state, ['version', 'sequence', 'entries']);
    if (state.version !== 1 || !Array.isArray(state.entries))
      throw new OutboxError('adapter', 'Unsupported outbox state.');
    integer(state.sequence, 0, Number.MAX_SAFE_INTEGER);
    if (state.entries.length > maxEntries)
      throw new OutboxError('capacity', 'Outbox state exceeds maxEntries.');
    const ids = new Set<string>();
    const tokens = new Set<string>();
    for (const entry of state.entries) {
      fields(entry, ['id', 'payload', 'status', 'attempts', 'availableAt', 'lease', 'error']);
      text(entry.id);
      if (ids.has(entry.id)) throw new OutboxError('adapter', 'Duplicate persisted id.');
      ids.add(entry.id);
      if (
        !Object.hasOwn(entry, 'payload') ||
        !['pending', 'leased', 'done', 'dead'].includes(entry.status)
      )
        throw new OutboxError('adapter', 'Invalid entry payload or status.');
      integer(entry.attempts, 0, Number.MAX_SAFE_INTEGER);
      integer(entry.availableAt, 0, Number.MAX_SAFE_INTEGER);
      if (entry.error !== null && (typeof entry.error !== 'string' || entry.error.length > 1024))
        throw new OutboxError('adapter', 'Invalid error text.');
      if (entry.status === 'leased') {
        fields(entry.lease, ['owner', 'token', 'expiresAt']);
        text(entry.lease.owner);
        text(entry.lease.token);
        const token = Number(entry.lease.token);
        integer(token, 1, state.sequence);
        if (String(token) !== entry.lease.token || tokens.has(entry.lease.token))
          throw new OutboxError('adapter', 'Invalid or reused fencing token.');
        tokens.add(entry.lease.token);
        integer(entry.lease.expiresAt, 0, Number.MAX_SAFE_INTEGER);
        if (entry.attempts < 1) throw new OutboxError('adapter', 'Leased entry has no attempt.');
      } else if (entry.lease !== null) throw new OutboxError('adapter', 'Unexpected lease.');
    }
    return state;
  };
  const run = async <T>(update: (state: OutboxState) => T): Promise<T> => {
    let invoked = false;
    let expectedOutput: Json = null;
    const output = await adapter.transact((input) => {
      invoked = true;
      const state = validate(input);
      const result = update(state);
      const checked = validate(state);
      expectedOutput = copy(result);
      return { state: checked, result: copy(expectedOutput) };
    });
    if (!invoked)
      throw new OutboxError('adapter', 'Adapter did not execute the transaction callback.');
    const result = copy(output);
    if (JSON.stringify(result) !== JSON.stringify(expectedOutput))
      throw new OutboxError('adapter', 'Adapter returned a different transaction result.');
    return result as T;
  };
  const key = (input: LeaseKey): LeaseKey => {
    fields(input, ['id', 'token', 'owner', 'expiresAt', 'attempt', 'payload']);
    text(input.id);
    text(input.token);
    text(input.owner);
    return { id: input.id, token: input.token, owner: input.owner };
  };
  const owned = (state: OutboxState, handle: LeaseKey, time: number): OutboxEntry => {
    const entry = state.entries.find((item) => item.id === handle.id);
    if (
      !entry ||
      entry.status !== 'leased' ||
      !entry.lease ||
      entry.lease.token !== handle.token ||
      entry.lease.owner !== handle.owner ||
      entry.lease.expiresAt <= time
    )
      throw new OutboxError('lease', 'Lease is expired, missing or fenced by another claim.');
    return entry;
  };
  const leaseOf = (entry: OutboxEntry): OutboxLease => ({
    id: entry.id,
    token: entry.lease!.token,
    owner: entry.lease!.owner,
    expiresAt: entry.lease!.expiresAt,
    attempt: entry.attempts,
    payload: copy(entry.payload),
  });
  return {
    enqueue(id, payload) {
      text(id);
      const value = copy(payload);
      const time = now();
      return run((state) => {
        const existing = state.entries.find((entry) => entry.id === id);
        if (existing) {
          if (JSON.stringify(existing.payload) !== JSON.stringify(value))
            throw new OutboxError('conflict', 'Id already belongs to a different payload.');
          return 'duplicate';
        }
        if (state.entries.length >= maxEntries)
          throw new OutboxError('capacity', 'Outbox is full.');
        state.entries.push({
          id,
          payload: copy(value),
          status: 'pending',
          attempts: 0,
          availableAt: time,
          lease: null,
          error: null,
        });
        return 'enqueued';
      });
    },
    claim(owner, limit = 1) {
      text(owner);
      integer(limit, 1, Math.min(maxEntries, 1000));
      const startedAt = now();
      add(startedAt, leaseMs);
      return run((state) => {
        const time = now();
        const expiresAt = add(time, leaseMs);
        const leases: OutboxLease[] = [];
        for (const entry of state.entries) {
          const ready =
            (entry.status === 'pending' && entry.availableAt <= time) ||
            (entry.status === 'leased' && entry.lease!.expiresAt <= time);
          if (!ready) continue;
          if (entry.attempts >= maxAttempts) {
            entry.status = 'dead';
            entry.lease = null;
            entry.error = 'Attempt limit reached after lease expiry.';
            continue;
          }
          if (leases.length >= limit) continue;
          state.sequence = integer(state.sequence + 1, 1, Number.MAX_SAFE_INTEGER);
          entry.attempts++;
          entry.status = 'leased';
          entry.lease = { owner, token: String(state.sequence), expiresAt };
          leases.push(leaseOf(entry));
        }
        return leases;
      });
    },
    ack(input) {
      const handle = key(input);
      now();
      return run((state) => {
        const entry = owned(state, handle, now());
        entry.status = 'done';
        entry.lease = null;
        entry.error = null;
        return null;
      }).then(() => undefined);
    },
    fail(input, message) {
      const handle = key(input);
      now();
      if (typeof message !== 'string' || message.length > 1024)
        throw new OutboxError('invalid', 'Failure text must contain at most 1024 characters.');
      return run((state) => {
        const time = now();
        const entry = owned(state, handle, time);
        entry.status = entry.attempts >= maxAttempts ? 'dead' : 'pending';
        entry.availableAt = add(
          time,
          Math.min(maxRetryMs, retryMs * 2 ** Math.min(entry.attempts - 1, 30)),
        );
        entry.lease = null;
        entry.error = message;
        return entry.status as 'pending' | 'dead';
      });
    },
    renew(input) {
      const handle = key(input);
      add(now(), leaseMs);
      return run((state) => {
        const time = now();
        const expiresAt = add(time, leaseMs);
        const entry = owned(state, handle, time);
        entry.lease!.expiresAt = expiresAt;
        return leaseOf(entry);
      });
    },
    retryDead(id) {
      text(id);
      const time = now();
      return run((state) => {
        const entry = state.entries.find((item) => item.id === id);
        if (!entry || entry.status !== 'dead') return false;
        entry.status = 'pending';
        entry.attempts = 0;
        entry.availableAt = time;
        entry.error = null;
        return true;
      });
    },
    forget(id) {
      text(id);
      return run((state) => {
        const index = state.entries.findIndex((item) => item.id === id);
        if (index < 0) return false;
        if (!['done', 'dead'].includes(state.entries[index]!.status))
          throw new OutboxError('conflict', 'Only done or dead entries can be forgotten.');
        state.entries.splice(index, 1);
        return true;
      });
    },
    inspect: () => run((state) => state.entries),
  };
}
