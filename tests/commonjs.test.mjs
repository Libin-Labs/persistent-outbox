import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import * as esm from '../dist/esm/index.js';
const cjs = createRequire(import.meta.url)('../dist/cjs/index.js');
test('CommonJS exports match ESM and execute fenced claim and ack', async () => {
  assert.deepEqual(Object.keys(cjs).sort(), Object.keys(esm).sort());
  let state = null;
  const box = cjs.createPersistentOutbox(
    {
      transact: async (update) => {
        const commit = update(state);
        state = structuredClone(commit.state);
        return commit.result;
      },
    },
    { clock: () => 0 },
  );
  await box.enqueue('id', 1);
  await box.ack((await box.claim('worker'))[0]);
  assert.equal((await box.inspect())[0].status, 'done');
});
