import { mkdir, open, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { replaceFile } from './replace-file.mjs';
const gates = new Map();
// Process-local serialization only. Real files survive reopening; no multi-process or power-loss claim.
export async function fileAdapter(name, seed = null) {
  const directory = new URL('./results/', import.meta.url);
  await mkdir(directory, { recursive: true });
  const path = fileURLToPath(new URL(`${name}.json`, directory));
  await writeFile(path, JSON.stringify(seed));
  const reopen = () => ({
    async transact(update) {
      const previous = gates.get(path) ?? Promise.resolve();
      const operation = previous.then(async () => {
        const commit = update(JSON.parse(await readFile(path, 'utf8')));
        const output = structuredClone(commit.result);
        const bytes = JSON.stringify(commit.state);
        const handle = await open(`${path}.next`, 'w');
        try {
          await handle.writeFile(bytes);
          await handle.sync();
        } finally {
          await handle.close();
        }
        await replaceFile(`${path}.next`, path);
        return output;
      });
      gates.set(
        path,
        operation.catch(() => undefined),
      );
      return operation;
    },
  });
  return { adapter: reopen(), reopen, path };
}
