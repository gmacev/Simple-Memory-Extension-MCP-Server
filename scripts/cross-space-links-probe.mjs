#!/usr/bin/env node
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-cross-space-links-'));

function assert(condition, message) {
  if (!condition) throw new Error(`Cross-space links probe assertion failed: ${message}`);
}

function environment(grants) {
  return {
    ...process.env,
    SIMPLE_MEMORY_DATA_DIR: dataDir,
    SIMPLE_MEMORY_LOG_LEVEL: 'error',
    SIMPLE_MEMORY_MODELS: 'disabled',
    SIMPLE_MEMORY_ACCESS_MODE: 'fixed',
    SIMPLE_MEMORY_FIXED_PRINCIPAL: 'cross-space-probe',
    SIMPLE_MEMORY_FIXED_ACCESS: JSON.stringify({ spaces: grants }),
  };
}

function parsedResult(response) {
  const text = response.content.find((item) => item.type === 'text');
  assert(text, 'tool response should contain JSON text');
  return JSON.parse(text.text);
}

async function withClient(grants, callback) {
  const client = new Client(
    { name: 'simple-memory-cross-space-links-probe', version: '3.10.0' },
    { versionNegotiation: { mode: 'auto' } },
  );
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(root, 'dist', 'index.js')],
    cwd: root,
    env: environment(grants),
    stderr: 'pipe',
  });
  try {
    await client.connect(transport);
    await callback({
      async call(name, arguments_) {
        const response = await client.callTool({ name, arguments: arguments_ });
        if (response.isError) {
          const text = response.content.find((item) => item.type === 'text');
          throw new Error(text?.text ?? `${name} failed`);
        }
        return parsedResult(response);
      },
      async error(name, arguments_) {
        const response = await client.callTool({ name, arguments: arguments_ });
        assert(response.isError, `${name} should fail`);
        return response.content.find((item) => item.type === 'text')?.text ?? '';
      },
    });
  } finally {
    await client.close();
  }
}

async function run() {
  let a;
  let b;
  let c;
  let linkToB;
  await withClient({ '*': 'manage' }, async ({ call }) => {
    for (const id of ['cross-a', 'cross-b', 'cross-c']) {
      await call('space_create', { id, name: id });
    }
    a = await call('memory_create', {
      spaceId: 'cross-a',
      title: 'Cross-space root',
      content: 'cross-space-root-marker',
    });
    b = await call('memory_create', {
      spaceId: 'cross-b',
      title: 'Private destination',
      content: 'private-destination-marker',
    });
    c = await call('memory_create', {
      spaceId: 'cross-c',
      title: 'Readable destination',
      content: 'readable-destination-marker',
    });
    linkToB = await call('memory_link', {
      fromMemoryId: a.id,
      toMemoryId: b.id,
      relation: 'references',
    });
    await call('memory_link', {
      fromMemoryId: a.id,
      toMemoryId: c.id,
      relation: 'references',
    });
  });

  await withClient({ 'cross-a': 'write', 'cross-c': 'read' }, async ({ call, error }) => {
    const traversal = await call('memory_traverse', { memoryId: a.id, maxDepth: 1, limit: 2 });
    const ids = traversal.items.map((item) => item.memory.id);
    assert(ids.includes(c.id), 'authorized cross-space destination should be traversable');
    assert(!ids.includes(b.id), 'unauthorized destination should be omitted before pagination');

    const expanded = await call('memory_search', {
      query: 'cross-space-root-marker',
      spaceIds: ['cross-a'],
      mode: 'lexical',
      topK: 5,
      expandRelations: true,
    });
    const expandedIds = expanded.results.map((item) => item.id);
    assert(expandedIds.includes(c.id), 'authorized relation expansion should cross spaces');
    assert(!expandedIds.includes(b.id), 'relation expansion should not leak unauthorized spaces');

    const hiddenError = await error('memory_unlink', { linkId: linkToB.id });
    assert(
      hiddenError.includes('not-found-or-inaccessible'),
      'a link with an unreadable endpoint should hide its existence',
    );
  });

  await withClient({ 'cross-a': 'write', 'cross-b': 'read' }, async ({ error }) => {
    const createError = await error('memory_link', {
      fromMemoryId: a.id,
      toMemoryId: b.id,
      relation: 'cannot-write-target',
    });
    assert(
      createError.includes('access-denied'),
      'link creation should require both spaces writable',
    );
    const unlinkError = await error('memory_unlink', { linkId: linkToB.id });
    assert(unlinkError.includes('access-denied'), 'unlink should require both spaces writable');
  });

  await withClient({ '*': 'manage' }, async ({ call }) => {
    await call('space_delete', { spaceId: 'cross-b' });
    const hidden = await call('memory_traverse', { memoryId: a.id, maxDepth: 1, limit: 10 });
    assert(
      !hidden.items.some((item) => item.memory.id === b.id),
      'soft-deleted destination spaces should hide crossing links',
    );
    await call('space_restore', { spaceId: 'cross-b' });
    const restored = await call('memory_traverse', { memoryId: a.id, maxDepth: 1, limit: 10 });
    assert(
      restored.items.some((item) => item.memory.id === b.id),
      'restored destination spaces should restore crossing links',
    );
    await call('memory_unlink', { linkId: linkToB.id });
  });

  process.stdout.write(
    `${JSON.stringify({ status: 'ok', crossSpaceTraversal: true, authorization: true, lifecycle: true })}\n`,
  );
}

try {
  await run();
} finally {
  rmSync(dataDir, { recursive: true, force: true });
}
