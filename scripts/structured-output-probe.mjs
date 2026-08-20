#!/usr/bin/env node
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';
import * as z from 'zod/v4';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataRoot = mkdtempSync(path.join(tmpdir(), 'simple-memory-structured-output-'));
const calledTools = new Set();

function environment() {
  return {
    ...process.env,
    SIMPLE_MEMORY_DATA_DIR: dataRoot,
    SIMPLE_MEMORY_LOG_LEVEL: 'error',
    SIMPLE_MEMORY_MODELS: 'disabled',
  };
}

const client = new Client(
  { name: 'simple-memory-structured-output-probe', version: '3.5.0' },
  { versionNegotiation: { mode: 'auto', probe: { timeoutMs: 5_000 } } },
);
const transport = new StdioClientTransport({
  command: process.execPath,
  args: [path.join(root, 'dist', 'index.js')],
  cwd: root,
  env: environment(),
  stderr: 'pipe',
});

let advertisedTools = new Map();

async function call(name, arguments_) {
  const response = await client.callTool({ name, arguments: arguments_ });
  assert.equal(response.isError, undefined, `${name} must succeed`);
  assert.notEqual(response.structuredContent, undefined, `${name} must return structuredContent`);

  const text = response.content.find((item) => item.type === 'text');
  assert(text, `${name} must retain a JSON text block`);
  const parsedText = JSON.parse(text.text);
  assert.equal(text.text, JSON.stringify(parsedText), `${name} JSON text must remain minified`);
  assert.deepEqual(
    response.structuredContent,
    parsedText,
    `${name} structuredContent must equal its JSON text representation`,
  );

  const tool = advertisedTools.get(name);
  assert(tool?.outputSchema, `${name} must advertise outputSchema`);
  const validator = z.fromJSONSchema(tool.outputSchema);
  const validation = validator.safeParse(response.structuredContent);
  assert(
    validation.success,
    `${name} structuredContent must validate: ${validation.error?.message ?? 'unknown error'}`,
  );
  calledTools.add(name);
  return parsedText;
}

try {
  await client.connect(transport);
  const listed = await client.listTools(undefined, { cacheMode: 'refresh' });
  advertisedTools = new Map(listed.tools.map((tool) => [tool.name, tool]));
  assert.equal(advertisedTools.size, 21, 'all Simple Memory tools must be advertised');
  for (const tool of advertisedTools.values()) {
    assert(tool.outputSchema, `${tool.name} must advertise outputSchema`);
    z.fromJSONSchema(tool.outputSchema);
  }

  await call('memory_status', { probeModels: false });
  const detailedStatus = await call('memory_status', {
    probeModels: false,
    includeDetails: true,
  });
  assert.equal(typeof detailedStatus.database, 'string', 'detailed status must retain database');
  assert.equal(
    detailedStatus.inferenceScheduler?.queueLimit,
    128,
    'detailed status must expose bounded inference scheduler diagnostics',
  );

  const space = await call('space_create', {
    name: 'Structured output probe',
    description: 'Exercises every MCP tool output contract.',
    metadata: { fixture: true },
  });
  const spaces = await call('space_list', {
    id: space.id,
    includeMetadata: true,
  });
  assert.deepEqual(spaces.items[0]?.metadata, { fixture: true }, 'space metadata opt-in');

  const canonical = await call('memory_create', {
    spaceId: space.id,
    logicalKey: 'structured/canonical',
    title: 'Structured canonical memory',
    kind: 'protocol-probe',
    content: { marker: 'STRUCTURED-OUTPUT-PROBE', version: 1 },
    tags: ['structured-output'],
    metadata: { owner: 'probe' },
    sources: [
      {
        uri: 'urn:simple-memory:structured-output',
        type: 'fixture',
        metadata: { phase: 1 },
      },
    ],
    confidence: 0.95,
    salience: 0.8,
  });
  const duplicate = await call('memory_create', {
    spaceId: space.id,
    logicalKey: 'structured/duplicate',
    title: 'Structured duplicate memory',
    kind: 'protocol-probe',
    content: { marker: 'STRUCTURED-OUTPUT-DUPLICATE' },
  });

  const revised = await call('memory_revise', {
    memoryId: canonical.id,
    expectedRevisionId: canonical.revisionId,
    spaceId: space.id,
    title: 'Structured canonical memory',
    kind: 'protocol-probe',
    content: { marker: 'STRUCTURED-OUTPUT-PROBE', version: 2 },
    tags: ['structured-output'],
    metadata: { owner: 'probe' },
    sources: [
      {
        uri: 'urn:simple-memory:structured-output',
        type: 'fixture',
        metadata: { phase: 2 },
      },
    ],
    confidence: 0.96,
    salience: 0.85,
  });

  const memory = await call('memory_get', { memoryId: canonical.id });
  assert.deepEqual(memory.revision.content, {
    marker: 'STRUCTURED-OUTPUT-PROBE',
    version: 2,
  });
  const historySummary = await call('memory_history', {
    memoryId: canonical.id,
    limit: 1,
  });
  assert.equal(historySummary.revisions[0]?.content, undefined, 'history content defaults off');
  const historyDetail = await call('memory_history', {
    memoryId: canonical.id,
    includeContent: true,
  });
  assert.equal(historyDetail.revisions.length, 2, 'history content opt-in');

  await call('memory_list', { spaceId: space.id, limit: 1 });
  const compactSearch = await call('memory_search', {
    query: 'STRUCTURED-OUTPUT-PROBE',
    spaceIds: [space.id],
    mode: 'lexical',
    topK: 2,
  });
  assert.equal(compactSearch.mode, undefined, 'ordinary search must omit diagnostics');
  assert.equal(
    compactSearch.results[0]?.score,
    undefined,
    'ordinary search score must remain hidden',
  );
  assert.equal(
    compactSearch.results[0]?.sources?.[0]?.metadata,
    undefined,
    'ordinary search source metadata must remain hidden',
  );
  const explainedSearch = await call('memory_search', {
    query: 'STRUCTURED-OUTPUT-PROBE',
    spaceIds: [space.id],
    mode: 'lexical',
    topK: 2,
    explain: true,
    includeSourceMetadata: true,
  });
  assert.equal(explainedSearch.mode, 'lexical', 'explained search mode');
  assert.equal(typeof explainedSearch.timingMs, 'number', 'explained search timing');
  assert.equal(typeof explainedSearch.results[0]?.score?.fusedScore, 'number', 'explained score');
  assert.deepEqual(
    explainedSearch.results[0]?.sources?.[0]?.metadata,
    { phase: 2 },
    'search source metadata opt-in',
  );

  await call('memory_feedback', {
    memoryId: canonical.id,
    revisionId: revised.revisionId,
    scope: 'content',
    signal: 'verified',
    actorType: 'agent',
    note: 'Structured output fixture verification.',
    metadata: { fixture: true },
  });
  const feedback = await call('memory_feedback_list', {
    memoryId: canonical.id,
    includeDetails: true,
  });
  assert.equal(feedback.items[0]?.note, 'Structured output fixture verification.');

  const link = await call('memory_link', {
    fromMemoryId: canonical.id,
    toMemoryId: duplicate.id,
    relation: 'duplicates',
    metadata: { fixture: true },
  });
  const traversal = await call('memory_traverse', {
    memoryId: canonical.id,
    maxDepth: 1,
    explain: true,
  });
  assert.equal(traversal.items.length, 2, 'traversal must include root and linked memory');
  await call('memory_unlink', { linkId: link.id });

  await call('memory_archive', { memoryId: duplicate.id });
  await call('memory_restore', { memoryId: duplicate.id });
  await call('memory_merge', {
    canonicalMemoryId: canonical.id,
    expectedCanonicalRevisionId: revised.revisionId,
    duplicates: [
      {
        memoryId: duplicate.id,
        expectedRevisionId: duplicate.revisionId,
      },
    ],
    reason: 'Structured output fixture duplicate.',
  });
  const redirected = await call('memory_get_by_key', {
    spaceId: space.id,
    logicalKey: 'structured/duplicate',
  });
  assert.equal(redirected.redirectedFromMemoryId, duplicate.id, 'logical-key redirect output');

  await call('space_delete', { spaceId: space.id });
  await call('space_restore', { spaceId: space.id });
  await call('memory_delete', { memoryId: duplicate.id });
  await call('memory_delete', { memoryId: canonical.id });

  assert.deepEqual(
    [...calledTools].sort(),
    [...advertisedTools.keys()].sort(),
    'every advertised tool must produce a validated structured response',
  );
  console.log(
    JSON.stringify({
      status: 'ok',
      toolsAdvertised: advertisedTools.size,
      toolsCalled: calledTools.size,
      structuredParity: true,
      outputSchemaValidation: true,
    }),
  );
} finally {
  await client.close().catch(() => undefined);
  rmSync(dataRoot, { recursive: true, force: true });
}
