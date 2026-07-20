#!/usr/bin/env node
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const modelsEnabled = !process.argv.includes('--models-disabled');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-live-'));
let activeClient;

function assert(condition, message) {
  if (!condition) throw new Error(`Live probe assertion failed: ${message}`);
}

function deletionRowCounts(memoryId, revisionIds, segmentIds) {
  const database = new Database(path.join(dataDir, 'memory.db'));
  try {
    sqliteVec.load(database);
    const revisionPlaceholders = revisionIds.map(() => '?').join(', ');
    const segmentPlaceholders = segmentIds.map(() => '?').join(', ');
    const count = (sql, ...parameters) => Number(database.prepare(sql).get(...parameters).count);
    return {
      memories: count('SELECT COUNT(*) AS count FROM memories WHERE id = ?', memoryId),
      revisions: count(
        'SELECT COUNT(*) AS count FROM memory_revisions WHERE memory_id = ?',
        memoryId,
      ),
      stateEvents: count(
        'SELECT COUNT(*) AS count FROM memory_state_events WHERE memory_id = ?',
        memoryId,
      ),
      tags: count(
        `SELECT COUNT(*) AS count FROM revision_tags WHERE revision_id IN (${revisionPlaceholders})`,
        ...revisionIds,
      ),
      sources: count(
        `SELECT COUNT(*) AS count FROM revision_sources WHERE revision_id IN (${revisionPlaceholders})`,
        ...revisionIds,
      ),
      segments: count(
        'SELECT COUNT(*) AS count FROM memory_segments WHERE memory_id = ?',
        memoryId,
      ),
      lexicalEntries: count(
        'SELECT COUNT(*) AS count FROM memory_fts WHERE memory_id = ?',
        memoryId,
      ),
      vectors: count(
        `SELECT COUNT(*) AS count FROM memory_vectors WHERE segment_id IN (${segmentPlaceholders})`,
        ...segmentIds,
      ),
      links: count(
        'SELECT COUNT(*) AS count FROM memory_links WHERE from_memory_id = ? OR to_memory_id = ?',
        memoryId,
        memoryId,
      ),
      feedback: count(
        'SELECT COUNT(*) AS count FROM memory_feedback WHERE memory_id = ?',
        memoryId,
      ),
      indexJobs: count(
        `SELECT COUNT(*) AS count FROM index_jobs WHERE revision_id IN (${revisionPlaceholders})`,
        ...revisionIds,
      ),
    };
  } finally {
    database.close();
  }
}

function seedDeletionProbeVector(segmentId) {
  const database = new Database(path.join(dataDir, 'memory.db'));
  try {
    sqliteVec.load(database);
    const existing = Number(
      database
        .prepare('SELECT COUNT(*) AS count FROM memory_vectors WHERE segment_id = ?')
        .get(segmentId).count,
    );
    if (existing > 0) return;
    const dimensions = Number.parseInt(
      process.env.SIMPLE_MEMORY_EMBEDDING_DIMENSION ?? '1024',
      10,
    );
    database
      .prepare(
        `INSERT INTO memory_vectors(segment_id, embedding, model_profile_id)
         VALUES (?, ?, ?)`,
      )
      .run(
        segmentId,
        Buffer.from(new Float32Array(dimensions).buffer),
        'deletion-integrity-probe',
      );
  } finally {
    database.close();
  }
}

function toolResult(response) {
  const text = response.content.find((item) => item.type === 'text');
  if (!text) throw new Error('Tool did not return JSON text content');
  return JSON.parse(text.text);
}

async function connect() {
  const client = new Client({ name: 'simple-memory-live-probe', version: '2.0.0' });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(root, 'dist', 'index.js')],
    cwd: root,
    env: {
      ...process.env,
      SIMPLE_MEMORY_DATA_DIR: dataDir,
      SIMPLE_MEMORY_MODELS: modelsEnabled ? 'enabled' : 'disabled',
      SIMPLE_MEMORY_LOG_LEVEL: process.env.SIMPLE_MEMORY_LOG_LEVEL ?? 'error',
    },
  });
  await client.connect(transport);
  activeClient = client;
  return client;
}

async function rawCall(client, name, args = {}) {
  return client.callTool({ name, arguments: args }, undefined, {
    timeout: 900_000,
    maxTotalTimeout: 900_000,
  });
}

async function call(client, name, args = {}) {
  const response = await rawCall(client, name, args);
  if (response.isError) {
    throw new Error(`${name} failed: ${JSON.stringify(response.content)}`);
  }
  const parsed = toolResult(response);
  const text = response.content.find((item) => item.type === 'text');
  assert(text?.text === JSON.stringify(parsed), `${name} should return minified canonical JSON`);
  assert(response.structuredContent === undefined, `${name} should not duplicate its JSON result`);
  return parsed;
}

async function expectToolError(client, name, args) {
  const response = await rawCall(client, name, args);
  assert(response.isError, `${name} should reject invalid state`);
}

async function run() {
  let client = await connect();
  const tools = await client.listTools();
  const names = new Set(tools.tools.map((tool) => tool.name));
  for (const required of [
    'memory_create',
    'memory_revise',
    'memory_get',
    'memory_history',
    'memory_search',
    'memory_link',
    'memory_traverse',
    'memory_feedback',
    'memory_status',
    'memory_archive',
    'memory_restore',
    'memory_delete',
  ]) {
    assert(names.has(required), `missing MCP tool ${required}`);
  }
  const archiveTool = tools.tools.find((tool) => tool.name === 'memory_archive');
  const restoreTool = tools.tools.find((tool) => tool.name === 'memory_restore');
  const deleteTool = tools.tools.find((tool) => tool.name === 'memory_delete');
  assert(
    archiveTool?.description?.includes('Reversibly') &&
      archiveTool.description.includes('memory_delete'),
    'archive description should distinguish recoverable retention from permanent deletion',
  );
  assert(
    restoreTool?.description?.includes('active status') &&
      restoreTool.description.includes('without changing its content'),
    'restore description should explain reactivation without revision changes',
  );
  assert(
    deleteTool?.description?.includes('Permanently and irreversibly') &&
      deleteTool.description.includes('all relationships'),
    'delete description should disclose complete irreversible erasure',
  );
  assert(deleteTool?.annotations?.destructiveHint === true, 'delete must be marked destructive');

  const space = await call(client, 'space_create', {
    id: 'live-probe',
    name: 'Live probe',
    description: 'Isolated end-to-end verification',
  });
  assert(space.id === 'live-probe', 'space creation');

  const lease = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Warehouse lease renewal',
    kind: 'agreement',
    content: {
      organization: 'Acme Logistics',
      statement: 'The company renewed its Kaunas warehouse lease through December 2030.',
      annualCostEur: 84000,
    },
    tags: ['operations', 'property'],
    sources: [
      {
        uri: 'urn:probe:contract-17',
        type: 'contract',
        metadata: { contractVersion: 17 },
      },
    ],
    confidence: 0.95,
    reviewAfter: '2000-01-01T00:00:00.000Z',
    idempotencyKey: 'probe-lease',
  });
  assert(lease.indexStatus === (modelsEnabled ? 'ready' : 'lexical-only'), 'expected index status');
  const originalRecordedAt = lease.revision.recordedAt;
  assert(lease.revision.content === undefined, 'create response should not echo submitted content');
  assert(lease.revision.contentHash === undefined, 'create response should hide internal hash');

  const preference = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Kelionių pageidavimas',
    kind: 'preference',
    content: 'Vartotojas kelionėms renkasi ramius viešbučius netoli gamtos, ne miesto centre.',
    tags: ['lietuvių', 'kelionės'],
  });

  const structuredRate = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Northline refrigerated delivery rate',
    kind: 'commercial-term',
    content: {
      carrier: 'Northline Logistics',
      price: {
        amount: 18.25,
        currency: 'EUR',
        basis: 'per temperature-controlled pallet',
      },
    },
    tags: ['procurement', 'cold-chain'],
  });

  const duplicate = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'This title must not replace the original',
    content: 'Duplicate retry',
    idempotencyKey: 'probe-lease',
  });
  assert(duplicate.id === lease.id, 'idempotency key must return the original memory');

  const lexical = await call(client, 'memory_search', {
    query: 'Kaunas warehouse lease',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(lexical.results[0]?.id === lease.id, 'lexical search should retrieve the lease');
  assert(typeof lexical.results[0]?.relevanceScore === 'number', 'compact relevance score');
  assert(lexical.results[0]?.score === undefined, 'rank diagnostics should be opt-in');
  assert(
    lexical.results[0]?.sources?.[0]?.metadata === undefined,
    'source metadata should be opt-in',
  );
  assert(
    lexical.results[0]?.revisionId === lease.currentRevisionId,
    'search result should identify the returned revision',
  );
  assert(
    lexical.results[0]?.isCurrentRevision === true,
    'current search result should identify itself as current',
  );
  assert(
    lexical.results[0]?.reviewAfter === '2000-01-01T00:00:00.000Z',
    'search should expose the optional review date',
  );
  assert(lexical.results[0]?.reviewDue === true, 'overdue review should be explicit');

  const structuredRateSearch = await call(client, 'memory_search', {
    query: 'temperature-controlled pallet',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(
    structuredRateSearch.results[0]?.id === structuredRate.id,
    'structured rate search should retrieve the correct memory',
  );
  assert(
    structuredRateSearch.results[0]?.segmentPath === '$/price/basis',
    'structured rate probe should exercise a narrow matched field',
  );
  assert(
    structuredRateSearch.results[0]?.excerpt.includes('$/price/amount: 18.25'),
    'narrow field match should include answer-bearing sibling context',
  );

  const highConfidence = await call(client, 'memory_search', {
    query: 'warehouse lease',
    spaceIds: ['live-probe'],
    minConfidence: 0.9,
    mode: 'lexical',
  });
  assert(highConfidence.results[0]?.id === lease.id, 'minimum-confidence search filter');

  const provenanceSearch = await call(client, 'memory_search', {
    query: 'contract-17',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    includeSourceMetadata: true,
  });
  assert(
    provenanceSearch.results[0]?.id === lease.id,
    'source provenance must participate in recall',
  );
  assert(
    provenanceSearch.results[0]?.sources?.[0]?.metadata?.contractVersion === 17,
    'source metadata should be available explicitly',
  );

  const exactId = await call(client, 'memory_search', {
    query: lease.id,
    spaceIds: ['live-probe'],
    mode: 'lexical',
    explain: true,
  });
  assert(exactId.results[0]?.id === lease.id, 'exact memory ID must be a recall path');
  assert(exactId.results[0]?.score?.exactBoost > 0, 'exact recall should explain its boost');

  const temporal = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Mercury supplier rate card',
    kind: 'commercial-term',
    content: 'The negotiated supplier rate is 47 euros per unit.',
    validFrom: '2030-01-01T00:00:00.000Z',
    validTo: '2040-01-01T00:00:00.000Z',
  });
  const beforeValidity = await call(client, 'memory_search', {
    query: 'Mercury supplier rate card',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    validAt: '2029-12-31T00:00:00.000Z',
  });
  assert(
    !beforeValidity.results.some((item) => item.id === temporal.id),
    'valid-time search should exclude a future fact',
  );
  const currentValidity = await call(client, 'memory_search', {
    query: 'Mercury supplier rate card',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(
    !currentValidity.results.some((item) => item.id === temporal.id),
    'ordinary search should enforce present-time validity',
  );
  const duringValidity = await call(client, 'memory_search', {
    query: 'Mercury supplier rate card',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    validAt: '2035-01-01T00:00:00.000Z',
  });
  assert(
    duringValidity.results[0]?.id === temporal.id,
    'valid-time search should include an applicable fact',
  );
  const pastTemporal = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Legacy Atlas support window',
    kind: 'commercial-term',
    content: 'Atlas support was available during 2020 only.',
    validFrom: '2020-01-01T00:00:00.000Z',
    validTo: '2021-01-01T00:00:00.000Z',
  });
  const afterPastValidity = await call(client, 'memory_search', {
    query: 'Legacy Atlas support window',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(
    !afterPastValidity.results.some((item) => item.id === pastTemporal.id),
    'ordinary search should exclude information whose validity ended',
  );
  const duringPastValidity = await call(client, 'memory_search', {
    query: 'Legacy Atlas support window',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    validAt: '2020-06-01T00:00:00.000Z',
  });
  assert(
    duringPastValidity.results[0]?.id === pastTemporal.id,
    'explicit historical validity should recover ended information',
  );

  const firstPage = await call(client, 'memory_list', {
    spaceId: 'live-probe',
    limit: 1,
  });
  assert(firstPage.items.length === 1, 'memory list page size');
  assert(firstPage.items[0].content === undefined, 'memory list should return summaries');
  assert(typeof firstPage.nextCursor === 'string', 'memory list should return an opaque cursor');
  const secondPage = await call(client, 'memory_list', {
    spaceId: 'live-probe',
    limit: 1,
    cursor: firstPage.nextCursor,
  });
  assert(secondPage.items.length === 1, 'memory list second page');
  assert(firstPage.items[0].id !== secondPage.items[0].id, 'memory list pages must not overlap');

  if (modelsEnabled) {
    const semantic = await call(client, 'memory_search', {
      query: 'When does the logistics property agreement expire?',
      spaceIds: ['live-probe'],
      mode: 'quality',
    });
    assert(!semantic.degraded, 'quality search should have both Qwen models available');
    assert(semantic.results[0]?.id === lease.id, 'semantic paraphrase should retrieve the lease');

    const lithuanian = await call(client, 'memory_search', {
      query: 'Kokį viešbutį rinktis kelionei?',
      spaceIds: ['live-probe'],
      mode: 'quality',
    });
    assert(
      lithuanian.results[0]?.id === preference.id,
      `Lithuanian semantic recall: ${JSON.stringify(lithuanian)}`,
    );
  } else {
    const degraded = await call(client, 'memory_search', {
      query: 'Kaunas warehouse lease',
      spaceIds: ['live-probe'],
      mode: 'auto',
    });
    assert(degraded.degraded, 'auto mode must explicitly report model degradation');
    assert(degraded.results[0]?.id === lease.id, 'degraded lexical retrieval should still work');
  }

  await new Promise((resolve) => setTimeout(resolve, 20));
  const revised = await call(client, 'memory_revise', {
    memoryId: lease.id,
    expectedRevisionId: lease.currentRevisionId,
    spaceId: 'live-probe',
    title: 'Warehouse lease renewal',
    kind: 'agreement',
    content: {
      organization: 'Acme Logistics',
      statement: 'The company extended its Kaunas warehouse lease through December 2032.',
      annualCostEur: 88000,
    },
    tags: ['operations', 'property'],
    sources: [{ uri: 'urn:probe:amendment-2', type: 'contract-amendment' }],
    reviewAfter: '2999-01-01T00:00:00.000Z',
  });
  assert(revised.revision.revisionNumber === 2, 'revision number must advance');

  await expectToolError(client, 'memory_revise', {
    memoryId: lease.id,
    expectedRevisionId: lease.currentRevisionId,
    spaceId: 'live-probe',
    title: 'Stale concurrent update',
    content: 'This write must not be accepted.',
  });

  const history = await call(client, 'memory_history', { memoryId: lease.id });
  assert(history.revisions.length === 2, 'append-only history should retain both revisions');
  assert(history.revisions[0]?.content === undefined, 'history content should be opt-in');
  assert(history.revisions[0]?.contentHash === undefined, 'history should hide internal hashes');
  assert(history.revisions[0]?.reviewDue === undefined, 'future review should not warn');
  assert(history.revisions[1]?.reviewDue === true, 'historical overdue review should be retained');
  const detailedHistory = await call(client, 'memory_history', {
    memoryId: lease.id,
    includeContent: true,
    limit: 1,
  });
  assert(detailedHistory.revisions[0]?.content !== undefined, 'detailed history content');
  assert(typeof detailedHistory.nextCursor === 'string', 'history pagination cursor');
  const olderHistory = await call(client, 'memory_history', {
    memoryId: lease.id,
    includeContent: true,
    limit: 1,
    cursor: detailedHistory.nextCursor,
  });
  assert(olderHistory.revisions[0]?.revisionNumber === 1, 'history second page');
  assert(olderHistory.nextCursor === null, 'history final page');
  const historical = await call(client, 'memory_get', {
    memoryId: lease.id,
    atTime: originalRecordedAt,
  });
  assert(historical.revision.revisionNumber === 1, 'time-travel read should return revision one');
  assert(historical.revision.content !== undefined, 'memory_get should retain complete content');
  assert(
    historical.revision.searchableText === undefined,
    'internal search projection must be hidden',
  );
  assert(historical.revision.contentHash === undefined, 'memory_get should hide internal hash');
  const historicalSearch = await call(client, 'memory_search', {
    query: 'December 2030',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    atTime: originalRecordedAt,
  });
  assert(
    historicalSearch.results[0]?.id === lease.id,
    'historical search should retrieve the revision valid at the requested record time',
  );
  const currentOldTerm = await call(client, 'memory_search', {
    query: 'December 2030',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  const currentLeaseResult = currentOldTerm.results.find((item) => item.id === lease.id);
  assert(
    !currentLeaseResult ||
      (currentLeaseResult.excerpt.includes('2032') && !currentLeaseResult.excerpt.includes('2030')),
    'current search may recall the memory but must not leak superseded revision text',
  );

  await call(client, 'space_create', {
    id: 'live-probe-isolated',
    name: 'Isolation probe',
  });
  const isolated = await call(client, 'memory_create', {
    spaceId: 'live-probe-isolated',
    title: 'Unrelated isolated memory',
    content: 'This belongs to a different memory space.',
  });
  await expectToolError(client, 'memory_link', {
    fromMemoryId: lease.id,
    toMemoryId: isolated.id,
    relation: 'must_not_cross_spaces',
  });

  const link = await call(client, 'memory_link', {
    fromMemoryId: lease.id,
    toMemoryId: preference.id,
    relation: 'discussed_with',
    metadata: { test: true },
  });
  const traversal = await call(client, 'memory_traverse', { memoryId: lease.id, maxDepth: 2 });
  assert(
    traversal.items.some((entry) => entry.memory.id === preference.id),
    'graph traversal',
  );
  await call(client, 'memory_feedback', {
    memoryId: lease.id,
    signal: 'verified',
    value: 1,
    note: 'Verified by live MCP probe',
  });
  const firstUnlink = await call(client, 'memory_unlink', { linkId: link.id });
  const secondUnlink = await call(client, 'memory_unlink', { linkId: link.id });
  assert(firstUnlink.deletedAt === secondUnlink.deletedAt, 'unlink must be idempotent');

  const resource = await client.readResource({
    uri: `memory://spaces/live-probe/memories/${lease.id}`,
  });
  assert(resource.contents.length === 1, 'memory resource should be readable');
  assert(
    !JSON.stringify(resource.contents).includes('searchableText'),
    'memory resource must hide internal search projection',
  );
  assert(
    !JSON.stringify(resource.contents).includes('contentHash'),
    'memory resource must hide internal content hash',
  );

  const status = await call(client, 'memory_status', { probeModels: modelsEnabled });
  if (modelsEnabled) {
    assert(status.modelWorkerStarts === 1, 'all operations must reuse one model worker');
    assert(typeof status.modelLauncherPid === 'number', 'model launcher must be running');
    assert(typeof status.modelWorkerPid === 'number', 'model worker must be running');
    assert(
      status.modelWorkerPid === status.modelHealth?.pid,
      'status must report one coherent model-worker PID snapshot',
    );
    assert(status.modelHealth?.embedding_loaded, 'embedding model must be loaded');
    assert(status.modelHealth?.reranker_loaded, 'reranker model must be loaded');
    assert(status.modelProfiles === 1, 'all vectors should use one pinned model profile');
  }

  await call(client, 'memory_archive', { memoryId: preference.id });
  const afterArchive = await call(client, 'memory_search', {
    query: 'Kelionių pageidavimas',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(
    !afterArchive.results.some((item) => item.id === preference.id),
    'archived memory exclusion',
  );
  const defaultInventoryAfterArchive = await call(client, 'memory_list', {
    spaceId: 'live-probe',
  });
  assert(
    !defaultInventoryAfterArchive.items.some((item) => item.id === preference.id),
    'default memory inventory should exclude archived memories',
  );
  const archivedInventory = await call(client, 'memory_list', {
    spaceId: 'live-probe',
    state: 'archived',
  });
  assert(
    archivedInventory.items.some((item) => item.id === preference.id),
    'explicit archived inventory should include archived memories',
  );
  const archivedRecall = await call(client, 'memory_search', {
    query: 'KelioniÅ³ pageidavimas',
    spaceIds: ['live-probe'],
    states: ['archived'],
    mode: 'lexical',
  });
  assert(
    archivedRecall.results.some((item) => item.id === preference.id),
    'explicit archived-state recall',
  );
  const beforeArchive = await call(client, 'memory_search', {
    query: 'KelioniÅ³ pageidavimas',
    spaceIds: ['live-probe'],
    atTime: preference.revision.recordedAt,
    mode: 'lexical',
  });
  assert(
    beforeArchive.results.some((item) => item.id === preference.id),
    'record-time search should use the historical lifecycle state',
  );
  assert(
    beforeArchive.results.find((item) => item.id === preference.id)?.state === 'active',
    'record-time search should serialize the historical lifecycle state',
  );
  const historicalPreference = await call(client, 'memory_get', {
    memoryId: preference.id,
    atTime: preference.revision.recordedAt,
  });
  assert(
    historicalPreference.state === 'active',
    'record-time read should return historical state',
  );
  const restoredPreference = await call(client, 'memory_restore', { memoryId: preference.id });
  assert(restoredPreference.state === 'active', 'restored memory should return to active state');
  const defaultInventoryAfterRestore = await call(client, 'memory_list', {
    spaceId: 'live-probe',
  });
  assert(
    defaultInventoryAfterRestore.items.some((item) => item.id === preference.id),
    'restored memory should return to the default active inventory',
  );
  const restoredRecall = await call(client, 'memory_search', {
    query: 'Kelionių pageidavimas',
    spaceIds: ['live-probe'],
    mode: 'lexical',
  });
  assert(
    restoredRecall.results.some((item) => item.id === preference.id),
    'restored memory should return to normal recall',
  );

  const disposable = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Disposable memory',
    kind: 'test-draft',
    content: { secret: 'This content must be permanently erased.' },
    tags: ['delete-probe'],
    sources: [{ uri: 'urn:probe:permanent-deletion', type: 'test-fixture' }],
  });
  await call(client, 'memory_link', {
    fromMemoryId: lease.id,
    toMemoryId: disposable.id,
    relation: 'temporary-test-link',
  });
  await call(client, 'memory_feedback', {
    memoryId: disposable.id,
    signal: 'test-only',
  });
  const inspectionDatabase = new Database(path.join(dataDir, 'memory.db'), { readonly: true });
  const disposableSegmentIds = inspectionDatabase
    .prepare('SELECT id FROM memory_segments WHERE memory_id = ? ORDER BY id')
    .all(disposable.id)
    .map((row) => String(row.id));
  inspectionDatabase.close();
  assert(disposableSegmentIds.length > 0, 'deletion fixture should have indexed segments');
  if (!modelsEnabled) seedDeletionProbeVector(disposableSegmentIds[0]);
  const deletionRevisionIds = [disposable.revision.id];
  const rowsBeforeDeletion = deletionRowCounts(
    disposable.id,
    deletionRevisionIds,
    disposableSegmentIds,
  );
  for (const [table, rowCount] of Object.entries(rowsBeforeDeletion)) {
    assert(rowCount > 0, `deletion fixture should populate ${table}`);
  }
  const deletion = await call(client, 'memory_delete', { memoryId: disposable.id });
  assert(deletion.id === disposable.id && deletion.deleted === true, 'deletion acknowledgement');
  await expectToolError(client, 'memory_get', { memoryId: disposable.id });
  await expectToolError(client, 'memory_history', { memoryId: disposable.id });
  await call(client, 'memory_delete', { memoryId: disposable.id });
  const afterDeleteTraversal = await call(client, 'memory_traverse', {
    memoryId: lease.id,
    maxDepth: 2,
  });
  assert(
    !afterDeleteTraversal.items.some((entry) => entry.memory.id === disposable.id),
    'permanent deletion should remove relationships',
  );
  const rowsAfterDeletion = deletionRowCounts(
    disposable.id,
    deletionRevisionIds,
    disposableSegmentIds,
  );
  for (const [table, rowCount] of Object.entries(rowsAfterDeletion)) {
    assert(rowCount === 0, `permanent deletion should purge ${table}`);
  }

  await client.close();
  activeClient = undefined;
  client = await connect();
  const persisted = await call(client, 'memory_get', { memoryId: lease.id });
  assert(persisted.revision.revisionNumber === 2, 'memory must survive MCP process restart');
  await client.close();
  activeClient = undefined;

  return {
    ok: true,
    modelsEnabled,
    toolCount: tools.tools.length,
    leaseMemoryId: lease.id,
    linkId: link.id,
    revisions: history.revisions.length,
    persistedRevision: persisted.revision.revisionNumber,
    physicalDeletionVerified: true,
    payloadCharacters: {
      createAcknowledgement: JSON.stringify(lease).length,
      listPage: JSON.stringify(firstPage).length,
      search: JSON.stringify(lexical).length,
      historySummary: JSON.stringify(history).length,
      detailedHistoryPage: JSON.stringify(detailedHistory).length,
      fullHistoricalGet: JSON.stringify(historical).length,
    },
    dataDir,
  };
}

let outcome;
let failure;
try {
  outcome = await run();
} catch (error) {
  failure = error;
}
if (activeClient) {
  try {
    await activeClient.close();
  } catch (closeError) {
    process.stderr.write(`Live probe close warning: ${String(closeError)}\n`);
  }
  activeClient = undefined;
}
await new Promise((resolve) => setTimeout(resolve, 1_500));
try {
  const resolvedTemp = path.resolve(tmpdir());
  const resolvedData = path.resolve(dataDir);
  if (resolvedData.startsWith(`${resolvedTemp}${path.sep}`)) {
    rmSync(resolvedData, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
} catch (cleanupError) {
  process.stderr.write(`Live probe cleanup warning: ${String(cleanupError)}\n`);
}
if (failure) {
  throw failure;
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
