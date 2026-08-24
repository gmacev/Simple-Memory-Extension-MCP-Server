#!/usr/bin/env node
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';
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
    const dimensions = Number.parseInt(process.env.SIMPLE_MEMORY_EMBEDDING_DIMENSION ?? '896', 10);
    database
      .prepare(
        `INSERT INTO memory_vectors(segment_id, embedding, model_profile_id)
         VALUES (?, ?, ?)`,
      )
      .run(segmentId, Buffer.from(new Float32Array(dimensions).buffer), 'deletion-integrity-probe');
  } finally {
    database.close();
  }
}

function toolResult(response) {
  const text = response.content.find((item) => item.type === 'text');
  if (!text) throw new Error('Tool did not return JSON text content');
  return JSON.parse(text.text);
}

function legacySearchEnvelope(query, compact, explained) {
  const explainedById = new Map(explained.results.map((item) => [item.id, item]));
  return {
    query,
    mode: explained.mode,
    degraded: explained.degraded ?? false,
    timingMs: explained.timingMs,
    results: compact.results.map((item) => {
      const diagnostics = explainedById.get(item.id);
      const headers = [
        item.title ? `Title: ${item.title}` : null,
        item.kind ? `Kind: ${item.kind}` : null,
        item.tags?.length ? `Tags: ${item.tags.join(', ')}` : null,
      ].filter(Boolean);
      return {
        ...item,
        revisionNumber: 1,
        currentRevisionId: item.currentRevisionId ?? item.revisionId,
        isCurrentRevision: item.currentRevisionId === undefined,
        excerpt: [...headers, item.excerpt].join('\n'),
        segmentPath: diagnostics?.segmentPath,
        relevanceScore: diagnostics?.score?.fusedScore,
        resourceUri: `memory://spaces/${item.spaceId}/memories/${item.id}`,
      };
    }),
  };
}

async function connect() {
  const client = new Client(
    { name: 'simple-memory-live-probe', version: '3.9.0' },
    { versionNegotiation: { mode: 'auto' } },
  );
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
  assert(response.structuredContent !== undefined, `${name} should return structuredContent`);
  assert(
    JSON.stringify(response.structuredContent) === text?.text,
    `${name} structuredContent should match canonical JSON`,
  );
  assert(
    !JSON.stringify(parsed).includes('"resourceUri"'),
    `${name} should use MCP resource links instead of JSON resourceUri fields`,
  );
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
    'memory_feedback_list',
    'memory_status',
    'memory_archive',
    'memory_restore',
    'memory_delete',
    'space_delete',
    'space_restore',
  ]) {
    assert(names.has(required), `missing MCP tool ${required}`);
  }
  const archiveTool = tools.tools.find((tool) => tool.name === 'memory_archive');
  const restoreTool = tools.tools.find((tool) => tool.name === 'memory_restore');
  const deleteTool = tools.tools.find((tool) => tool.name === 'memory_delete');
  assert(
    archiveTool?.description?.includes('recoverable information') &&
      archiveTool.description.includes('delete only for erasure'),
    'archive description should distinguish recoverable retention from permanent deletion',
  );
  assert(
    restoreTool?.description?.includes('normal recall') &&
      restoreTool.description.includes('without changing its content'),
    'restore description should explain reactivation without revision changes',
  );
  assert(
    deleteTool?.description?.includes('Irreversibly erase') &&
      deleteTool.description.includes('links') &&
      deleteTool.description.includes('merge redirects'),
    'delete description should disclose complete irreversible erasure',
  );
  assert(deleteTool?.annotations?.destructiveHint === true, 'delete must be marked destructive');

  const space = await call(client, 'space_create', {
    id: 'live-probe',
    name: 'Live probe',
    description: 'Isolated end-to-end verification',
  });
  assert(space.id === 'live-probe', 'space creation');
  assert(space.name === undefined && typeof space.createdAt === 'string', 'compact space creation');
  const spaces = await call(client, 'space_list');
  const listedSpace = spaces.items.find((item) => item.id === space.id);
  assert(listedSpace?.name === 'Live probe', 'space list should retain identifying details');
  assert(listedSpace?.createdAt === undefined, 'space list should omit creation timestamps');

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
        observedAt: '2026-01-15T12:00:00.000Z',
        metadata: { contractVersion: 17 },
      },
    ],
    observedAt: '2026-01-15T12:00:00.000Z',
    confidence: 0.95,
    reviewAfter: '2000-01-01T00:00:00.000Z',
    idempotencyKey: 'probe-lease',
  });
  assert(lease.indexStatus === (modelsEnabled ? 'ready' : 'lexical-only'), 'expected index status');
  const originalRecordedAt = lease.recordedAt;
  assert(typeof lease.revisionId === 'string', 'create response should identify the revision');
  assert(lease.revision === undefined, 'create response should not duplicate revision details');
  assert(
    lease.currentRevisionId === undefined,
    'create response should not duplicate current revision',
  );

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
    observedAt: '2026-02-01T09:00:00.000Z',
    sources: [
      {
        uri: 'urn:probe:northline-rate',
        type: 'rate-card',
        observedAt: '2026-02-01T09:00:00.000Z',
        metadata: { version: 4 },
      },
    ],
  });
  const structuredRateDetail = await call(client, 'memory_get', { memoryId: structuredRate.id });
  assert(
    structuredRateDetail.revision.observedAt === '2026-02-01T09:00:00.000Z' &&
      structuredRateDetail.revision.sources[0].observedAt === undefined &&
      structuredRateDetail.revision.sources[0].metadata.version === 4,
    'direct reads should keep provenance while omitting duplicate source observation time',
  );

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
  assert(lexical.query === undefined && lexical.timingMs === undefined, 'ordinary search metadata');
  assert(lexical.degraded === undefined, 'healthy search should omit degraded false');
  assert(lexical.results[0]?.relevanceScore === undefined, 'duplicate relevance score is omitted');
  assert(lexical.results[0]?.score === undefined, 'rank diagnostics should be opt-in');
  assert(lexical.results[0]?.segmentPath === undefined, 'segment diagnostics should be opt-in');
  assert(
    lexical.results[0]?.sources?.[0]?.metadata === undefined,
    'source metadata should be opt-in',
  );
  assert(
    lexical.results[0]?.observedAt === '2026-01-15T12:00:00.000Z' &&
      lexical.results[0]?.sources?.[0]?.observedAt === undefined,
    'source observation time should not duplicate the memory observation time',
  );
  assert(
    lexical.results[0]?.revisionId === lease.revisionId,
    'search result should identify the returned revision',
  );
  assert(
    lexical.results[0]?.currentRevisionId === undefined &&
      lexical.results[0]?.isCurrentRevision === undefined,
    'current search result should omit current-revision duplication',
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
    !structuredRateSearch.results[0]?.excerpt.startsWith('Title:'),
    'search excerpts should omit duplicate structured headers',
  );
  assert(
    structuredRateSearch.results[0]?.excerpt.includes('$/price/amount: 18.25'),
    'narrow field match should include answer-bearing sibling context',
  );
  const explainedStructuredRateSearch = await call(client, 'memory_search', {
    query: 'temperature-controlled pallet',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    explain: true,
  });
  assert(
    explainedStructuredRateSearch.results[0]?.segmentPath === '$/price/basis',
    'explained search should expose the matched field',
  );
  assert(
    typeof explainedStructuredRateSearch.results[0]?.score?.fusedScore === 'number' &&
      typeof explainedStructuredRateSearch.timingMs === 'number' &&
      explainedStructuredRateSearch.mode === 'lexical',
    'explained search should expose ranking diagnostics',
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
  assert(
    firstPage.items[0].state === undefined && firstPage.items[0].revisionNumber === undefined,
    'memory list should omit request-fixed state and revision number',
  );
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
    assert(
      semantic.degraded === undefined,
      'quality search should have both configured models available',
    );
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
    expectedRevisionId: lease.revisionId,
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
  assert(revised.revisionId !== lease.revisionId, 'revision identifier must advance');
  assert(revised.revision === undefined, 'revision acknowledgement should remain compact');

  await expectToolError(client, 'memory_revise', {
    memoryId: lease.id,
    expectedRevisionId: lease.revisionId,
    spaceId: 'live-probe',
    title: 'Stale concurrent update',
    content: 'This write must not be accepted.',
  });

  const history = await call(client, 'memory_history', { memoryId: lease.id });
  assert(history.revisions.length === 2, 'append-only history should retain both revisions');
  assert(history.memoryId === undefined, 'history should not echo the requested memory ID');
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
  assert(olderHistory.nextCursor === undefined, 'history final page should omit an empty cursor');
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
  assert(
    historical.currentRevisionId === revised.revisionId,
    'historical reads should identify the current revision',
  );
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
  assert(
    historicalSearch.results[0]?.currentRevisionId === revised.revisionId,
    'historical search should identify the current revision',
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
  assert(
    traversal.query === undefined && traversal.items.every((entry) => entry.via === undefined),
    'compact traversal should omit echoed query and duplicated via links',
  );
  const rankedTraversal = await call(client, 'memory_traverse', {
    memoryId: lease.id,
    maxDepth: 2,
    query: 'quiet nature hotel',
  });
  assert(
    rankedTraversal.items.every(
      (entry) => entry.relevanceScore === undefined && entry.rerankerScore === undefined,
    ),
    'traversal ranking diagnostics should be opt-in',
  );
  const explainedTraversal = await call(client, 'memory_traverse', {
    memoryId: lease.id,
    maxDepth: 2,
    query: 'quiet nature hotel',
    explain: true,
  });
  assert(
    explainedTraversal.items.some((entry) => typeof entry.relevanceScore === 'number'),
    'explained traversal should expose ranking diagnostics',
  );
  const verifiedFeedback = await call(client, 'memory_feedback', {
    memoryId: lease.id,
    scope: 'content',
    signal: 'verified',
    actorType: 'system',
    note: 'Verified by live MCP probe',
    idempotencyKey: 'live-probe-verified-feedback',
  });
  assert(verifiedFeedback.revisionId === revised.revisionId, 'feedback should target revision');
  assert(
    verifiedFeedback.signal === undefined && verifiedFeedback.actorType === undefined,
    'feedback acknowledgement should not echo submitted values',
  );
  const feedbackRead = await call(client, 'memory_feedback_list', {
    memoryId: lease.id,
    revisionId: revised.revisionId,
    includeDetails: true,
  });
  assert(feedbackRead.items[0]?.signal === 'verified', 'feedback should be readable');

  const feedbackFixture = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Revision-aware feedback fixture',
    kind: 'feedback-probe',
    content: { marker: 'revision-aware-feedback-probe', version: 1 },
    tags: ['feedback-probe'],
  });
  const beforeConcern = new Date().toISOString();
  await new Promise((resolve) => setTimeout(resolve, 5));
  const concern = await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    scope: 'content',
    signal: 'incorrect',
    actorType: 'user',
    note: 'The user corrected this revision.',
    metadata: { test: true },
    idempotencyKey: 'feedback-probe-concern',
  });
  assert(concern.revisionId === feedbackFixture.revisionId, 'content feedback current revision');
  const concerned = await call(client, 'memory_get', { memoryId: feedbackFixture.id });
  assert(
    concerned.feedbackSummary.feedbackStatus === 'needs-review',
    'incorrect feedback should require review',
  );
  assert(
    concerned.currentRevisionId === undefined &&
      concerned.feedbackSummary.revisionId === undefined &&
      concerned.feedbackSummary.retrievalEventCount === undefined,
    'current memory reads should omit duplicate revision and zero feedback counts',
  );
  const beforeConcernRead = await call(client, 'memory_get', {
    memoryId: feedbackFixture.id,
    atTime: beforeConcern,
  });
  assert(
    beforeConcernRead.feedbackSummary.feedbackStatus === 'unreviewed',
    'historical read must exclude future feedback',
  );
  assert(
    beforeConcernRead.feedbackSummary.contentEventCount === undefined &&
      beforeConcernRead.feedbackSummary.retrievalEventCount === undefined,
    'zero historical feedback counts should be omitted',
  );
  const feedbackRevision = await call(client, 'memory_revise', {
    memoryId: feedbackFixture.id,
    expectedRevisionId: feedbackFixture.revisionId,
    title: 'Revision-aware feedback fixture',
    kind: 'feedback-probe',
    content: { marker: 'revision-aware-feedback-probe', version: 2 },
    tags: ['feedback-probe'],
  });
  const retriedConcern = await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    scope: 'content',
    signal: 'incorrect',
    actorType: 'user',
    note: 'The user corrected this revision.',
    metadata: { test: true },
    idempotencyKey: 'feedback-probe-concern',
  });
  assert(retriedConcern.id === concern.id, 'feedback retry should return original event');
  assert(
    retriedConcern.revisionId === feedbackFixture.revisionId,
    'feedback retry should retain original revision',
  );
  await expectToolError(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    scope: 'content',
    signal: 'stale',
    actorType: 'user',
    idempotencyKey: 'feedback-probe-concern',
  });
  const cleanRevision = await call(client, 'memory_get', { memoryId: feedbackFixture.id });
  assert(
    cleanRevision.revision.id === feedbackRevision.revisionId &&
      cleanRevision.feedbackSummary.feedbackStatus === 'unreviewed',
    'new revision should not inherit old feedback',
  );
  await expectToolError(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    scope: 'retrieval',
    signal: 'relevant',
    actorType: 'agent',
    query: 'revision-aware feedback probe',
  });
  await expectToolError(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    revisionId: feedbackRevision.revisionId,
    scope: 'retrieval',
    signal: 'relevant',
    actorType: 'agent',
  });
  const beforeRetrievalFeedback = await call(client, 'memory_search', {
    query: 'revision-aware-feedback-probe',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    explain: true,
  });
  await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    revisionId: feedbackRevision.revisionId,
    scope: 'retrieval',
    signal: 'relevant',
    actorType: 'agent',
    query: 'revision-aware-feedback-probe',
  });
  const afterRetrievalFeedback = await call(client, 'memory_search', {
    query: 'revision-aware-feedback-probe',
    spaceIds: ['live-probe'],
    mode: 'lexical',
    explain: true,
  });
  assert(
    JSON.stringify(beforeRetrievalFeedback.results.map((item) => [item.id, item.score])) ===
      JSON.stringify(afterRetrievalFeedback.results.map((item) => [item.id, item.score])),
    'retrieval feedback must not change ranking',
  );
  await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    revisionId: feedbackRevision.revisionId,
    scope: 'content',
    signal: 'verified',
    actorType: 'external',
  });
  const canonicalMergeFixture = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    logicalKey: 'payload-merge-canonical',
    title: 'Canonical merge fixture',
    content: { marker: 'canonical-merge-fixture' },
  });
  const duplicateMergeFixture = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    logicalKey: 'payload-merge-duplicate',
    title: 'Duplicate merge fixture',
    content: { marker: 'duplicate-merge-fixture' },
  });
  const merge = await call(client, 'memory_merge', {
    canonicalMemoryId: canonicalMergeFixture.id,
    expectedCanonicalRevisionId: canonicalMergeFixture.revisionId,
    duplicates: [
      {
        memoryId: duplicateMergeFixture.id,
        expectedRevisionId: duplicateMergeFixture.revisionId,
      },
    ],
    actorId: 'live-probe',
    reason: 'Payload acknowledgement verification',
  });
  assert(
    merge.canonicalMemoryId === canonicalMergeFixture.id &&
      merge.canonicalRevisionId === canonicalMergeFixture.revisionId &&
      merge.mergedMemoryIds[0] === duplicateMergeFixture.id,
    'merge acknowledgement should retain workflow identifiers',
  );
  assert(
    merge.actorId === undefined &&
      merge.reason === undefined &&
      merge.redirectedMemoryCount === undefined,
    'merge acknowledgement should omit echoes and redundant counts',
  );
  const redirectedByKey = await call(client, 'memory_get_by_key', {
    spaceId: 'live-probe',
    logicalKey: 'payload-merge-duplicate',
  });
  assert(
    redirectedByKey.redirectedFromMemoryId === duplicateMergeFixture.id &&
      redirectedByKey.memory.id === canonicalMergeFixture.id &&
      redirectedByKey.logicalKey === undefined &&
      redirectedByKey.matchedMemoryId === undefined &&
      redirectedByKey.redirected === undefined,
    'logical-key retrieval should report only a meaningful redirect',
  );
  const verifiedFixture = await call(client, 'memory_get', { memoryId: feedbackFixture.id });
  assert(verifiedFixture.feedbackSummary.feedbackStatus === 'verified', 'verified status');
  await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    revisionId: feedbackRevision.revisionId,
    scope: 'content',
    signal: 'correct',
    actorType: 'agent',
  });
  const supportedFixture = await call(client, 'memory_get', { memoryId: feedbackFixture.id });
  assert(supportedFixture.feedbackSummary.feedbackStatus === 'supported', 'latest correct status');
  await call(client, 'memory_feedback', {
    memoryId: feedbackFixture.id,
    revisionId: feedbackRevision.revisionId,
    scope: 'content',
    signal: 'stale',
    actorType: 'agent',
  });
  const staleFixture = await call(client, 'memory_get', { memoryId: feedbackFixture.id });
  assert(staleFixture.feedbackSummary.feedbackStatus === 'needs-review', 'latest stale status');
  const compactFeedback = await call(client, 'memory_feedback_list', {
    memoryId: feedbackFixture.id,
    limit: 2,
  });
  assert(compactFeedback.nextCursor, 'feedback history should paginate');
  assert(
    compactFeedback.items.every(
      (item) =>
        item.memoryId === undefined && item.note === undefined && item.metadata === undefined,
    ),
    'feedback history should be compact by default',
  );
  const detailedFeedback = await call(client, 'memory_feedback_list', {
    memoryId: feedbackFixture.id,
    includeDetails: true,
    limit: 100,
  });
  assert(
    detailedFeedback.items.some((item) => item.note === 'The user corrected this revision.'),
    'feedback details should be opt-in',
  );
  const secondConcern = await call(client, 'memory_create', {
    spaceId: 'live-probe',
    title: 'Second feedback review fixture',
    kind: 'feedback-probe',
    content: { marker: 'second-feedback-review-fixture' },
  });
  await call(client, 'memory_feedback', {
    memoryId: secondConcern.id,
    scope: 'content',
    signal: 'contradicted',
    actorType: 'external',
  });
  const payloadQuery = 'feedback-probe';
  const compactPayloadSearch = await call(client, 'memory_search', {
    query: payloadQuery,
    spaceIds: ['live-probe'],
    mode: 'lexical',
    topK: 2,
  });
  const explainedPayloadSearch = await call(client, 'memory_search', {
    query: payloadQuery,
    spaceIds: ['live-probe'],
    mode: 'lexical',
    topK: 2,
    explain: true,
  });
  assert(compactPayloadSearch.results.length === 2, 'payload fixture should return two results');
  assert(
    JSON.stringify(compactPayloadSearch.results.map((item) => item.id)) ===
      JSON.stringify(explainedPayloadSearch.results.map((item) => item.id)),
    'explain must not change candidate selection or ordering',
  );
  const legacyPayload = legacySearchEnvelope(
    payloadQuery,
    compactPayloadSearch,
    explainedPayloadSearch,
  );
  assert(
    JSON.stringify(compactPayloadSearch).length <= JSON.stringify(legacyPayload).length * 0.8,
    'two-result compact search should reduce the legacy-equivalent payload by at least 20%',
  );
  const reviewPageOne = await call(client, 'memory_list', {
    spaceId: 'live-probe',
    feedbackStatus: 'needs-review',
    limit: 1,
  });
  const reviewPageTwo = await call(client, 'memory_list', {
    spaceId: 'live-probe',
    feedbackStatus: 'needs-review',
    limit: 1,
    cursor: reviewPageOne.nextCursor,
  });
  assert(reviewPageOne.items.length === 1 && reviewPageOne.nextCursor, 'review page one');
  assert(reviewPageTwo.items.length === 1, 'review filter must apply before pagination');
  assert(
    new Set([...reviewPageOne.items, ...reviewPageTwo.items].map((item) => item.id)).size === 2,
    'review pagination should return distinct matching memories',
  );
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
  const provenanceResource = await client.readResource({
    uri: `memory://spaces/live-probe/memories/${structuredRate.id}`,
  });
  const resourceMemory = JSON.parse(provenanceResource.contents[0].text);
  assert(
    resourceMemory.revision.sources[0].observedAt === '2026-02-01T09:00:00.000Z',
    'canonical resources should retain the complete source representation',
  );

  const compactStatus = await call(client, 'memory_status', {});
  assert(
    Number.isInteger(compactStatus.schemaVersion) && compactStatus.schemaVersion > 0,
    'compact status should expose the schema version',
  );
  assert(compactStatus.database === undefined, 'compact status should hide storage diagnostics');
  assert(
    compactStatus.modelLauncherPid === undefined,
    'compact status should hide process diagnostics',
  );
  const detailedStatus = await call(client, 'memory_status', { includeDetails: true });
  assert(
    typeof detailedStatus.database === 'string',
    'detailed status should expose storage diagnostics',
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
    atTime: preference.recordedAt,
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
    atTime: preference.recordedAt,
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
    scope: 'content',
    signal: 'stale',
    actorType: 'system',
  });
  const inspectionDatabase = new Database(path.join(dataDir, 'memory.db'), { readonly: true });
  const disposableSegmentIds = inspectionDatabase
    .prepare('SELECT id FROM memory_segments WHERE memory_id = ? ORDER BY id')
    .all(disposable.id)
    .map((row) => String(row.id));
  inspectionDatabase.close();
  assert(disposableSegmentIds.length > 0, 'deletion fixture should have indexed segments');
  if (!modelsEnabled) seedDeletionProbeVector(disposableSegmentIds[0]);
  const deletionRevisionIds = [disposable.revisionId];
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
      twoResultSearch: JSON.stringify(compactPayloadSearch).length,
      legacyEquivalentTwoResultSearch: JSON.stringify(legacyPayload).length,
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
