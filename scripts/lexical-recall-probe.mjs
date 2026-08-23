#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
// Correctness probe: scoped lexical recall must survive >10,000 global FTS matches.
// Guards against rank-pool-before-filter designs that starve small restricted spaces.
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { Indexer } from '../dist/indexing/indexer.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-lexical-recall-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

const NOISE_COUNT = Number(process.env.LEXICAL_RECALL_NOISE ?? 10_500);

function assert(condition, message) {
  if (!condition) throw new Error(`Lexical recall probe assertion failed: ${message}`);
}

async function run() {
  const config = loadConfig();
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const indexer = new Indexer(config, store, models, logger);
  const searchEngine = new SearchEngine(config, store, models, logger);
  const seedConnection = new Database(config.databasePath);
  seedConnection.pragma('journal_mode = WAL');
  seedConnection.pragma('foreign_keys = ON');

  try {
    for (const id of ['recall-noise-a', 'recall-noise-b', 'recall-target']) {
      store.createSpace({ id, name: id });
    }

    const stamp = new Date().toISOString();
    const insertMemory = seedConnection.prepare(
      `INSERT INTO memories (id, space_id, state, current_revision_id, created_at, updated_at, index_status, idempotency_key)
       VALUES (?, ?, 'active', ?, ?, ?, 'lexical-only', NULL)`,
    );
    const insertRevision = seedConnection.prepare(
      `INSERT INTO memory_revisions (id, memory_id, revision_number, parent_revision_id, title, kind,
                                     content_json, metadata_json, salience, confidence, observed_at,
                                     valid_from, valid_to, expires_at, review_after, recorded_at, actor,
                                     content_hash, searchable_text)
       VALUES (?, ?, 1, NULL, ?, 'note', '{}', '{}', 0.5, 0.5, NULL, NULL, NULL, NULL, NULL, ?, 'probe', ?, ?)`,
    );
    const insertSegment = seedConnection.prepare(
      `INSERT INTO memory_segments (id, memory_id, revision_id, space_id, ordinal, path, text, token_count, content_hash, model_profile_id)
       VALUES (?, ?, ?, ?, 0, '$', ?, 6, ?, NULL)`,
    );
    const insertFts = seedConnection.prepare(
      `INSERT INTO memory_fts (segment_id, memory_id, revision_id, space_id, title, text, tags)
       VALUES (?, ?, ?, ?, ?, ?, '')`,
    );

    seedConnection.transaction(() => {
      for (let index = 0; index < NOISE_COUNT; index += 1) {
        const spaceId = index % 2 === 0 ? 'recall-noise-a' : 'recall-noise-b';
        const memoryId = randomUUID();
        const revisionId = randomUUID();
        const segmentId = randomUUID();
        const title = `architecture blueprint ${index}`;
        const text = `architecture deployment pipeline cache index ${index}`;
        insertMemory.run(memoryId, spaceId, revisionId, stamp, stamp);
        insertRevision.run(revisionId, memoryId, title, stamp, randomUUID(), `${title}\n${text}`);
        insertSegment.run(segmentId, memoryId, revisionId, spaceId, text, randomUUID());
        insertFts.run(segmentId, memoryId, revisionId, spaceId, title, text);
      }
    })();

    // Scoped-space targets. The weak record must rank below every seeded match
    // globally: single occurrence of the term inside a long document.
    const fillerNarrative = `${'Unrelated operational narrative sentence for volume. '.repeat(40)}architecture appears exactly once here.`;
    const weakRecord = await indexer.indexRevision(
      store.createMemory(
        {
          spaceId: 'recall-target',
          title: 'long form review notes',
          kind: 'note',
          tags: ['target'],
          content: {
            topic: 'long form architecture review',
            notes: [fillerNarrative],
            flags: {},
            history: [],
          },
          metadata: {},
          salience: 0.5,
          confidence: 0.5,
          sources: [],
        },
        'probe',
      ).revision.id,
      false,
    );
    const strongRecord = await indexer.indexRevision(
      store.createMemory(
        {
          spaceId: 'recall-target',
          title: 'short architecture note',
          kind: 'note',
          tags: ['target'],
          content: {
            topic: 'architecture decisions',
            notes: ['architecture owns the data flow.'],
            flags: {},
            history: [],
          },
          metadata: {},
          salience: 0.5,
          confidence: 0.5,
          sources: [],
        },
        'probe',
      ).revision.id,
      false,
    );
    await indexer.indexRevision(
      store.createMemory(
        {
          spaceId: 'recall-target',
          title: 'unrelated cooking notes',
          kind: 'note',
          tags: ['target'],
          content: { topic: 'cooking', notes: ['boil water for pasta.'], flags: {}, history: [] },
          metadata: {},
          salience: 0.5,
          confidence: 0.5,
          sources: [],
        },
        'probe',
      ).revision.id,
      false,
    );

    const candidates = store.lexicalCandidates('architecture', { spaceIds: ['recall-target'] }, 10);
    assert(candidates.length >= 2, `expected scoped candidates, got ${candidates.length}`);
    assert(
      candidates.some((candidate) => candidate.memoryId === weakRecord.id),
      'weak low-rank target memory missing from scoped lexical candidates (pool-before-filter starvation)',
    );
    assert(
      candidates.some((candidate) => candidate.memoryId === strongRecord.id),
      'strong target memory missing from scoped lexical candidates',
    );
    assert(
      candidates[0].memoryId === strongRecord.id,
      'stronger target should outrank the weak long-document target',
    );
    assert(
      candidates.every((candidate) => candidate.text.length > 0),
      'hydrated candidate text missing',
    );

    const filteredCandidates = store.lexicalCandidates(
      'architecture',
      { spaceIds: ['recall-target'], tags: ['target'], minConfidence: 0.5 },
      10,
    );
    assert(
      filteredCandidates.length === candidates.length,
      'tag/confidence filters must not drop scoped candidates',
    );
    const impossibleCandidates = store.lexicalCandidates(
      'architecture',
      { spaceIds: ['recall-target'], minConfidence: 0.99 },
      10,
    );
    assert(
      impossibleCandidates.length === 0,
      'minConfidence filter must exclude low-confidence target',
    );

    const search = await searchEngine.search({
      query: 'architecture',
      spaceIds: ['recall-target'],
      topK: 5,
      mode: 'fast',
    });
    assert(
      search.results.some((result) => result.memory.id === weakRecord.id),
      'weak target missing from scoped search results',
    );

    const globalSearch = await searchEngine.search({
      query: 'architecture',
      topK: 5,
      mode: 'fast',
    });
    assert(
      globalSearch.results.length === 5,
      'global search should return bounded top-k across large match set',
    );

    process.stdout.write(
      `${JSON.stringify({
        ok: true,
        seededMatches: NOISE_COUNT,
        scopedCandidates: candidates.length,
        weakTargetFound: true,
        scopedSearchResults: search.results.length,
        globalSearchResults: globalSearch.results.length,
      })}\n`,
    );
  } finally {
    seedConnection.close();
    await models.stop().catch(() => {});
    store.close();
    rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await run();
