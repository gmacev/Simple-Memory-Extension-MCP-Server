#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { loadConfig } from '../dist/config.js';
import { Indexer } from '../dist/indexing/indexer.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const require = createRequire(import.meta.url);
const Database = require('better-sqlite3');

const SEED_COUNT = Number(process.env.BENCH_SEED_COUNT ?? 300);
const CREATE_COUNT = Number(process.env.BENCH_CREATE_COUNT ?? 150);
const REVISE_COUNT = Number(process.env.BENCH_REVISE_COUNT ?? 100);
const GET_COUNT = Number(process.env.BENCH_GET_COUNT ?? 400);
const SEARCH_COUNT = Number(process.env.BENCH_SEARCH_COUNT ?? 80);
const LIST_TRAVERSALS = Number(process.env.BENCH_LIST_TRAVERSALS ?? 3);
const FILTERED_SEARCH_COUNT = Number(process.env.BENCH_FILTERED_SEARCH_COUNT ?? 40);
const STARVATION_FILLER = Number(process.env.BENCH_STARVATION_FILLER ?? 4_000);
const SCOPED_SEARCH_COUNT = Number(process.env.BENCH_SCOPED_SEARCH_COUNT ?? 20);

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

function percentile(values, fraction) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
}

function stats(values) {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  return {
    count: values.length,
    meanMs: Math.round(mean * 100) / 100,
    p50Ms: Math.round(percentile(values, 0.5) * 100) / 100,
    p95Ms: Math.round(percentile(values, 0.95) * 100) / 100,
  };
}

// Seeds dense filler matches in dedicated noise spaces through a direct connection.
// Scoped searches against bench-a must filter past all of them before ranking.
function seedStarvationFiller(databasePath) {
  if (STARVATION_FILLER <= 0) return;
  const db = new Database(databasePath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  try {
    const stamp = new Date().toISOString();
    const insertMemory = db.prepare(
      `INSERT INTO memories (id, space_id, state, current_revision_id, created_at, updated_at, index_status, idempotency_key)
       VALUES (?, ?, 'active', ?, ?, ?, 'lexical-only', NULL)`,
    );
    const insertRevision = db.prepare(
      `INSERT INTO memory_revisions (id, memory_id, revision_number, parent_revision_id, title, kind,
                                     content_json, metadata_json, salience, confidence, observed_at,
                                     valid_from, valid_to, expires_at, review_after, recorded_at, actor,
                                     content_hash, searchable_text)
       VALUES (?, ?, 1, NULL, ?, 'note', '{}', '{}', 0.5, 0.5, NULL, NULL, NULL, NULL, NULL, ?, 'bench', ?, ?)`,
    );
    const insertSegment = db.prepare(
      `INSERT INTO memory_segments (id, memory_id, revision_id, space_id, ordinal, path, text, token_count, content_hash, model_profile_id)
       VALUES (?, ?, ?, ?, 0, '$', ?, 6, ?, NULL)`,
    );
    const insertFts = db.prepare(
      `INSERT INTO memory_fts (segment_id, memory_id, revision_id, space_id, title, text, tags)
       VALUES (?, ?, ?, ?, ?, ?, '')`,
    );
    db.transaction(() => {
      for (let index = 0; index < STARVATION_FILLER; index += 1) {
        const spaceId = index % 2 === 0 ? 'bench-noise-a' : 'bench-noise-b';
        const memoryId = randomUUID();
        const revisionId = randomUUID();
        const segmentId = randomUUID();
        const title = `deployment pipeline blueprint ${index}`;
        const text = `deployment pipeline cache rollout ${index}`;
        insertMemory.run(memoryId, spaceId, revisionId, stamp, stamp);
        insertRevision.run(revisionId, memoryId, title, stamp, randomUUID(), `${title}\n${text}`);
        insertSegment.run(segmentId, memoryId, revisionId, spaceId, text, randomUUID());
        insertFts.run(segmentId, memoryId, revisionId, spaceId, title, text);
      }
    })();
  } finally {
    db.close();
  }
}

const TOPIC_WORDS = [
  'deployment',
  'pipeline',
  'cache',
  'index',
  'scheduler',
  'embedding',
  'retrieval',
  'storage',
  'migration',
  'rollback',
  'throughput',
  'latency',
  'preference',
  'workflow',
  'release',
  'probe',
  'checkpoint',
  'vacuum',
];

function makeContent(random, index) {
  const words = Array.from(
    { length: 24 },
    () => TOPIC_WORDS[Math.floor(random() * TOPIC_WORDS.length)],
  );
  return {
    topic: words.slice(0, 4).join(' '),
    notes: [
      `Entry ${index} covers ${words.slice(4, 12).join(', ')}.`,
      `Follow-up guidance references ${words.slice(12, 20).join(', ')} and review checkpoints.`,
    ],
    flags: { priority: random() > 0.5 ? 'high' : 'normal', verified: random() > 0.3 },
    history: Array.from({ length: 6 }, (_, step) => ({
      step,
      note: `step ${step} ${words[step % words.length]}`,
    })),
  };
}

async function main() {
  const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-bench-'));
  process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
  process.env.SIMPLE_MEMORY_MODELS = 'disabled';
  process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';
  const config = loadConfig();
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const indexer = new Indexer(config, store, models, logger);
  const searchEngine = new SearchEngine(config, store, models, logger);

  const timings = {
    create: [],
    revise: [],
    get: [],
    list: [],
    search: [],
    searchFiltered: [],
    searchScoped: [],
    searchGlobalScale: [],
  };
  let rssPeakBytes = process.memoryUsage().rss;
  let listPagesTraversed = 0;

  const random = lcg(42);
  const spaceA = store.createSpace({ id: 'bench-a', name: 'Bench A' });
  store.createSpace({ id: 'bench-b', name: 'Bench B' });
  store.createSpace({ id: 'bench-noise-a', name: 'Bench Noise A' });
  store.createSpace({ id: 'bench-noise-b', name: 'Bench Noise B' });

  try {
    const seedRecords = [];
    for (let index = 0; index < SEED_COUNT; index += 1) {
      const spaceId = index % 3 === 0 ? 'bench-b' : 'bench-a';
      const created = await indexer.indexRevision(
        store.createMemory(
          {
            spaceId,
            logicalKey: index % 7 === 0 ? `key-${index}` : undefined,
            title: `${TOPIC_WORDS[index % TOPIC_WORDS.length]} memory ${index}`,
            kind: index % 2 === 0 ? 'fact' : 'preference',
            tags: [
              `topic-${TOPIC_WORDS[index % TOPIC_WORDS.length]}`,
              index % 5 === 0 ? 'verified' : 'draft',
            ],
            content: makeContent(random, index),
            metadata: { source: 'bench', order: index },
            salience: 0.4 + random() * 0.6,
            confidence: 0.4 + random() * 0.6,
            sources: [
              { type: 'workspace', uri: `file:///bench/${index}.ts`, label: `Bench ${index}` },
            ],
          },
          'bench',
        ).revision.id,
        false,
      );
      seedRecords.push(created);
    }

    const queryTargets = [];
    for (let index = 0; index < SEARCH_COUNT; index += 1) {
      const target = seedRecords[Math.floor(random() * seedRecords.length)];
      const word = TOPIC_WORDS[Math.floor(random() * TOPIC_WORDS.length)];
      queryTargets.push(random() > 0.5 ? `${word} memory` : target.revision.title);
    }

    // Warm-up (untimed): JIT, page cache, statement compilation.
    await searchEngine.search({ query: 'deployment pipeline', mode: 'auto', topK: 5 });
    store.listMemories({ spaceId: 'bench-a', limit: 10 });

    for (let index = 0; index < CREATE_COUNT; index += 1) {
      const started = performance.now();
      const created = store.createMemory({
        spaceId: 'bench-a',
        title: `created memory ${index}`,
        kind: 'note',
        tags: ['created'],
        content: makeContent(random, 10_000 + index),
        metadata: { source: 'bench-create', order: index },
        sources: [],
      });
      await indexer.indexRevision(created.revision.id, false, created);
      timings.create.push(performance.now() - started);
    }

    for (let index = 0; index < REVISE_COUNT; index += 1) {
      const target = seedRecords[index * 2];
      const started = performance.now();
      const revised = store.reviseMemory(
        target.id,
        {
          title: target.revision.title,
          kind: target.revision.kind,
          tags: target.revision.tags,
          content: makeContent(random, 20_000 + index),
          metadata: { source: 'bench-revise', order: index },
          sources: [],
        },
        target.revision.id,
      );
      await indexer.indexRevision(revised.revision.id, false, revised);
      timings.revise.push(performance.now() - started);
    }

    for (let index = 0; index < GET_COUNT; index += 1) {
      const target = seedRecords[Math.floor(random() * seedRecords.length)];
      const started = performance.now();
      store.getMemory(target.id);
      timings.get.push(performance.now() - started);
    }

    for (let traversal = 0; traversal < LIST_TRAVERSALS; traversal += 1) {
      let cursor = null;
      do {
        const started = performance.now();
        const page = store.listMemories({ limit: 100, ...(cursor ? { cursor } : {}) });
        timings.list.push(performance.now() - started);
        cursor = page.nextCursor;
        listPagesTraversed += 1;
      } while (cursor);
    }

    for (let index = 0; index < queryTargets.length; index += 1) {
      const started = performance.now();
      await searchEngine.search({ query: queryTargets[index], mode: 'auto', topK: 5 });
      timings.search.push(performance.now() - started);
      rssPeakBytes = Math.max(rssPeakBytes, process.memoryUsage().rss);
    }

    const filteredQueryTargets = queryTargets.slice(0, FILTERED_SEARCH_COUNT);
    for (const query of filteredQueryTargets) {
      const started = performance.now();
      await searchEngine.search({
        query,
        spaceIds: ['bench-a'],
        kinds: ['fact'],
        tags: ['verified'],
        minConfidence: 0.5,
        minSalience: 0.45,
        mode: 'fast',
        topK: 5,
      });
      timings.searchFiltered.push(performance.now() - started);
      rssPeakBytes = Math.max(rssPeakBytes, process.memoryUsage().rss);
    }

    seedStarvationFiller(config.databasePath);

    for (let index = 0; index < SCOPED_SEARCH_COUNT; index += 1) {
      const started = performance.now();
      await searchEngine.search({
        query: 'deployment pipeline',
        spaceIds: ['bench-a'],
        mode: 'fast',
        topK: 5,
      });
      timings.searchScoped.push(performance.now() - started);
      rssPeakBytes = Math.max(rssPeakBytes, process.memoryUsage().rss);
    }

    for (let index = 0; index < SCOPED_SEARCH_COUNT; index += 1) {
      const started = performance.now();
      await searchEngine.search({ query: 'deployment pipeline', mode: 'fast', topK: 5 });
      timings.searchGlobalScale.push(performance.now() - started);
      rssPeakBytes = Math.max(rssPeakBytes, process.memoryUsage().rss);
    }

    const outcome = {
      ok: true,
      seeds: SEED_COUNT,
      spaces: [spaceA.id],
      starvationFiller: STARVATION_FILLER,
      listPagesTraversed,
      rssPeakMb: Math.round((rssPeakBytes / (1024 * 1024)) * 10) / 10,
      timings: Object.fromEntries(
        Object.entries(timings).map(([key, values]) => [key, stats(values)]),
      ),
      totalSeconds:
        Math.round(
          (Object.values(timings).reduce(
            (sum, values) => sum + values.reduce((a, b) => a + b, 0),
            0,
          ) /
            1000) *
            100,
        ) / 100,
    };
    process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
  } finally {
    store.close();
    rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await main();
