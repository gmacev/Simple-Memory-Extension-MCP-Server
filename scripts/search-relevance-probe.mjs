#!/usr/bin/env node
// Model-independent checks for exact lookup and conservative per-result pruning.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { Indexer } from '../dist/indexing/indexer.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-search-relevance-'));
if (!dataDir.startsWith(`${path.resolve(tmpdir())}${path.sep}`)) {
  throw new Error('Refusing to use an unexpected probe directory');
}
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Search relevance probe assertion failed: ${message}`);
}

async function main() {
  const config = loadConfig();
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const indexer = new Indexer(config, store, models, logger);
  const search = new SearchEngine(config, store, models, logger);
  try {
    store.createSpace({ id: 'relevance-probe', name: 'Relevance Probe' });
    const ids = new Map();
    for (const [title, logicalKey] of [
      ['Target status', 'project/exact-status'],
      ['Related status', undefined],
      ['Weak status', undefined],
      ['Other status', undefined],
    ]) {
      const created = store.createMemory(
        {
          spaceId: 'relevance-probe',
          ...(logicalKey ? { logicalKey } : {}),
          title,
          kind: 'note',
          tags: ['status'],
          content: { detail: `${title} describes the status.` },
          metadata: {},
          salience: 0.8,
          confidence: 0.8,
          sources: [],
        },
        'probe',
      );
      ids.set(title, created.id);
      await indexer.indexRevision(created.revision.id, false);
    }

    let embedCalls = 0;
    models.embedQuery = async () => {
      embedCalls += 1;
      throw new Error('exact lookup must not embed');
    };
    config.modelsEnabled = true;
    const exact = await search.search({
      query: 'project/exact-status',
      spaceIds: ['relevance-probe'],
      topK: 5,
      mode: 'auto',
    });
    assert(exact.results.length === 1, 'exact logical key must return only its memory');
    assert(exact.results[0]?.memory.id === ids.get('Target status'), 'wrong exact result');
    assert(embedCalls === 0, 'exact lookup started embedding');
    assert(exact.stageTimings?.embedMs === undefined, 'exact lookup reported embedding time');

    config.modelsEnabled = false;
    models.rerank = async (_query, documents) =>
      documents.map((document) => {
        if (document.includes('Title: Target status')) return 1;
        if (document.includes('Title: Related status')) return 0.8;
        if (document.includes('Title: Weak status')) return 0.01;
        return 0.001;
      });
    const ranked = await search.search({
      query: 'status',
      spaceIds: ['relevance-probe'],
      topK: 5,
      mode: 'quality',
    });
    const returned = new Set(ranked.results.map((result) => result.memory.id));
    assert(returned.has(ids.get('Target status')), 'strong winner was pruned');
    assert(returned.has(ids.get('Related status')), 'plausible alternative was pruned');
    assert(!returned.has(ids.get('Weak status')), 'weak alternative survived');
    assert(!returned.has(ids.get('Other status')), 'very weak alternative survived');

    models.rerank = async (_query, documents) => documents.map(() => 0.4);
    const uncertain = await search.search({
      query: 'status',
      spaceIds: ['relevance-probe'],
      topK: 5,
      mode: 'quality',
    });
    assert(uncertain.results.length === 4, 'uncertain reranking must preserve recall');

    for (const detail of ['first', 'second']) {
      const duplicate = store.createMemory(
        {
          spaceId: 'relevance-probe',
          title: 'Shared status',
          kind: 'note',
          tags: [],
          content: { detail },
          metadata: {},
          sources: [],
        },
        'probe',
      );
      await indexer.indexRevision(duplicate.revision.id, false);
    }
    const exactTitle = await search.search({
      query: 'Shared status',
      spaceIds: ['relevance-probe'],
      topK: 5,
      mode: 'auto',
    });
    assert(exactTitle.results.length === 2, 'all exact-title duplicates must be retained');
    assert(
      exactTitle.results.every((result) => result.memory.revision.title === 'Shared status'),
      'exact-title lookup included loose matches',
    );

    process.stdout.write(
      `${JSON.stringify({ ok: true, exactResults: exact.results.length, exactTitleResults: exactTitle.results.length, prunedResults: ranked.results.length, uncertainResults: uncertain.results.length })}\n`,
    );
  } finally {
    await models.stop().catch(() => {});
    store.close();
    rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await main();
