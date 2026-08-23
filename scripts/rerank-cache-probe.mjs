#!/usr/bin/env node
// Deterministic, model-independent rerank-cache probe.
// Mocks worker health and scheduler reranking and exercises ModelClient.rerank()
// directly: cache hits, identity-keyed misses, framing-ambiguity resistance,
// failure eviction/propagation, profile-failure bypass, and LRU eviction.
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-rerank-cache-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Rerank cache probe assertion failed: ${message}`);
}

async function main() {
  const config = loadConfig();
  const models = new ModelClient(config, new Logger('error'));

  let healthCalls = 0;
  let failHealthNext = false;
  const baseProfile = {
    reranker_model: 'mock-reranker',
    reranker_revision: 'rev-1',
    rerank_instruction_hash: 'ih-1',
  };
  const baseHealth = async () => {
    healthCalls += 1;
    if (failHealthNext) {
      failHealthNext = false;
      throw new Error('synthetic health failure');
    }
    return { ...baseProfile };
  };
  models.health = baseHealth;

  let schedulerCalls = 0;
  let failNextRerank = false;
  models.scheduler.rerank = async (query, documents) => {
    schedulerCalls += 1;
    if (failNextRerank) {
      failNextRerank = false;
      throw new Error('synthetic rerank failure');
    }
    return documents.map(
      (document, index) => (((query.length * 31 + document.length * 17 + index) % 97) + 1) / 98,
    );
  };
  models.stop = async () => {};

  try {
    // Cache hit: identical request resolves without touching the scheduler.
    const first = await models.rerank('query one', ['doc alpha', 'doc beta']);
    assert(
      schedulerCalls === 1 && healthCalls === 1,
      'first call should invoke scheduler + health once',
    );
    const cached = await models.rerank('query one', ['doc alpha', 'doc beta']);
    assert(schedulerCalls === 1, 'identical request must be served from cache');
    assert(cached === first, 'cached entry must resolve to the same promise instance');

    // Distinct query with same docs misses; distinct docs with same query misses.
    await models.rerank('query two', ['doc alpha', 'doc beta']);
    assert(schedulerCalls === 2, 'different query must miss');
    await models.rerank('query one', ['doc gamma']);
    assert(schedulerCalls === 3, 'different documents must miss');

    // Reranker identity is part of the key; returning to a prior identity hits its own entry.
    models.rerankProfilePromise = null;
    models.health = async () => ({ ...baseProfile, reranker_revision: 'rev-2' });
    await models.rerank('query one', ['doc alpha', 'doc beta']);
    assert(schedulerCalls === 4, 'reranker revision change must miss');
    models.rerankProfilePromise = null;
    models.health = baseHealth;
    await models.rerank('query one', ['doc alpha', 'doc beta']);
    assert(schedulerCalls === 4, 'prior-profile entry must still be served from cache');

    // Framing resistance: NUL-bearing components must not alias across tuples.
    const tupleA = ['a', ['b\u0000c']];
    const tupleB = ['a\u0000b', ['c']];
    await models.rerank(...tupleA);
    const afterTupleA = schedulerCalls;
    await models.rerank(...tupleB);
    assert(schedulerCalls === afterTupleA + 1, 'NUL-framed tuples must not collide');
    await models.rerank(...tupleA);
    assert(schedulerCalls === afterTupleA + 1, 'tuple A must still hit its own entry');

    // Profile-resolution failure bypasses caching but still serves the request.
    models.rerankProfilePromise = null;
    failHealthNext = true;
    const bypassed = await models.rerank('bypass query', ['doc x']);
    assert(
      schedulerCalls === afterTupleA + 2,
      'profile failure must fall through to scheduler once',
    );
    assert(bypassed.length === 1, 'fallback rerank must return scores');
    await models.rerank('bypass query', ['doc x']);
    assert(schedulerCalls === afterTupleA + 3, 'fallback result must not have been cached');

    // Failed cached rerank: evicted and propagated, no silent immediate retry.
    failNextRerank = true;
    await models.rerank('doomed query', ['doc y']).then(
      () => {
        throw new Error('expected rerank rejection');
      },
      (error) => assert(String(error).includes('synthetic'), 'failure must propagate'),
    );
    const afterFailure = schedulerCalls;
    await models.rerank('doomed query', ['doc y']);
    assert(schedulerCalls === afterFailure + 1, 'failed entry must be evicted for a later retry');
    await models.rerank('doomed query', ['doc y']);
    assert(schedulerCalls === afterFailure + 1, 'successful retry must be cached');

    // LRU bound: oldest entries are evicted beyond the cap.
    for (let index = 0; index < 140; index += 1) {
      await models.rerank(`lru filler ${index}`, ['doc z']);
    }
    const beforeEvictionCheck = schedulerCalls;
    await models.rerank('query one', ['doc alpha', 'doc beta']);
    assert(
      schedulerCalls === beforeEvictionCheck + 1,
      'oldest entry must have been evicted by LRU bound',
    );
    await models.rerank('lru filler 139', ['doc z']);
    assert(schedulerCalls === beforeEvictionCheck + 1, 'recent entry must remain cached');

    process.stdout.write(
      `${JSON.stringify({ ok: true, schedulerCalls, healthCalls, modelIndependent: true })}\n`,
    );
  } finally {
    await models.stop();
    rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await main();
