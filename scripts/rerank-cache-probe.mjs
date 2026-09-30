#!/usr/bin/env node
// Model-independent regression coverage for exact query/passage score reuse.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';

function score(query, document) {
  return (
    createHash('sha256')
      .update(JSON.stringify([query, document]))
      .digest()
      .readUInt32LE() /
    2 ** 32
  );
}

function fixture() {
  const models = new ModelClient({ ...loadConfig(), modelsEnabled: false }, new Logger('error'));
  const calls = [];
  const profile = {
    reranker_model: 'mock-reranker',
    reranker_revision: 'rev-1',
    rerank_instruction_hash: 'instruction-1',
  };
  models.health = async () => ({ ...profile });
  models.scheduler.rerank = async (query, documents) => {
    calls.push({ query, documents });
    return documents.map((document) => score(query, document));
  };
  return { models, calls, profile };
}

async function reuseAndIdentity() {
  const { models, calls, profile } = fixture();
  try {
    assert.deepEqual(await models.rerank('empty', []), []);
    assert.equal(calls.length, 0);
    const first = await models.rerank('query', ['alpha', 'beta', 'alpha']);
    assert.deepEqual(calls[0].documents, ['alpha', 'beta'], 'duplicate passages must score once');
    assert.deepEqual(first, [
      score('query', 'alpha'),
      score('query', 'beta'),
      score('query', 'alpha'),
    ]);
    first[0] = -1;
    assert.deepEqual(await models.rerank('query', ['beta', 'alpha']), [
      score('query', 'beta'),
      score('query', 'alpha'),
    ]);
    assert.equal(
      calls.length,
      1,
      'reordering/subsets and caller mutation must preserve cached scores',
    );
    await models.rerank('query', ['alpha', 'gamma', 'beta']);
    assert.deepEqual(calls[1].documents, ['gamma'], 'only the added passage should be scored');
    await models.rerank('different query', ['alpha']);
    assert.equal(calls.length, 3, 'different query must miss');
    await models.rerank('query', ['Alpha']);
    assert.equal(calls.length, 4, 'exact scoring text, including case, must be keyed');
    for (const field of ['reranker_model', 'reranker_revision', 'rerank_instruction_hash']) {
      const previous = profile[field];
      profile[field] = `${previous}-changed`;
      models.rerankProfilePromise = null;
      const before = calls.length;
      await models.rerank('query', ['alpha']);
      assert.equal(calls.length, before + 1, `${field} change must miss`);
      profile[field] = previous;
      models.rerankProfilePromise = null;
      await models.rerank('query', ['alpha']);
      assert.equal(calls.length, before + 1, 'restored model identity may reuse its own scores');
    }
    await models.rerank('a', ['b\u0000c']);
    const before = calls.length;
    await models.rerank('a\u0000b', ['c']);
    assert.equal(calls.length, before + 1, 'framed query/document tuples must not collide');
  } finally {
    await models.stop();
  }
}

async function pendingAndFailures() {
  const { models } = fixture();
  const calls = [];
  const releases = [];
  models.scheduler.rerank = (query, documents) => {
    calls.push(documents);
    return new Promise((resolve, reject) => releases.push({ query, documents, resolve, reject }));
  };
  try {
    const first = models.rerank('query', ['a', 'b']);
    const second = models.rerank('query', ['b', 'c']);
    await new Promise(setImmediate);
    assert.deepEqual(calls, [['a', 'b'], ['c']], 'overlapping in-flight requests must share pairs');
    for (const release of releases.splice(0)) {
      release.resolve(release.documents.map((document) => score(release.query, document)));
    }
    assert.deepEqual(await first, [score('query', 'a'), score('query', 'b')]);
    assert.deepEqual(await second, [score('query', 'b'), score('query', 'c')]);
    const failed = models.rerank('query', ['a', 'd']);
    const overlappingFailure = models.rerank('query', ['d']);
    const settled = Promise.allSettled([failed, overlappingFailure]);
    await new Promise(setImmediate);
    assert.deepEqual(calls.at(-1), ['d']);
    releases.shift().reject(new Error('synthetic failure'));
    assert((await settled).every((result) => result.status === 'rejected'));
    const retry = models.rerank('query', ['d', 'a']);
    await new Promise(setImmediate);
    assert.deepEqual(calls.at(-1), ['d'], 'failure must evict only failed pairs');
    releases.shift().resolve([score('query', 'd')]);
    assert.deepEqual(await retry, [score('query', 'd'), score('query', 'a')]);
  } finally {
    await models.stop();
  }
}

async function bypassAndEviction() {
  const { models, calls } = fixture();
  try {
    const health = models.health;
    models.health = async () => {
      throw new Error('synthetic health failure');
    };
    await models.rerank('bypass', ['a']);
    models.health = health;
    await models.rerank('bypass', ['a']);
    assert.equal(calls.length, 2, 'unknown profile must bypass caching');
    models.rerankCache.clear();
    await models.rerank('lru', ['old', 'recent']);
    await models.rerank('lru', ['recent']);
    const filler = Array.from({ length: 4_095 }, (_, index) => `filler-${index}`);
    const values = await models.rerank('lru', filler);
    assert.equal(values.length, filler.length, 'eviction must not lose current caller mappings');
    const before = calls.length;
    await models.rerank('lru', ['recent']);
    assert.equal(calls.length, before, 'recent pair must remain cached');
    await models.rerank('lru', ['old']);
    assert.equal(calls.length, before + 1, 'oldest pair must be evicted at the bounded capacity');
  } finally {
    await models.stop();
  }
}

await reuseAndIdentity();
await pendingAndFailures();
await bypassAndEviction();
process.stdout.write('Rerank pair cache probe passed.\n');
