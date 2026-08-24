#!/usr/bin/env node
import { Logger } from '../dist/logger.js';
import {
  InferenceCapacityError,
  InferenceExecutionTimeoutError,
  InferenceQueueTimeoutError,
} from '../dist/models/inference-errors.js';
import { InferenceScheduler } from '../dist/models/inference-scheduler.js';

function assert(condition, message) {
  if (!condition) throw new Error(`Inference scheduler probe failed: ${message}`);
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function packedVectors(vectors) {
  const rows = vectors.length;
  const dimensions = vectors[0]?.length ?? 0;
  assert(
    vectors.every((vector) => vector.length === dimensions),
    'fake embedding vectors must have equal dimensions',
  );
  const values = new Float32Array(vectors.flat());
  return {
    encoding: 'base64-f32le',
    rows,
    dimensions,
    data: Buffer.from(values.buffer).toString('base64'),
  };
}

function config(overrides = {}) {
  return {
    embeddingBatchSize: 8,
    rerankBatchSize: 4,
    inferenceQueueLimit: 128,
    inferenceQueueTimeoutMs: 30_000,
    modelTimeoutMs: 30_000,
    ...overrides,
  };
}

class FakeTransport {
  launcherPid = null;
  workerPid = null;
  processStarts = 1;
  calls = [];
  aborts = 0;
  stopped = false;
  holdNext = false;
  failNext = null;
  held = null;

  dispatch(operation, payload = {}) {
    this.calls.push({ operation, payload });
    if (this.failNext) {
      const error = this.failNext;
      this.failNext = null;
      return Promise.reject(error);
    }
    if (this.holdNext) {
      this.holdNext = false;
      return new Promise((resolve, reject) => {
        this.held = { operation, payload, resolve, reject };
      });
    }
    return Promise.resolve(this.response(operation, payload));
  }

  response(operation, payload) {
    if (operation === 'embed_queries' || operation === 'embed_documents') {
      return {
        vectors: packedVectors(
          payload.texts.map((text) => [text.length, text.codePointAt(0) ?? 0]),
        ),
      };
    }
    if (operation === 'count_tokens') return { counts: payload.texts.map((text) => text.length) };
    if (operation === 'rerank_pairs') {
      return {
        scores: payload.pairs.map(({ document }) => Number.parseInt(document.slice(1), 10)),
      };
    }
    return { operation };
  }

  release() {
    const held = this.held;
    if (!held) throw new Error('No fake worker call is held');
    this.held = null;
    held.resolve(this.response(held.operation, held.payload));
  }

  abort(error) {
    this.aborts += 1;
    const held = this.held;
    this.held = null;
    held?.reject(error);
  }

  async stop() {
    this.stopped = true;
    this.abort(new Error('fake worker stopped'));
  }
}

const logger = new Logger('error');

async function batchingAndFairness() {
  const transport = new FakeTransport();
  const scheduler = new InferenceScheduler(
    config({ embeddingBatchSize: 3, rerankBatchSize: 2 }),
    transport,
    logger,
  );

  const queryResults = await Promise.all([
    scheduler.embedQuery('alpha'),
    scheduler.embedQuery('beta'),
  ]);
  assert(
    transport.calls[0]?.operation === 'embed_queries',
    'query embeddings should use embed_queries',
  );
  assert(transport.calls[0]?.payload.texts.length === 2, 'compatible queries should microbatch');
  assert(
    queryResults.every((vector) => vector instanceof Float32Array) &&
      queryResults[0]?.[0] === 5 &&
      queryResults[1]?.[0] === 4,
    'query vectors should map to callers',
  );

  const [firstDocuments, secondDocuments] = await Promise.all([
    scheduler.embedDocuments(['aa', 'bbb']),
    scheduler.embedDocuments(['cccc']),
  ]);
  const documentCall = transport.calls.find((call) => call.operation === 'embed_documents');
  assert(
    documentCall?.payload.texts.length === 3,
    'document embeddings should coalesce across writes',
  );
  assert(
    firstDocuments.every((vector) => vector instanceof Float32Array) &&
      firstDocuments[0]?.[0] === 2 &&
      firstDocuments[1]?.[0] === 3,
    'first document mapping should be stable',
  );
  assert(secondDocuments[0]?.[0] === 4, 'second document mapping should be stable');

  const counts = await Promise.all([
    scheduler.countTokens(['a', 'bbbb']),
    scheduler.countTokens(['ccc']),
  ]);
  assert(
    counts[0]?.join(',') === '1,4' && counts[1]?.[0] === 3,
    'token counts should preserve request order',
  );

  transport.holdNext = true;
  const rerank = scheduler.rerank('query', ['d0', 'd1', 'd2', 'd3', 'd4']);
  await delay(1);
  const query = scheduler.embedQuery('interleave');
  transport.release();
  const [scores, vector] = await Promise.all([rerank, query]);
  assert(scores.join(',') === '0,1,2,3,4', 'cooperative reranking should preserve score positions');
  assert(vector[0] === 10, 'interleaved query should complete');
  const tail = transport.calls.slice(-4).map((call) => call.operation);
  assert(
    tail.indexOf('embed_queries') < tail.lastIndexOf('rerank_pairs'),
    'query work should interleave between rerank slices',
  );

  const snapshot = scheduler.snapshot();
  assert(
    snapshot.counters.completed === 8,
    'logical completion counter should count callers, not slices',
  );
  assert(
    snapshot.counters.dispatchedBatches < snapshot.counters.completed + 5,
    'batch counter should count worker turns',
  );
  assert(
    snapshot.lanes.query.completed === 3 && snapshot.lanes.rerank.completed === 1,
    'per-lane completion metrics should be accurate',
  );
  await scheduler.stop();
}

async function embeddingResponseValidation() {
  const malformed = [
    {
      encoding: 'base64-f32le',
      rows: 1,
      dimensions: 2,
      data: '%%%%',
    },
    {
      encoding: 'base64-f32le',
      rows: 2,
      dimensions: 2,
      data: Buffer.from(new Float32Array([1, 2, 3]).buffer).toString('base64'),
    },
    packedVectors([[Number.NaN, 1]]),
    {
      encoding: 'base64-f32le',
      rows: 0,
      dimensions: 2,
      data: '',
    },
  ];
  for (const vectors of malformed) {
    const transport = new FakeTransport();
    transport.response = () => ({ vectors });
    const scheduler = new InferenceScheduler(config(), transport, logger);
    await assertRejects(
      scheduler.embedQuery('invalid response'),
      Error,
      'malformed packed embeddings must fail validation',
    );
    await scheduler.stop();
  }
}

async function capacityAndQueueTimeout() {
  const capacityTransport = new FakeTransport();
  capacityTransport.holdNext = true;
  const capacityScheduler = new InferenceScheduler(
    config({ inferenceQueueLimit: 2 }),
    capacityTransport,
    logger,
  );
  const first = capacityScheduler.raw('first');
  const second = capacityScheduler.raw('second');
  await assertRejects(
    capacityScheduler.raw('third'),
    InferenceCapacityError,
    'queue capacity should reject immediately',
  );
  capacityTransport.release();
  await Promise.all([first, second]);
  assert(
    capacityScheduler.snapshot().counters.rejected === 1,
    'capacity rejection should be counted',
  );
  await capacityScheduler.stop();

  const timeoutTransport = new FakeTransport();
  timeoutTransport.holdNext = true;
  const timeoutScheduler = new InferenceScheduler(
    config({ inferenceQueueTimeoutMs: 15 }),
    timeoutTransport,
    logger,
  );
  const blocking = timeoutScheduler.raw('blocking');
  const queued = timeoutScheduler.embedQuery('expires');
  await assertRejects(
    queued,
    InferenceQueueTimeoutError,
    'undispatched work should time out in queue',
  );
  timeoutTransport.release();
  await blocking;
  assert(
    timeoutScheduler.snapshot().counters.queueTimeouts === 1,
    'queue timeout should be counted',
  );
  await timeoutScheduler.stop();
}

async function weightedLaneOrder() {
  const transport = new FakeTransport();
  transport.holdNext = true;
  const scheduler = new InferenceScheduler(
    config({ embeddingBatchSize: 1, rerankBatchSize: 1 }),
    transport,
    logger,
  );
  const blocker = scheduler.raw('blocker');
  const work = [
    ...Array.from({ length: 6 }, (_, index) => scheduler.embedQuery(`q${index}`)),
    ...Array.from({ length: 4 }, (_, index) => scheduler.embedDocuments([`w${index}`])),
    ...Array.from({ length: 3 }, (_, index) => scheduler.rerank('q', [`d${index}`])),
  ];
  transport.release();
  await Promise.all([blocker, ...work]);
  const laneOperations = transport.calls.slice(1, 8).map((call) => call.operation);
  assert(
    laneOperations.join(',') ===
      'embed_queries,embed_queries,embed_queries,embed_documents,embed_documents,rerank_pairs,embed_queries',
    `weighted lanes should dispatch 4:2:1 without starving reranks; got ${laneOperations.join(',')}`,
  );
  await scheduler.stop();
}

async function executionRecoveryAndShutdown() {
  const transport = new FakeTransport();
  transport.holdNext = true;
  const scheduler = new InferenceScheduler(config({ modelTimeoutMs: 20 }), transport, logger);
  const stuck = scheduler.raw('stuck');
  const retained = scheduler.embedQuery('retained');
  await assertRejects(
    stuck,
    InferenceExecutionTimeoutError,
    'in-flight timeout should fail the affected request',
  );
  const vector = await retained;
  assert(vector[0] === 8, 'undispatched work should recover after worker termination');
  assert(transport.aborts === 1, 'execution timeout should terminate the worker once');
  assert(
    scheduler.snapshot().counters.executionTimeouts === 1,
    'execution timeout should be counted',
  );

  transport.failNext = new Error('simulated worker crash');
  await assertRejects(
    scheduler.raw('crash'),
    Error,
    'worker failure should reject its logical request',
  );
  const recovered = await scheduler.raw('recovered');
  assert(
    recovered.operation === 'recovered',
    'later queued work should continue after a worker failure',
  );

  transport.holdNext = true;
  const pending = scheduler.raw('shutdown-pending');
  await delay(1);
  await scheduler.stop();
  await assertRejects(pending, Error, 'shutdown should reject in-flight work cleanly');
  assert(transport.stopped, 'scheduler shutdown should stop the transport');
}

async function assertRejects(promise, ErrorType, message) {
  try {
    await promise;
  } catch (error) {
    assert(error instanceof ErrorType, `${message}; received ${String(error)}`);
    return;
  }
  throw new Error(`Inference scheduler probe failed: ${message}; promise resolved`);
}

await batchingAndFairness();
await embeddingResponseValidation();
await capacityAndQueueTimeout();
await weightedLaneOrder();
await executionRecoveryAndShutdown();
process.stdout.write('Inference scheduler probe passed.\n');
