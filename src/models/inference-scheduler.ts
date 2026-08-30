import { endianness } from 'node:os';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { compileSchema } from '../validation.js';
import {
  InferenceCapacityError,
  InferenceExecutionTimeoutError,
  InferenceQueueTimeoutError,
  ModelWorkerFailureError,
} from './inference-errors.js';
import type { InferenceWorkerTransport } from './worker-transport.js';

const canonicalBase64Schema = z
  .string()
  .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/);
const packedFloat32MatrixSchema = z.object({
  encoding: z.literal('base64-f32le'),
  rows: z.number().int().nonnegative(),
  dimensions: z.number().int().nonnegative(),
  data: canonicalBase64Schema,
});
const embeddingResponseSchema = compileSchema(z.object({ vectors: packedFloat32MatrixSchema }));
const countResponseSchema = compileSchema(
  z.object({ counts: z.array(z.number().int().nonnegative()) }),
);
const rerankResponseSchema = compileSchema(z.object({ scores: z.array(z.number()) }));

const TOKEN_COUNT_BATCH_SIZE = 64;
const RERANK_SLICE_CHARACTER_BUDGET = 32_000;

type Lane = 'query' | 'ingestion' | 'rerank';
type RequestKind = 'raw' | 'query' | 'documents' | 'count' | 'rerank';

export type EmbeddingVector = Float32Array;

interface RequestBase<T> {
  id: number;
  kind: RequestKind;
  lane: Lane;
  enqueuedAt: number;
  startedAt: number | null;
  queueTimer: NodeJS.Timeout | null;
  executionTimer: NodeJS.Timeout | null;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

interface RawRequest extends RequestBase<unknown> {
  kind: 'raw';
  operation: string;
  payload: Record<string, unknown>;
}

interface QueryRequest extends RequestBase<EmbeddingVector> {
  kind: 'query';
  text: string;
}

interface DocumentsRequest extends RequestBase<EmbeddingVector[]> {
  kind: 'documents';
  texts: string[];
  cursor: number;
  vectors: Array<EmbeddingVector | undefined>;
}

interface CountRequest extends RequestBase<number[]> {
  kind: 'count';
  texts: string[];
  cursor: number;
  counts: Array<number | undefined>;
}

interface RerankRequest extends RequestBase<number[]> {
  kind: 'rerank';
  query: string;
  documents: string[];
  order: number[];
  cursor: number;
  scores: Array<number | undefined>;
}

type ScheduledRequest = RawRequest | QueryRequest | DocumentsRequest | CountRequest | RerankRequest;

interface Batch {
  lane: Lane;
  requests: Set<ScheduledRequest>;
  operation: string;
  payload: Record<string, unknown>;
  apply: (result: unknown) => void;
}

interface LaneMetric {
  completed: number;
  queueMs: number;
  executionMs: number;
}

export interface InferenceSchedulerSnapshot {
  queueDepth: number;
  queueLimit: number;
  inFlight: boolean;
  counters: {
    completed: number;
    rejected: number;
    queueTimeouts: number;
    executionTimeouts: number;
    dispatchedBatches: number;
  };
  lanes: Record<
    Lane,
    { queued: number; completed: number; meanQueueMs: number; meanExecutionMs: number }
  >;
}

const laneCycle: Lane[] = ['query', 'query', 'query', 'query', 'ingestion', 'ingestion', 'rerank'];

function requestPromise<T>(
  create: (resolve: (value: T) => void, reject: (error: Error) => void) => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => create(resolve, reject));
}

function completedArray<T>(items: Array<T | undefined>, label: string): T[] {
  if (items.some((item) => item === undefined)) {
    throw new ModelWorkerFailureError(`Incomplete ${label} response from model worker`);
  }
  return items as T[];
}

function decodeFloat32Values(data: string, valueCount: number, label: string): Float32Array {
  if (!Number.isSafeInteger(valueCount)) {
    throw new ModelWorkerFailureError(`${label} shape exceeds the supported size`);
  }
  const bytes = Buffer.from(data, 'base64');
  if (bytes.toString('base64') !== data) {
    throw new ModelWorkerFailureError(`${label} contains invalid base64 data`);
  }
  const expectedBytes = valueCount * Float32Array.BYTES_PER_ELEMENT;
  if (bytes.byteLength !== expectedBytes) {
    throw new ModelWorkerFailureError(
      `${label} byte length mismatch: expected ${String(expectedBytes)}, received ${String(bytes.byteLength)}`,
    );
  }
  const values = new Float32Array(valueCount);
  if (endianness() === 'LE') {
    new Uint8Array(values.buffer).set(bytes);
  } else {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (let index = 0; index < valueCount; index += 1) {
      values[index] = view.getFloat32(index * Float32Array.BYTES_PER_ELEMENT, true);
    }
  }
  for (const value of values) {
    if (!Number.isFinite(value)) {
      throw new ModelWorkerFailureError(`${label} contains a non-finite value`);
    }
  }
  return values;
}

function decodeEmbeddingMatrix(raw: unknown): EmbeddingVector[] {
  const packed = embeddingResponseSchema.parse(raw).vectors;
  if ((packed.rows === 0) !== (packed.dimensions === 0)) {
    throw new ModelWorkerFailureError('Embedding matrix has an invalid empty shape');
  }
  const valueCount = packed.rows * packed.dimensions;
  const values = decodeFloat32Values(packed.data, valueCount, 'Embedding matrix');
  return Array.from({ length: packed.rows }, (_, row) => {
    const start = row * packed.dimensions;
    return values.subarray(start, start + packed.dimensions);
  });
}

export class InferenceScheduler {
  private readonly queues: Record<Lane, ScheduledRequest[]> = {
    query: [],
    ingestion: [],
    rerank: [],
  };
  private readonly outstanding = new Map<number, ScheduledRequest>();
  private readonly metrics: Record<Lane, LaneMetric> = {
    query: { completed: 0, queueMs: 0, executionMs: 0 },
    ingestion: { completed: 0, queueMs: 0, executionMs: 0 },
    rerank: { completed: 0, queueMs: 0, executionMs: 0 },
  };
  private readonly counters = {
    completed: 0,
    rejected: 0,
    queueTimeouts: 0,
    executionTimeouts: 0,
    dispatchedBatches: 0,
  };
  private requestCounter = 0;
  private laneCursor = 0;
  private dispatching = false;
  private currentBatchRequests = new Set<ScheduledRequest>();
  private microBatchTimer: NodeJS.Immediate | null = null;
  private microBatchLane: Lane | null = null;
  private stopped = false;

  public constructor(
    private readonly config: AppConfig,
    private readonly transport: InferenceWorkerTransport,
    private readonly logger: Logger,
  ) {}

  private base<T>(
    kind: RequestKind,
    lane: Lane,
    resolve: (value: T) => void,
    reject: (error: Error) => void,
  ): RequestBase<T> {
    return {
      id: ++this.requestCounter,
      kind,
      lane,
      enqueuedAt: Date.now(),
      startedAt: null,
      queueTimer: null,
      executionTimer: null,
      resolve,
      reject,
    };
  }

  private enqueue(request: ScheduledRequest): void {
    if (this.stopped) {
      request.reject(new ModelWorkerFailureError('Inference scheduler is stopped'));
      return;
    }
    if (this.outstanding.size >= this.config.inferenceQueueLimit) {
      this.counters.rejected += 1;
      request.reject(
        new InferenceCapacityError(
          `Inference queue is full (${String(this.config.inferenceQueueLimit)} requests)`,
        ),
      );
      return;
    }
    this.outstanding.set(request.id, request);
    this.queues[request.lane].push(request);
    request.queueTimer = setTimeout(
      () => this.expireQueued(request),
      this.config.inferenceQueueTimeoutMs,
    );
    this.pump();
  }

  private expireQueued(request: ScheduledRequest): void {
    if (!this.outstanding.has(request.id) || request.startedAt !== null) return;
    this.counters.queueTimeouts += 1;
    this.failRequest(
      request,
      new InferenceQueueTimeoutError(
        `Inference request ${request.kind} exceeded ${String(this.config.inferenceQueueTimeoutMs)} ms in queue`,
      ),
    );
  }

  private expireExecuting(request: ScheduledRequest): void {
    if (!this.outstanding.has(request.id) || request.startedAt === null) return;
    const error = new InferenceExecutionTimeoutError(
      `Inference request ${request.kind} exceeded ${String(this.config.modelTimeoutMs)} ms during execution`,
    );
    this.counters.executionTimeouts += 1;
    const inCurrentBatch = this.currentBatchRequests.has(request);
    this.failRequest(request, error);
    if (inCurrentBatch) this.transport.abort(error);
  }

  private removeFromQueues(request: ScheduledRequest): void {
    const queue = this.queues[request.lane];
    for (let index = queue.length - 1; index >= 0; index -= 1) {
      if (queue[index] === request) queue.splice(index, 1);
    }
  }

  private clearTimers(request: {
    queueTimer: NodeJS.Timeout | null;
    executionTimer: NodeJS.Timeout | null;
  }): void {
    if (request.queueTimer) clearTimeout(request.queueTimer);
    if (request.executionTimer) clearTimeout(request.executionTimer);
    request.queueTimer = null;
    request.executionTimer = null;
  }

  private failRequest(request: ScheduledRequest, error: Error): void {
    if (!this.outstanding.delete(request.id)) return;
    this.removeFromQueues(request);
    this.clearTimers(request);
    request.reject(error);
  }

  private startRequest(request: ScheduledRequest): void {
    if (request.startedAt !== null) return;
    request.startedAt = Date.now();
    if (request.queueTimer) clearTimeout(request.queueTimer);
    request.queueTimer = null;
    request.executionTimer = setTimeout(
      () => this.expireExecuting(request),
      this.config.modelTimeoutMs,
    );
  }

  private completeRequest<T>(request: RequestBase<T>, value: T): void {
    if (!this.outstanding.delete(request.id)) return;
    this.clearTimers(request);
    const completedAt = Date.now();
    const startedAt = request.startedAt ?? completedAt;
    const metric = this.metrics[request.lane];
    metric.completed += 1;
    metric.queueMs += Math.max(0, startedAt - request.enqueuedAt);
    metric.executionMs += Math.max(0, completedAt - startedAt);
    this.counters.completed += 1;
    request.resolve(value);
  }

  private requeue(request: ScheduledRequest): void {
    if (this.outstanding.has(request.id)) this.queues[request.lane].push(request);
  }

  private selectLane(): Lane | null {
    for (let attempt = 0; attempt < laneCycle.length; attempt += 1) {
      const lane = laneCycle[this.laneCursor];
      this.laneCursor = (this.laneCursor + 1) % laneCycle.length;
      if (lane && this.queues[lane].length > 0) return lane;
    }
    return null;
  }

  private shouldMicroBatch(lane: Lane): boolean {
    const first = this.queues[lane][0];
    if (!first || (first.kind !== 'query' && first.kind !== 'documents')) return false;
    if (first.kind === 'query') {
      let compatible = 0;
      for (const request of this.queues.query) {
        if (request.kind !== 'query') break;
        compatible += 1;
      }
      return compatible < this.config.embeddingBatchSize;
    }
    let texts = 0;
    for (const request of this.queues.ingestion) {
      if (request.kind !== 'documents') break;
      texts += request.texts.length - request.cursor;
      if (texts >= this.config.embeddingBatchSize) return false;
    }
    return true;
  }

  private scheduleMicroBatch(lane: Lane): void {
    if (this.microBatchTimer) return;
    this.microBatchLane = lane;
    this.microBatchTimer = setImmediate(() => {
      this.microBatchTimer = null;
      const selected = this.microBatchLane;
      this.microBatchLane = null;
      this.pump(selected);
    });
  }

  private queryBatch(): Batch {
    const requests: QueryRequest[] = [];
    while (requests.length < this.config.embeddingBatchSize) {
      const next = this.queues.query[0];
      if (next?.kind !== 'query') break;
      this.queues.query.shift();
      requests.push(next);
    }
    return {
      lane: 'query',
      requests: new Set(requests),
      operation: 'embed_queries',
      payload: { texts: requests.map((request) => request.text) },
      apply: (raw) => {
        const vectors = decodeEmbeddingMatrix(raw);
        if (vectors.length !== requests.length) {
          throw new ModelWorkerFailureError('Query embedding batch length mismatch');
        }
        requests.forEach((request, index) => {
          const vector = vectors[index];
          if (vector) this.completeRequest(request, vector);
        });
      },
    };
  }

  private rawBatch(request: RawRequest): Batch {
    this.queues.query.shift();
    return {
      lane: 'query',
      requests: new Set([request]),
      operation: request.operation,
      payload: request.payload,
      apply: (raw) => this.completeRequest(request, raw),
    };
  }

  private documentsBatch(): Batch {
    const mappings: Array<{ request: DocumentsRequest; index: number }> = [];
    const requests = new Set<DocumentsRequest>();
    while (mappings.length < this.config.embeddingBatchSize) {
      const next = this.queues.ingestion[0];
      if (next?.kind !== 'documents') break;
      this.queues.ingestion.shift();
      requests.add(next);
      while (next.cursor < next.texts.length && mappings.length < this.config.embeddingBatchSize) {
        mappings.push({ request: next, index: next.cursor++ });
      }
      if (next.cursor < next.texts.length) break;
    }
    return {
      lane: 'ingestion',
      requests: new Set(requests),
      operation: 'embed_documents',
      payload: { texts: mappings.map(({ request, index }) => request.texts[index] ?? '') },
      apply: (raw) => {
        const vectors = decodeEmbeddingMatrix(raw);
        if (vectors.length !== mappings.length) {
          throw new ModelWorkerFailureError('Document embedding batch length mismatch');
        }
        mappings.forEach(({ request, index }, outputIndex) => {
          request.vectors[index] = vectors[outputIndex];
        });
        for (const request of requests) {
          if (request.cursor < request.texts.length) this.requeue(request);
          else this.completeRequest(request, completedArray(request.vectors, 'document embedding'));
        }
      },
    };
  }

  private countBatch(): Batch {
    const mappings: Array<{ request: CountRequest; index: number }> = [];
    const requests = new Set<CountRequest>();
    while (mappings.length < TOKEN_COUNT_BATCH_SIZE) {
      const next = this.queues.ingestion[0];
      if (next?.kind !== 'count') break;
      this.queues.ingestion.shift();
      requests.add(next);
      while (next.cursor < next.texts.length && mappings.length < TOKEN_COUNT_BATCH_SIZE) {
        mappings.push({ request: next, index: next.cursor++ });
      }
      if (next.cursor < next.texts.length) break;
    }
    return {
      lane: 'ingestion',
      requests: new Set(requests),
      operation: 'count_tokens',
      payload: { texts: mappings.map(({ request, index }) => request.texts[index] ?? '') },
      apply: (raw) => {
        const { counts } = countResponseSchema.parse(raw);
        if (counts.length !== mappings.length) {
          throw new ModelWorkerFailureError('Token count batch length mismatch');
        }
        mappings.forEach(({ request, index }, outputIndex) => {
          request.counts[index] = counts[outputIndex];
        });
        for (const request of requests) {
          if (request.cursor < request.texts.length) this.requeue(request);
          else this.completeRequest(request, completedArray(request.counts, 'token count'));
        }
      },
    };
  }

  private rerankBatch(): Batch {
    const mappings: Array<{ request: RerankRequest; index: number }> = [];
    const requests = new Set<RerankRequest>();
    let characters = 0;
    while (mappings.length < this.config.rerankBatchSize) {
      const next = this.queues.rerank[0];
      if (next?.kind !== 'rerank') break;
      this.queues.rerank.shift();
      requests.add(next);
      while (next.cursor < next.order.length && mappings.length < this.config.rerankBatchSize) {
        const index = next.order[next.cursor];
        if (index === undefined) {
          throw new ModelWorkerFailureError('Reranking order is incomplete');
        }
        const document = next.documents[index] ?? '';
        const pairCharacters = next.query.length + document.length;
        if (mappings.length > 0 && characters + pairCharacters > RERANK_SLICE_CHARACTER_BUDGET) {
          break;
        }
        mappings.push({ request: next, index });
        next.cursor += 1;
        characters += pairCharacters;
      }
      break;
    }
    return {
      lane: 'rerank',
      requests: new Set(requests),
      operation: 'rerank_pairs',
      payload: {
        pairs: mappings.map(({ request, index }) => ({
          query: request.query,
          document: request.documents[index] ?? '',
        })),
      },
      apply: (raw) => {
        const { scores } = rerankResponseSchema.parse(raw);
        if (scores.length !== mappings.length) {
          throw new ModelWorkerFailureError('Reranking batch length mismatch');
        }
        mappings.forEach(({ request, index }, outputIndex) => {
          request.scores[index] = scores[outputIndex];
        });
        for (const request of requests) {
          if (request.cursor < request.order.length) this.requeue(request);
          else this.completeRequest(request, completedArray(request.scores, 'reranking'));
        }
      },
    };
  }

  private buildBatch(lane: Lane): Batch | null {
    const first = this.queues[lane][0];
    if (!first) return null;
    if (lane === 'query') {
      return first.kind === 'raw' ? this.rawBatch(first) : this.queryBatch();
    }
    if (lane === 'ingestion') {
      return first.kind === 'count' ? this.countBatch() : this.documentsBatch();
    }
    return this.rerankBatch();
  }

  private pump(forcedLane: Lane | null = null): void {
    if (this.stopped || this.dispatching) return;
    const lane = forcedLane && this.queues[forcedLane].length > 0 ? forcedLane : this.selectLane();
    if (!lane) return;
    if (!forcedLane && this.shouldMicroBatch(lane)) {
      this.scheduleMicroBatch(lane);
      return;
    }
    const batch = this.buildBatch(lane);
    if (!batch || batch.requests.size === 0) return;
    this.dispatching = true;
    this.currentBatchRequests = batch.requests;
    for (const request of batch.requests) this.startRequest(request);
    this.counters.dispatchedBatches += 1;
    void this.dispatchBatch(batch);
  }

  private async dispatchBatch(batch: Batch): Promise<void> {
    try {
      const result = await this.transport.dispatch(batch.operation, batch.payload);
      batch.apply(result);
    } catch (error) {
      const failure =
        error instanceof Error
          ? error
          : new ModelWorkerFailureError(`Unknown model worker failure: ${String(error)}`);
      for (const request of batch.requests) this.failRequest(request, failure);
      this.logger.warn('Inference batch failed', {
        lane: batch.lane,
        operation: batch.operation,
        error: failure.message,
      });
    } finally {
      this.currentBatchRequests = new Set();
      this.dispatching = false;
      this.pump();
    }
  }

  public raw(operation: string, payload: Record<string, unknown> = {}): Promise<unknown> {
    return requestPromise((resolve, reject) => {
      const request: RawRequest = {
        ...this.base('raw', 'query', resolve, reject),
        kind: 'raw',
        operation,
        payload,
      };
      this.enqueue(request);
    });
  }

  public embedQuery(text: string): Promise<EmbeddingVector> {
    return requestPromise((resolve, reject) => {
      const request: QueryRequest = {
        ...this.base('query', 'query', resolve, reject),
        kind: 'query',
        text,
      };
      this.enqueue(request);
    });
  }

  public embedDocuments(texts: string[]): Promise<EmbeddingVector[]> {
    if (texts.length === 0) return Promise.resolve([]);
    return requestPromise((resolve, reject) => {
      const request: DocumentsRequest = {
        ...this.base('documents', 'ingestion', resolve, reject),
        kind: 'documents',
        texts,
        cursor: 0,
        vectors: Array.from({ length: texts.length }),
      };
      this.enqueue(request);
    });
  }

  public countTokens(texts: string[]): Promise<number[]> {
    if (texts.length === 0) return Promise.resolve([]);
    return requestPromise((resolve, reject) => {
      const request: CountRequest = {
        ...this.base('count', 'ingestion', resolve, reject),
        kind: 'count',
        texts,
        cursor: 0,
        counts: Array.from({ length: texts.length }),
      };
      this.enqueue(request);
    });
  }

  public rerank(query: string, documents: string[]): Promise<number[]> {
    if (documents.length === 0) return Promise.resolve([]);
    return requestPromise((resolve, reject) => {
      const request: RerankRequest = {
        ...this.base('rerank', 'rerank', resolve, reject),
        kind: 'rerank',
        query,
        documents,
        order: documents
          .map((document, index) => ({ index, length: query.length + document.length }))
          .sort((left, right) => right.length - left.length)
          .map(({ index }) => index),
        cursor: 0,
        scores: Array.from({ length: documents.length }),
      };
      this.enqueue(request);
    });
  }

  public snapshot(): InferenceSchedulerSnapshot {
    const queuedByLane = (lane: Lane): number =>
      new Set(this.queues[lane].map((request) => request.id)).size;
    const laneSnapshot = (lane: Lane) => {
      const metric = this.metrics[lane];
      return {
        queued: queuedByLane(lane),
        completed: metric.completed,
        meanQueueMs:
          metric.completed === 0 ? 0 : Math.round((metric.queueMs / metric.completed) * 100) / 100,
        meanExecutionMs:
          metric.completed === 0
            ? 0
            : Math.round((metric.executionMs / metric.completed) * 100) / 100,
      };
    };
    return {
      queueDepth: Math.max(0, this.outstanding.size - this.currentBatchRequests.size),
      queueLimit: this.config.inferenceQueueLimit,
      inFlight: this.dispatching,
      counters: { ...this.counters },
      lanes: {
        query: laneSnapshot('query'),
        ingestion: laneSnapshot('ingestion'),
        rerank: laneSnapshot('rerank'),
      },
    };
  }

  public async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    if (this.microBatchTimer) clearImmediate(this.microBatchTimer);
    this.microBatchTimer = null;
    this.microBatchLane = null;
    const error = new ModelWorkerFailureError('Inference scheduler stopped');
    for (const request of [...this.outstanding.values()]) this.failRequest(request, error);
    if (this.dispatching) this.transport.abort(error);
    await this.transport.stop();
  }
}
