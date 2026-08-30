import { createHash, type Hash } from 'node:crypto';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { compileSchema } from '../validation.js';
import {
  type EmbeddingVector,
  InferenceScheduler,
  type InferenceSchedulerSnapshot,
} from './inference-scheduler.js';
import { ModelWorkerTransport } from './worker-transport.js';

const modelHealthSchema = compileSchema(
  z.object({
    status: z.string(),
    pid: z.number().int(),
    embedding_model: z.string(),
    embedding_revision: z.string(),
    reranker_model: z.string(),
    reranker_revision: z.string(),
    query_instruction_hash: z.string(),
    rerank_instruction_hash: z.string(),
    device: z.string(),
    device_name: z.string(),
    torch_version: z.string(),
    torch_cuda_version: z.string().nullable(),
    embedding_dimension: z.number().int().nullable(),
    embedding_loaded: z.boolean(),
    reranker_loaded: z.boolean(),
  }),
);

export type ModelHealth = z.infer<typeof modelHealthSchema>;
export type EmbeddingModelProfile = Pick<
  ModelHealth,
  'embedding_dimension' | 'embedding_model' | 'embedding_revision' | 'query_instruction_hash'
>;

const QUERY_EMBEDDING_CACHE_LIMIT = 128;
const RERANK_CACHE_LIMIT = 128;

interface RerankProfile {
  rerankerModel: string;
  rerankerRevision: string;
  instructionHash: string;
}

function pushFramedComponent(hash: Hash, value: string): void {
  const bytes = Buffer.from(value, 'utf8');
  hash.update(`${bytes.length}:`, 'utf8');
  hash.update(bytes);
}

function rerankCacheKey(query: string, documents: string[], profile: RerankProfile): string {
  const hash = createHash('sha256');
  pushFramedComponent(hash, 'v2');
  pushFramedComponent(hash, profile.rerankerModel);
  pushFramedComponent(hash, profile.rerankerRevision);
  pushFramedComponent(hash, profile.instructionHash);
  pushFramedComponent(hash, query);
  pushFramedComponent(hash, String(documents.length));
  for (const document of documents) {
    pushFramedComponent(hash, document);
  }
  return hash.digest('hex');
}

export class ModelClient {
  private readonly transport: ModelWorkerTransport;
  private readonly scheduler: InferenceScheduler;
  private embeddingProfilePromise: Promise<EmbeddingModelProfile> | null = null;
  private rerankProfilePromise: Promise<RerankProfile> | null = null;
  private readonly queryEmbeddingCache = new Map<string, Promise<EmbeddingVector>>();
  private readonly rerankCache = new Map<string, Promise<number[]>>();

  public constructor(config: AppConfig, logger: Logger, forwardWorkerStderr = false) {
    this.transport = new ModelWorkerTransport(config, logger, forwardWorkerStderr);
    this.scheduler = new InferenceScheduler(config, this.transport, logger);
  }

  public get processStarts(): number {
    return this.transport.processStarts;
  }

  public get launcherPid(): number | null {
    return this.transport.launcherPid;
  }

  public get workerPid(): number | null {
    return this.transport.workerPid;
  }

  public async health(): Promise<ModelHealth> {
    const health = modelHealthSchema.parse(await this.scheduler.raw('health'));
    this.transport.reportWorkerPid(health.pid);
    return health;
  }

  public embeddingProfile(): Promise<EmbeddingModelProfile> {
    if (this.embeddingProfilePromise) return this.embeddingProfilePromise;
    const pending = this.health().then((health) => {
      if (health.embedding_dimension === null) {
        throw new Error('Embedding model profile is unavailable before the model is loaded');
      }
      return {
        embedding_dimension: health.embedding_dimension,
        embedding_model: health.embedding_model,
        embedding_revision: health.embedding_revision,
        query_instruction_hash: health.query_instruction_hash,
      };
    });
    this.embeddingProfilePromise = pending;
    void pending.catch(() => {
      if (this.embeddingProfilePromise === pending) this.embeddingProfilePromise = null;
    });
    return pending;
  }

  public embedDocuments(texts: string[]): Promise<EmbeddingVector[]> {
    return this.scheduler.embedDocuments(texts);
  }

  public countTokens(texts: string[]): Promise<number[]> {
    return this.scheduler.countTokens(texts);
  }

  public embedQuery(text: string): Promise<EmbeddingVector> {
    const key = createHash('sha256').update(text, 'utf8').digest('hex');
    const cached = this.queryEmbeddingCache.get(key);
    if (cached) {
      this.queryEmbeddingCache.delete(key);
      this.queryEmbeddingCache.set(key, cached);
      return cached;
    }
    const pending = this.scheduler.embedQuery(text);
    this.queryEmbeddingCache.set(key, pending);
    void pending.catch(() => {
      if (this.queryEmbeddingCache.get(key) === pending) this.queryEmbeddingCache.delete(key);
    });
    while (this.queryEmbeddingCache.size > QUERY_EMBEDDING_CACHE_LIMIT) {
      const oldest = this.queryEmbeddingCache.keys().next();
      if (oldest.done) break;
      this.queryEmbeddingCache.delete(oldest.value);
    }
    return pending;
  }

  public rerank(query: string, documents: string[]): Promise<number[]> {
    return this.withRerankProfile().then(
      (profile) => this.cachedRerank(query, documents, profile),
      () => this.scheduler.rerank(query, documents),
    );
  }

  private withRerankProfile(): Promise<RerankProfile> {
    if (this.rerankProfilePromise) return this.rerankProfilePromise;
    const pending = this.health().then((health) => ({
      rerankerModel: health.reranker_model,
      rerankerRevision: health.reranker_revision,
      instructionHash: health.rerank_instruction_hash,
    }));
    this.rerankProfilePromise = pending;
    void pending.catch(() => {
      if (this.rerankProfilePromise === pending) this.rerankProfilePromise = null;
    });
    return pending;
  }

  private cachedRerank(
    query: string,
    documents: string[],
    profile: RerankProfile,
  ): Promise<number[]> {
    const key = rerankCacheKey(query, documents, profile);
    const cached = this.rerankCache.get(key);
    if (cached) {
      this.rerankCache.delete(key);
      this.rerankCache.set(key, cached);
      return cached;
    }
    const pending = this.scheduler.rerank(query, documents);
    this.rerankCache.set(key, pending);
    void pending.catch(() => {
      if (this.rerankCache.get(key) === pending) this.rerankCache.delete(key);
    });
    while (this.rerankCache.size > RERANK_CACHE_LIMIT) {
      const oldest = this.rerankCache.keys().next();
      if (oldest.done) break;
      this.rerankCache.delete(oldest.value);
    }
    return pending;
  }

  public schedulerStatus(): InferenceSchedulerSnapshot {
    return this.scheduler.snapshot();
  }

  public stop(): Promise<void> {
    return this.scheduler.stop();
  }
}
