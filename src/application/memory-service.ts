import type { AppConfig } from '../config.js';
import type {
  JsonObject,
  MemoryInput,
  MemoryListFilters,
  MemoryRecord,
  SearchOptions,
} from '../domain/types.js';
import type { Indexer } from '../indexing/indexer.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { SearchEngine } from '../retrieval/search-engine.js';
import type { MemoryStore } from '../storage/memory-store.js';

export class MemoryService {
  public constructor(
    private readonly config: AppConfig,
    private readonly store: MemoryStore,
    private readonly indexer: Indexer,
    private readonly searchEngine: SearchEngine,
    private readonly models: ModelClient,
    private readonly logger: Logger,
  ) {}

  public createSpace(input: {
    id?: string;
    name: string;
    description?: string;
    metadata?: JsonObject;
  }): ReturnType<MemoryStore['createSpace']> {
    return this.store.createSpace(input);
  }

  public listSpaces(): ReturnType<MemoryStore['listSpaces']> {
    return this.store.listSpaces();
  }

  public async createMemory(
    input: MemoryInput,
    actor: string | null = null,
  ): Promise<MemoryRecord> {
    const created = this.store.createMemory(input, actor);
    try {
      return await this.indexer.indexRevision(created.revision.id);
    } catch (error) {
      this.logger.error('Memory was stored but indexing failed', {
        memoryId: created.id,
        error: String(error),
      });
      this.store.markIndexStatus(created.revision.id, 'failed', String(error));
      return this.store.getMemory(created.id);
    }
  }

  public async reviseMemory(
    memoryId: string,
    input: MemoryInput,
    expectedRevisionId: string,
    actor: string | null = null,
  ): Promise<MemoryRecord> {
    const revised = this.store.reviseMemory(memoryId, input, expectedRevisionId, actor);
    try {
      return await this.indexer.indexRevision(revised.revision.id);
    } catch (error) {
      this.logger.error('Revision was stored but indexing failed', {
        memoryId,
        error: String(error),
      });
      this.store.markIndexStatus(revised.revision.id, 'failed', String(error));
      return this.store.getMemory(memoryId);
    }
  }

  public getMemory(
    memoryId: string,
    options: { revisionId?: string; atTime?: string } = {},
  ): MemoryRecord {
    return this.store.getMemory(memoryId, options);
  }

  public getHistory(memoryId: string): ReturnType<MemoryStore['getHistory']> {
    return this.store.getHistory(memoryId);
  }

  public listMemories(filters: MemoryListFilters): ReturnType<MemoryStore['listMemories']> {
    return this.store.listMemories(filters);
  }

  public search(options: SearchOptions): ReturnType<SearchEngine['search']> {
    return this.searchEngine.search(options);
  }

  public setState(memoryId: string, state: 'active' | 'archived'): MemoryRecord {
    return this.store.setState(memoryId, state);
  }

  public deleteMemory(memoryId: string): boolean {
    return this.store.deleteMemory(memoryId);
  }

  public createLink(
    input: Parameters<MemoryStore['createLink']>[0],
  ): ReturnType<MemoryStore['createLink']> {
    return this.store.createLink(input);
  }

  public unlink(linkId: string): ReturnType<MemoryStore['unlink']> {
    return this.store.unlink(linkId);
  }

  public traverse(
    memoryId: string,
    maxDepth?: number,
    atTime?: string,
  ): ReturnType<MemoryStore['traverse']> {
    return this.store.traverse(memoryId, maxDepth, atTime);
  }

  public recordFeedback(
    input: Parameters<MemoryStore['recordFeedback']>[0],
  ): ReturnType<MemoryStore['recordFeedback']> {
    return this.store.recordFeedback(input);
  }

  public async status(probeModels = false): Promise<Record<string, unknown>> {
    let modelHealth: Awaited<ReturnType<ModelClient['health']>> | undefined;
    let modelError: string | undefined;
    if (probeModels && this.config.modelsEnabled) {
      try {
        modelHealth = await this.models.health();
      } catch (error) {
        modelError = String(error);
      }
    }
    const result: Record<string, unknown> = {
      ...this.store.status(),
      modelsEnabled: this.config.modelsEnabled,
      modelLauncherPid: this.models.launcherPid,
      modelWorkerPid: this.models.workerPid,
      modelWorkerStarts: this.models.processStarts,
    };
    if (modelHealth) result.modelHealth = modelHealth;
    if (modelError) result.modelError = modelError;
    return result;
  }

  public async reindexPending(): Promise<ReturnType<Indexer['indexPending']>> {
    return this.indexer.indexPending();
  }

  public async reindexAll(): Promise<{ queued: number; indexed: number; failed: number }> {
    const queued = this.store.queueAllCurrentForReindex();
    return { queued, ...(await this.indexer.indexPending()) };
  }

  public async warmModels(
    reportProgress: (message: string) => void = () => undefined,
  ): Promise<Record<string, unknown>> {
    reportProgress('Loading the embedding model; missing files will be downloaded');
    const embedding = await this.models.embedQuery('Simple Memory model readiness probe');
    reportProgress(`Embedding model ready (${String(embedding.length)} dimensions)`);
    reportProgress('Loading the reranker model; missing files will be downloaded');
    const reranker = await this.models.rerank('memory readiness', [
      'This candidate describes memory readiness.',
    ]);
    reportProgress('Reranker model ready');
    reportProgress('Verifying model runtime health');
    return {
      health: await this.models.health(),
      embeddingDimensions: embedding.length,
      rerankerScore: reranker[0] ?? null,
    };
  }

  public exportSnapshot(): ReturnType<MemoryStore['exportSnapshot']> {
    return this.store.exportSnapshot();
  }

  public compact(): void {
    this.store.compact();
  }

  public purgeDeleted(): number {
    return this.store.purgeDeleted();
  }

  public async close(): Promise<void> {
    await this.models.stop();
    this.store.close();
  }
}
