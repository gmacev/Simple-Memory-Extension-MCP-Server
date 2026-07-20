import { createHash } from 'node:crypto';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import type {
  JsonObject,
  MemoryInput,
  MemoryLinkDirection,
  MemoryListFilters,
  MemoryRecord,
  MemoryTraversalEntry,
  MemoryTraversalOptions,
  MemoryTraversalPage,
  SearchOptions,
} from '../domain/types.js';
import type { Indexer } from '../indexing/indexer.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { SearchEngine } from '../retrieval/search-engine.js';
import type { MemoryStore } from '../storage/memory-store.js';

const traversalCursorSchema = z.object({
  offset: z.number().int().min(1).max(10_000),
  atTime: z.string(),
  fingerprint: z.string().regex(/^[a-f0-9]{64}$/u),
});

const DEFAULT_TRAVERSAL_LIMIT = 50;
const MAX_TRAVERSAL_LIMIT = 200;
const MAX_TRAVERSAL_OFFSET = 10_000;
const RANKED_TRAVERSAL_CANDIDATES = 500;

function decodeTraversalCursor(cursor: string): z.infer<typeof traversalCursorSchema> {
  try {
    return traversalCursorSchema.parse(
      JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')),
    );
  } catch {
    throw new Error('Invalid memory traversal cursor');
  }
}

function encodeTraversalCursor(
  offset: number,
  atTime: string,
  fingerprint: string,
): string {
  return Buffer.from(JSON.stringify({ offset, atTime, fingerprint }), 'utf8').toString(
    'base64url',
  );
}

function traversalFingerprint(options: {
  memoryId: string;
  maxDepth: number;
  relations: string[];
  direction: MemoryLinkDirection;
  query: string | undefined;
}): string {
  return createHash('sha256').update(JSON.stringify(options), 'utf8').digest('hex');
}

function lexicalTraversalScore(query: string, entry: MemoryTraversalEntry): number {
  const normalizedQuery = query.toLocaleLowerCase();
  const text = entry.memory.revision.searchableText.toLocaleLowerCase();
  const queryTerms = [...new Set(normalizedQuery.match(/[\p{L}\p{N}]+/gu) ?? [])];
  const matchingTerms = queryTerms.filter((term) => text.includes(term)).length;
  const overlap = queryTerms.length > 0 ? matchingTerms / queryTerms.length : 0;
  const exact = text.includes(normalizedQuery) ? 1 : 0;
  const distance = 1 / (entry.depth + 1);
  const salience = entry.memory.revision.salience ?? 0.5;
  const confidence = entry.memory.revision.confidence ?? 0.5;
  return exact * 0.35 + overlap * 0.4 + distance * 0.15 + salience * 0.05 + confidence * 0.05;
}

function traversalRerankDocument(entry: MemoryTraversalEntry): string {
  const relations = entry.path.map((step) => step.link.relation).join(' -> ');
  const path = relations ? `Relationship path: ${relations}\n` : '';
  return `${path}${entry.memory.revision.searchableText}`.slice(0, 16_000);
}

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

  public async traverse(options: MemoryTraversalOptions): Promise<MemoryTraversalPage> {
    const cursor = options.cursor ? decodeTraversalCursor(options.cursor) : undefined;
    const maxDepth = Math.max(0, Math.min(options.maxDepth ?? 2, 5));
    const limit = Math.max(1, Math.min(options.limit ?? DEFAULT_TRAVERSAL_LIMIT, MAX_TRAVERSAL_LIMIT));
    const relations = [
      ...new Set(
        (options.relations ?? [])
          .map((relation) => relation.trim().toLocaleLowerCase())
          .filter(Boolean),
      ),
    ].sort();
    const direction = options.direction ?? 'both';
    const query = options.query?.trim() || undefined;
    const atTime = options.atTime ?? cursor?.atTime ?? new Date().toISOString();
    if (cursor && options.atTime && options.atTime !== cursor.atTime) {
      throw new Error('memory_traverse atTime must remain unchanged while using a cursor');
    }
    const fingerprint = traversalFingerprint({
      memoryId: options.memoryId,
      maxDepth,
      relations,
      direction,
      query,
    });
    if (cursor && cursor.fingerprint !== fingerprint) {
      throw new Error('memory_traverse filters must remain unchanged while using a cursor');
    }
    const offset = cursor?.offset ?? 0;
    const requestedResults = query
      ? RANKED_TRAVERSAL_CANDIDATES + 1
      : Math.min(offset + limit + 1, MAX_TRAVERSAL_OFFSET + 1);
    const traversed = this.store.traverseCandidates({
      memoryId: options.memoryId,
      maxDepth,
      atTime,
      relations,
      direction,
      maxResults: requestedResults,
    });
    let degraded = false;
    let degradationReason: string | undefined;
    let candidates = query
      ? traversed.items.slice(0, RANKED_TRAVERSAL_CANDIDATES)
      : traversed.items;
    const exceededRankedCandidateLimit = Boolean(
      query && (traversed.items.length > RANKED_TRAVERSAL_CANDIDATES || traversed.truncated),
    );

    if (query) {
      const root = candidates.find((entry) => entry.depth === 0);
      const connected = candidates.filter((entry) => entry.depth > 0);
      for (const entry of candidates) {
        entry.relevanceScore = lexicalTraversalScore(query, entry);
      }
      const rerankSet = [...connected]
        .sort((left, right) =>
          (right.relevanceScore ?? 0) - (left.relevanceScore ?? 0) ||
          left.depth - right.depth ||
          left.memory.id.localeCompare(right.memory.id),
        )
        .slice(0, this.config.rerankCandidates);
      if (rerankSet.length > 0) {
        try {
          if (!this.config.modelsEnabled) throw new Error('Model inference is disabled');
          const scores = await this.models.rerank(
            query,
            rerankSet.map((entry) => traversalRerankDocument(entry)),
          );
          rerankSet.forEach((entry, index) => {
            const rerankerScore = scores[index];
            if (rerankerScore === undefined) return;
            entry.rerankerScore = rerankerScore;
            entry.relevanceScore = (entry.relevanceScore ?? 0) * 0.4 + rerankerScore * 0.6;
          });
        } catch (error) {
          degraded = true;
          degradationReason = `Graph reranking unavailable: ${String(error)}`;
          this.logger.warn('Graph traversal degraded to lexical ranking', {
            error: String(error),
          });
        }
      }
      connected.sort(
        (left, right) =>
          (right.relevanceScore ?? 0) - (left.relevanceScore ?? 0) ||
          left.depth - right.depth ||
          left.memory.id.localeCompare(right.memory.id),
      );
      candidates = root ? [root, ...connected] : connected;
      for (const entry of candidates) {
        if (entry.relevanceScore !== undefined) {
          entry.relevanceScore = Math.round(entry.relevanceScore * 1_000_000) / 1_000_000;
        }
        if (entry.rerankerScore !== undefined) {
          entry.rerankerScore = Math.round(entry.rerankerScore * 1_000_000) / 1_000_000;
        }
      }
    }

    const items = candidates.slice(offset, offset + limit);
    const hasMore = offset + limit < candidates.length && offset + limit < MAX_TRAVERSAL_OFFSET;
    const page: MemoryTraversalPage = {
      items,
      nextCursor: hasMore
        ? encodeTraversalCursor(offset + limit, atTime, fingerprint)
        : null,
      truncated: hasMore || traversed.truncated || exceededRankedCandidateLimit,
      atTime,
      degraded,
    };
    if (query) page.query = query;
    if (degradationReason) page.degradationReason = degradationReason;
    return page;
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

  public migrationStatus(): ReturnType<MemoryStore['migrationStatus']> {
    return this.store.migrationStatus();
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
