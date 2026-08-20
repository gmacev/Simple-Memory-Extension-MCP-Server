import { createHash } from 'node:crypto';
import type { AppConfig } from '../config.js';
import type { MemoryRecord, SegmentRecord } from '../domain/types.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { ClaimedIndexJob, MemoryStore } from '../storage/memory-store.js';
import { createSegments } from './projector.js';

const MAX_SEGMENT_TOKENS = 1_200;

function splitAtBoundary(text: string): [string, string] {
  const midpoint = Math.floor(text.length / 2);
  const candidates = [
    text.lastIndexOf('\n', midpoint),
    text.lastIndexOf('. ', midpoint),
    text.lastIndexOf(' ', midpoint),
  ];
  const boundary = candidates.find((candidate) => candidate > text.length * 0.25) ?? midpoint;
  return [text.slice(0, boundary + 1).trim(), text.slice(boundary + 1).trim()];
}

function rebuildSegments(base: SegmentRecord[]): SegmentRecord[] {
  return base.map((segment, ordinal) => ({
    ...segment,
    id: `${segment.revisionId}:${ordinal}`,
    ordinal,
    contentHash: createHash('sha256').update(segment.text).digest('hex'),
  }));
}

export class Indexer {
  public constructor(
    private readonly config: AppConfig,
    private readonly store: MemoryStore,
    private readonly models: ModelClient,
    private readonly logger: Logger,
  ) {}

  private async exactTokenize(segments: SegmentRecord[]): Promise<SegmentRecord[]> {
    if (!this.config.modelsEnabled) return segments;
    let working = segments;
    for (let pass = 0; pass < 4; pass += 1) {
      const counts = await this.models.countTokens(working.map((segment) => segment.text));
      let changed = false;
      const next: SegmentRecord[] = [];
      for (let index = 0; index < working.length; index += 1) {
        const segment = working[index];
        const tokenCount = counts[index];
        if (!segment || tokenCount === undefined) throw new Error('Tokenizer result mismatch');
        if (tokenCount <= MAX_SEGMENT_TOKENS) {
          next.push({ ...segment, tokenCount });
          continue;
        }
        const [left, right] = splitAtBoundary(segment.text);
        if (!left || !right) {
          next.push({ ...segment, tokenCount });
          continue;
        }
        changed = true;
        next.push({ ...segment, text: left }, { ...segment, text: right });
      }
      working = rebuildSegments(next);
      if (!changed) return working;
    }
    return working;
  }

  public async indexRevision(
    revisionId: string,
    alreadyClaimed = false,
    preparedRecord?: MemoryRecord,
    claimedJob?: ClaimedIndexJob,
  ): Promise<MemoryRecord> {
    if (!alreadyClaimed) this.store.markRevisionIndexRunning(revisionId);
    const record = preparedRecord ?? this.store.revisionForIndex(revisionId);
    const requiresSemanticIndex = claimedJob?.embeddingGenerationId != null;
    let segments = requiresSemanticIndex ? this.store.segmentsForRevision(revisionId) : [];
    if (segments.length === 0) {
      segments = createSegments({
        memoryId: record.id,
        revisionId: record.revision.id,
        spaceId: record.spaceId,
        title: record.revision.title,
        kind: record.revision.kind,
        content: record.revision.content,
        tags: record.revision.tags,
        metadata: record.revision.metadata,
        sources: record.revision.sources,
      });
      try {
        segments = await this.exactTokenize(segments);
      } catch (error) {
        this.logger.warn('Exact model token counting failed; retaining deterministic estimates', {
          revisionId,
          error: String(error),
        });
      }
      this.store.indexSegments(revisionId, segments, record.revision.title, record.revision.tags);
    }
    if (!this.config.modelsEnabled || !this.store.vectorAvailable) {
      const error = 'Semantic models or sqlite-vec are unavailable';
      this.store.markIndexStatus(
        revisionId,
        'lexical-only',
        requiresSemanticIndex ? error : undefined,
        claimedJob
          ? { id: claimedJob.id, status: requiresSemanticIndex ? 'failed' : 'complete' }
          : undefined,
      );
      if (requiresSemanticIndex) throw new Error(error);
      return this.store.revisionForIndex(revisionId);
    }
    try {
      const vectors = await this.models.embedDocuments(segments.map((segment) => segment.text));
      for (const vector of vectors) {
        if (vector.length !== this.config.embeddingDimension) {
          throw new Error(
            `Embedding dimension ${vector.length} does not match configured ${this.config.embeddingDimension}`,
          );
        }
      }
      const profile = await this.models.embeddingProfile();
      if (profile.embedding_dimension !== this.config.embeddingDimension) {
        throw new Error(
          `Embedding model reports dimension ${String(profile.embedding_dimension)}; expected ${this.config.embeddingDimension}`,
        );
      }
      const modelProfileId = this.store.ensureModelProfile({
        provider: 'huggingface',
        model: profile.embedding_model,
        modelRevision: profile.embedding_revision,
        dimensions: profile.embedding_dimension,
        instructionHash: profile.query_instruction_hash,
      });
      if (
        claimedJob?.embeddingGenerationId &&
        modelProfileId !== claimedJob.embeddingGenerationId
      ) {
        throw new Error(
          `Embedding generation ${claimedJob.embeddingGenerationId} does not match loaded model profile ${modelProfileId}`,
        );
      }
      this.store.indexVectors(segments, vectors, modelProfileId);
      this.store.markIndexStatus(
        revisionId,
        'ready',
        undefined,
        claimedJob ? { id: claimedJob.id, status: 'complete' } : undefined,
      );
    } catch (error) {
      this.logger.warn('Semantic indexing degraded; lexical index remains available', {
        revisionId,
        error: String(error),
      });
      this.store.markIndexStatus(
        revisionId,
        'lexical-only',
        String(error),
        claimedJob
          ? { id: claimedJob.id, status: requiresSemanticIndex ? 'failed' : 'complete' }
          : undefined,
      );
      if (requiresSemanticIndex) throw error;
    }
    return this.store.revisionForIndex(revisionId);
  }

  public async indexPending(
    createdBefore?: string,
    options: {
      embeddingGenerationId?: string;
      includeEmbeddingGenerations?: boolean;
      onProgress?: (indexed: number, failed: number) => void;
    } = {},
  ): Promise<{ indexed: number; failed: number }> {
    let indexed = 0;
    let failed = 0;
    while (true) {
      const job = this.store.claimNextPendingRevision(createdBefore, {
        ...(options.embeddingGenerationId
          ? { embeddingGenerationId: options.embeddingGenerationId }
          : {}),
        includeEmbeddingGenerations:
          options.includeEmbeddingGenerations ?? this.config.modelsEnabled,
      });
      if (!job) break;
      try {
        await this.indexRevision(job.revisionId, true, undefined, job);
        indexed += 1;
      } catch (error) {
        failed += 1;
        if (job.embeddingGenerationId) {
          this.store.failClaimedIndexJob(job.id, String(error));
        } else {
          this.store.markIndexStatus(job.revisionId, 'failed', String(error), {
            id: job.id,
            status: 'failed',
          });
        }
      }
      options.onProgress?.(indexed, failed);
    }
    return { indexed, failed };
  }
}
