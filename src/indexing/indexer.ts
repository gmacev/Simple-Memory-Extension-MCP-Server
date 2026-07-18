import { createHash } from 'node:crypto';
import type { AppConfig } from '../config.js';
import type { MemoryRecord, SegmentRecord } from '../domain/types.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { MemoryStore } from '../storage/memory-store.js';
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

  public async indexRevision(revisionId: string, alreadyClaimed = false): Promise<MemoryRecord> {
    if (!alreadyClaimed) this.store.markRevisionIndexRunning(revisionId);
    const record = this.store.revisionForIndex(revisionId);
    let segments = createSegments({
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
      this.logger.warn('Exact Qwen token counting failed; retaining deterministic estimates', {
        revisionId,
        error: String(error),
      });
    }
    this.store.indexSegments(revisionId, segments, record.revision.title, record.revision.tags);
    if (!this.config.modelsEnabled || !this.store.vectorAvailable) {
      this.store.markIndexStatus(revisionId, 'lexical-only');
      return this.store.getMemory(record.id);
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
      const health = await this.models.health();
      if (health.embedding_dimension !== this.config.embeddingDimension) {
        throw new Error(
          `Embedding model reports dimension ${String(health.embedding_dimension)}; expected ${this.config.embeddingDimension}`,
        );
      }
      const modelProfileId = this.store.ensureModelProfile({
        provider: 'huggingface',
        model: health.embedding_model,
        modelRevision: health.embedding_revision,
        dimensions: health.embedding_dimension,
        instructionHash: health.query_instruction_hash,
      });
      this.store.indexVectors(segments, vectors, modelProfileId);
      this.store.markIndexStatus(revisionId, 'ready');
    } catch (error) {
      this.logger.warn('Semantic indexing degraded; lexical index remains available', {
        revisionId,
        error: String(error),
      });
      this.store.markIndexStatus(revisionId, 'lexical-only', String(error));
    }
    return this.store.getMemory(record.id);
  }

  public async indexPending(): Promise<{ indexed: number; failed: number }> {
    let indexed = 0;
    let failed = 0;
    while (true) {
      const revisionId = this.store.claimNextPendingRevision();
      if (!revisionId) break;
      try {
        await this.indexRevision(revisionId, true);
        indexed += 1;
      } catch (error) {
        failed += 1;
        this.store.markIndexStatus(revisionId, 'failed', String(error));
      }
    }
    return { indexed, failed };
  }
}
