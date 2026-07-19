import { performance } from 'node:perf_hooks';
import type { AppConfig } from '../config.js';
import type {
  MemoryRecord,
  SearchOptions,
  SearchResponse,
  SearchResult,
  SearchScoreExplanation,
} from '../domain/types.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { MemoryStore, RankedSegment } from '../storage/memory-store.js';

interface FusedCandidate {
  memoryId: string;
  revisionId: string;
  excerpt: string;
  path: string;
  rerankSegments: string[];
  score: SearchScoreExplanation;
  record?: MemoryRecord;
}

const RRF_CONSTANT = 60;
const EXCERPT_LIMIT = 2_000;
const EXCERPT_HEADER_LIMIT = 400;

function contextualExcerpt(candidate: FusedCandidate & { record: MemoryRecord }): string {
  const lines = candidate.record.revision.searchableText.split('\n');
  const fieldPrefix = `${candidate.path}:`;
  const matchedIndex = lines.findIndex((line) => line.startsWith(fieldPrefix));
  if (matchedIndex < 0) return candidate.excerpt.slice(0, EXCERPT_LIMIT);

  const separator = candidate.path.lastIndexOf('/');
  const parentPath = separator > 0 ? candidate.path.slice(0, separator) : candidate.path;
  const siblingPrefix = `${parentPath}/`;
  const siblingIndices = lines
    .map((line, index) => (line.startsWith(siblingPrefix) ? index : -1))
    .filter((index) => index >= 0);
  const matchedPosition = siblingIndices.indexOf(matchedIndex);
  if (matchedPosition < 0 || siblingIndices.length < 2) {
    return candidate.excerpt.slice(0, EXCERPT_LIMIT);
  }

  const headerLines: string[] = [];
  let headerLength = 0;
  for (const line of lines) {
    if (!line.startsWith('Title: ') && !line.startsWith('Kind: ') && !line.startsWith('Tags: ')) {
      continue;
    }
    const added = line.length + (headerLines.length > 0 ? 1 : 0);
    if (headerLength + added > EXCERPT_HEADER_LIMIT) continue;
    headerLines.push(line);
    headerLength += added;
  }

  const fieldBudget = EXCERPT_LIMIT - headerLength - (headerLines.length > 0 ? 1 : 0);
  const matchedLine = lines[matchedIndex];
  if (!matchedLine || matchedLine.length > fieldBudget) {
    return candidate.excerpt.slice(0, EXCERPT_LIMIT);
  }
  const selected = new Set<number>([matchedIndex]);
  let selectedLength = matchedLine.length;
  for (let distance = 1; distance < siblingIndices.length; distance += 1) {
    for (const position of [matchedPosition - distance, matchedPosition + distance]) {
      const index = siblingIndices[position];
      if (index === undefined || selected.has(index)) continue;
      const line = lines[index];
      if (line === undefined || selectedLength + line.length + 1 > fieldBudget) continue;
      selected.add(index);
      selectedLength += line.length + 1;
    }
  }
  const fieldLines = [...selected]
    .sort((left, right) => left - right)
    .map((index) => lines[index])
    .filter((line): line is string => line !== undefined);
  return [...headerLines, ...fieldLines].join('\n');
}

function addRanking(
  candidates: Map<string, FusedCandidate>,
  ranked: RankedSegment[],
  source: 'exact' | 'lexical' | 'semantic',
): void {
  const seen = new Set<string>();
  for (const segment of ranked) {
    if (seen.has(segment.memoryId)) continue;
    seen.add(segment.memoryId);
    const rank = seen.size;
    const contribution = source === 'exact' ? 0.1 / rank : 1 / (RRF_CONSTANT + rank);
    const existing = candidates.get(segment.memoryId);
    if (!existing) {
      const score: SearchScoreExplanation = { fusedScore: contribution };
      if (source === 'exact') score.exactBoost = contribution;
      else if (source === 'lexical') score.lexicalRank = rank;
      else score.semanticRank = rank;
      candidates.set(segment.memoryId, {
        memoryId: segment.memoryId,
        revisionId: segment.revisionId,
        excerpt: segment.text,
        path: segment.path,
        rerankSegments: [segment.text],
        score,
      });
      continue;
    }
    if (!existing.rerankSegments.includes(segment.text) && existing.rerankSegments.length < 6) {
      existing.rerankSegments.push(segment.text);
    }
    existing.score.fusedScore += contribution;
    if (source === 'exact') existing.score.exactBoost = contribution;
    else if (source === 'lexical') existing.score.lexicalRank = rank;
    else existing.score.semanticRank = rank;
    if (source === 'exact') {
      existing.excerpt = segment.text;
      existing.path = segment.path;
      existing.revisionId = segment.revisionId;
      continue;
    }
    const currentRank =
      source === 'lexical' ? existing.score.lexicalRank : existing.score.semanticRank;
    if (currentRank === rank) {
      existing.excerpt = segment.text;
      existing.path = segment.path;
      existing.revisionId = segment.revisionId;
    }
  }
}

function rerankDocument(candidate: FusedCandidate & { record: MemoryRecord }): string {
  const revision = candidate.record.revision;
  const context: string[] = [];
  if (revision.title) context.push(`Title: ${revision.title}`);
  if (revision.kind) context.push(`Kind: ${revision.kind}`);
  if (revision.tags.length > 0) context.push(`Tags: ${revision.tags.join(', ')}`);
  if (revision.confidence !== null) context.push(`Confidence: ${revision.confidence}`);
  if (revision.salience !== null) context.push(`Salience: ${revision.salience}`);
  if (revision.validFrom) context.push(`Valid from: ${revision.validFrom}`);
  if (revision.validTo) context.push(`Valid to: ${revision.validTo}`);
  const sources = revision.sources
    .map((source) => [source.label, source.type, source.uri].filter(Boolean).join(' | '))
    .filter(Boolean);
  if (sources.length > 0) context.push(`Sources: ${sources.join('; ')}`);
  context.push('Matched evidence:', ...candidate.rerankSegments);
  return context.join('\n').slice(0, 16_000);
}

export class SearchEngine {
  public constructor(
    private readonly config: AppConfig,
    private readonly store: MemoryStore,
    private readonly models: ModelClient,
    private readonly logger: Logger,
  ) {}

  public async search(options: SearchOptions): Promise<SearchResponse> {
    const started = performance.now();
    const mode = options.mode ?? 'auto';
    const topK = Math.max(1, Math.min(options.topK ?? 10, 50));
    const filters = {
      ...(options.spaceIds ? { spaceIds: options.spaceIds } : {}),
      ...(options.states ? { states: options.states } : {}),
      ...(options.kinds ? { kinds: options.kinds } : {}),
      ...(options.tags ? { tags: options.tags } : {}),
      ...(options.minConfidence !== undefined ? { minConfidence: options.minConfidence } : {}),
      ...(options.minSalience !== undefined ? { minSalience: options.minSalience } : {}),
      ...(options.atTime ? { atTime: options.atTime } : {}),
      ...(options.validAt ? { validAt: options.validAt } : {}),
    };
    const fused = new Map<string, FusedCandidate>();
    let degraded = false;
    let degradationReason: string | undefined;

    addRanking(
      fused,
      this.store.exactCandidates(options.query, filters, Math.min(topK * 2, 50)),
      'exact',
    );

    if (mode !== 'semantic') {
      addRanking(
        fused,
        this.store.lexicalCandidates(options.query, filters, this.config.lexicalCandidates),
        'lexical',
      );
    }

    if (mode !== 'lexical') {
      try {
        if (!this.config.modelsEnabled || !this.store.vectorAvailable) {
          throw new Error('Semantic inference or vector storage is disabled');
        }
        const vector = await this.models.embedQuery(options.query);
        const profile = await this.models.embeddingProfile();
        if (profile.embedding_dimension !== vector.length) {
          throw new Error('Embedding model dimension changed during query processing');
        }
        const modelProfileId = this.store.ensureModelProfile({
          provider: 'huggingface',
          model: profile.embedding_model,
          modelRevision: profile.embedding_revision,
          dimensions: vector.length,
          instructionHash: profile.query_instruction_hash,
        });
        addRanking(
          fused,
          this.store.semanticCandidates(
            vector,
            filters,
            this.config.semanticCandidates,
            modelProfileId,
          ),
          'semantic',
        );
      } catch (error) {
        degraded = true;
        degradationReason = `Semantic retrieval unavailable: ${String(error)}`;
        this.logger.warn('Search degraded to lexical retrieval', { error: String(error) });
      }
    }

    const queryNormalized = options.query.trim().toLocaleLowerCase();
    const recordsByRevisionId = this.store.getMemoriesByRevisionIds(
      [...fused.values()].map((candidate) => candidate.revisionId),
      options.atTime,
    );
    for (const candidate of fused.values()) {
      const record = recordsByRevisionId.get(candidate.revisionId);
      if (!record) continue;
      candidate.record = record;
      const title = candidate.record.revision.title?.toLocaleLowerCase() ?? '';
      if (title === queryNormalized || title.includes(queryNormalized)) {
        const titleBoost = title === queryNormalized ? 0.04 : 0.02;
        candidate.score.exactBoost = (candidate.score.exactBoost ?? 0) + titleBoost;
        candidate.score.fusedScore += titleBoost;
      }
    }

    if (options.expandRelations) {
      const seeds = [...fused.values()]
        .sort((left, right) => right.score.fusedScore - left.score.fusedScore)
        .slice(0, 10);
      for (const seed of seeds) {
        for (const link of this.store.linksFor(seed.memoryId, options.validAt ?? options.atTime)) {
          const adjacentId =
            link.fromMemoryId === seed.memoryId ? link.toMemoryId : link.fromMemoryId;
          if (fused.has(adjacentId)) continue;
          try {
            const record = this.store.getMemory(
              adjacentId,
              options.atTime ? { atTime: options.atTime } : {},
            );
            if (record.state !== 'active') continue;
            fused.set(adjacentId, {
              memoryId: adjacentId,
              revisionId: record.revision.id,
              excerpt: record.revision.searchableText.slice(0, 2_000),
              path: '$relation',
              rerankSegments: [record.revision.searchableText.slice(0, 8_000)],
              record,
              score: { relationBoost: 0.005, fusedScore: 0.005 },
            });
          } catch {
            // A relation to an inaccessible or historical tombstone is skipped.
          }
        }
      }
    }

    let ordered = [...fused.values()]
      .filter((candidate): candidate is FusedCandidate & { record: MemoryRecord } =>
        Boolean(candidate.record),
      )
      .sort((left, right) => right.score.fusedScore - left.score.fusedScore);

    const shouldRerank =
      mode === 'quality' || (mode === 'auto' && this.config.modelsEnabled && ordered.length > 1);
    if (shouldRerank) {
      const rerankSet = ordered.slice(0, this.config.rerankCandidates);
      try {
        const scores = await this.models.rerank(
          options.query,
          rerankSet.map((candidate) => rerankDocument(candidate)),
        );
        rerankSet.forEach((candidate, index) => {
          const score = scores[index];
          if (score === undefined) return;
          candidate.score.rerankerScore = score;
          candidate.score.fusedScore += score * 0.05;
        });
        ordered = ordered.sort((left, right) => right.score.fusedScore - left.score.fusedScore);
      } catch (error) {
        degraded = true;
        degradationReason = degradationReason
          ? `${degradationReason}; reranking unavailable: ${String(error)}`
          : `Reranking unavailable: ${String(error)}`;
      }
    }

    const results: SearchResult[] = ordered.slice(0, topK).map((candidate) => ({
      memory: candidate.record,
      excerpt: contextualExcerpt(candidate),
      segmentPath: candidate.path,
      score: candidate.score,
    }));
    const response: SearchResponse = {
      query: options.query,
      mode,
      degraded,
      results,
      timingMs: Math.round((performance.now() - started) * 100) / 100,
    };
    if (degradationReason) response.degradationReason = degradationReason;
    return response;
  }
}
