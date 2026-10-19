import { performance } from 'node:perf_hooks';
import type { AppConfig } from '../config.js';
import type {
  MemorySearchRecord,
  SearchOptions,
  SearchResponse,
  SearchResult,
  SearchScoreExplanation,
  SearchStageTimings,
  SearchTemporalContext,
} from '../domain/types.js';
import type { Logger } from '../logger.js';
import type { ModelClient } from '../models/model-client.js';
import type { MemoryStore, RankedSegment } from '../storage/memory-store.js';
import { datedExcerpt, queryTimeWindows, recordedSnapshotTime } from './temporal.js';

interface FusedCandidate {
  memoryId: string;
  revisionId: string;
  excerpt: string;
  path: string;
  rerankSegments: string[];
  exactRankValue?: number;
  score: SearchScoreExplanation;
  record?: MemorySearchRecord;
}

const RRF_CONSTANT = 60;
const DEFAULT_TOP_K = 5;
// Equal date matches retain the existing relevance order; observation time is weaker evidence.
const CONTENT_TIME_BOOST = 0.085;
const OBSERVED_TIME_BOOST = 0.055;

function roundStageMs(value: number): number {
  return Math.round(value * 100) / 100;
}
const EXCERPT_LIMIT = 2_000;
const EXCERPT_HEADER_LIMIT = 400;
const RERANK_DOCUMENT_LIMIT = 8_000;
const RERANK_EVIDENCE_SEGMENTS = 2;
const MIN_RERANK_CANDIDATES = 3;
const RERANK_CANDIDATE_MULTIPLIER = 2;
const AUTO_CLEAR_LEAD_RATIO = 1.1;
const DECISIVE_RERANKER_SCORE = 0.5;
const DECISIVE_RUNNER_UP_RATIO = 0.1;

function contextualExcerpt(candidate: FusedCandidate & { record: MemorySearchRecord }): string {
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
  source: 'exact' | 'lexical' | 'semantic' | 'temporal',
): void {
  const seen = new Set<string>();
  const alreadyAligned = new Set<string>();
  if (source === 'temporal') {
    for (const segment of ranked) {
      const candidate = candidates.get(segment.memoryId);
      if (candidate?.path === segment.path && candidate.excerpt === segment.text)
        alreadyAligned.add(segment.memoryId);
    }
  }
  for (const segment of ranked) {
    if (seen.has(segment.memoryId)) continue;
    seen.add(segment.memoryId);
    const rank = seen.size;
    const contribution =
      source === 'exact'
        ? 0.1 / rank
        : source === 'temporal'
          ? segment.timeMatch?.basis === 'observed'
            ? OBSERVED_TIME_BOOST
            : CONTENT_TIME_BOOST
          : 1 / (RRF_CONSTANT + rank);
    const existing = candidates.get(segment.memoryId);
    if (!existing) {
      const score: SearchScoreExplanation = { fusedScore: contribution };
      if (source === 'exact') score.exactBoost = contribution;
      else if (source === 'lexical') score.lexicalRank = rank;
      else if (source === 'semantic') score.semanticRank = rank;
      else {
        score.temporalRank = rank;
        if (segment.timeMatch) score.timeMatch = segment.timeMatch;
      }
      candidates.set(segment.memoryId, {
        memoryId: segment.memoryId,
        revisionId: segment.revisionId,
        excerpt:
          source === 'temporal' && segment.timeMatch
            ? datedExcerpt(segment.text, segment.timeMatch, EXCERPT_LIMIT)
            : segment.text,
        path: segment.path,
        rerankSegments: [segment.text],
        ...(source === 'exact' ? { exactRankValue: segment.rankValue } : {}),
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
    else if (source === 'semantic') existing.score.semanticRank = rank;
    else {
      existing.score.temporalRank = rank;
      if (segment.timeMatch) existing.score.timeMatch = segment.timeMatch;
    }
    if (source === 'exact' || source === 'temporal') {
      if (source === 'temporal' && alreadyAligned.has(segment.memoryId)) {
        if (segment.timeMatch)
          existing.excerpt = datedExcerpt(existing.excerpt, segment.timeMatch, EXCERPT_LIMIT);
        continue;
      }
      if (source === 'temporal')
        existing.rerankSegments = [
          segment.text,
          ...existing.rerankSegments.filter((text) => text !== segment.text),
        ];
      if (source === 'exact') existing.exactRankValue = segment.rankValue;
      existing.excerpt =
        source === 'temporal' && segment.timeMatch
          ? datedExcerpt(segment.text, segment.timeMatch, EXCERPT_LIMIT)
          : segment.text;
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

function rerankDocument(candidate: FusedCandidate & { record: MemorySearchRecord }): string {
  const revision = candidate.record.revision;
  const context: string[] = [];
  if (revision.title) context.push(`Title: ${revision.title}`);
  if (revision.kind) context.push(`Kind: ${revision.kind}`);
  if (revision.tags.length > 0) context.push(`Tags: ${revision.tags.join(', ')}`);
  if (revision.confidence !== null) context.push(`Confidence: ${revision.confidence}`);
  if (revision.salience !== null) context.push(`Salience: ${revision.salience}`);
  if (revision.validFrom) context.push(`Valid from: ${revision.validFrom}`);
  if (revision.validTo) context.push(`Valid to: ${revision.validTo}`);
  context.push('Matched evidence:', ...candidate.rerankSegments.slice(0, RERANK_EVIDENCE_SEGMENTS));
  const matchedSourceIndex = /^sources\/(\d+)\//u.exec(candidate.path)?.[1];
  if (matchedSourceIndex !== undefined) {
    const source = revision.sources[Number(matchedSourceIndex)];
    if (source) {
      context.push(
        `Matched source: ${[source.label, source.type, source.uri].filter(Boolean).join(' | ')}`,
      );
    }
  }
  return context.join('\n').slice(0, RERANK_DOCUMENT_LIMIT);
}

function isStrongExactMatch(candidate: FusedCandidate): boolean {
  return candidate.exactRankValue !== undefined && candidate.exactRankValue <= 2;
}

function autoSearchNeedsReranking(
  ordered: Array<FusedCandidate & { record: MemorySearchRecord }>,
  scoped: boolean,
): boolean {
  if (ordered.length <= 1) return false;
  const first = ordered[0];
  const second = ordered[1];
  if (!first || !second) return false;
  if (isStrongExactMatch(first)) return false;
  if (!scoped) return true;
  const retrievalChannelsAgree = first.score.lexicalRank === 1 && first.score.semanticRank === 1;
  const hasClearLead = first.score.fusedScore >= second.score.fusedScore * AUTO_CLEAR_LEAD_RATIO;
  return !(retrievalChannelsAgree && hasClearLead);
}

function rerankCandidateLimit(topK: number, configuredLimit: number): number {
  return Math.min(
    configuredLimit,
    Math.max(MIN_RERANK_CANDIDATES, topK * RERANK_CANDIDATE_MULTIPLIER),
  );
}

function pruneWeakRerankAlternatives(
  ordered: Array<FusedCandidate & { record: MemorySearchRecord }>,
): Array<FusedCandidate & { record: MemorySearchRecord }> {
  const winner = ordered[0];
  const winnerScore = winner?.score.rerankerScore;
  if (!winner || winnerScore === undefined || winnerScore < DECISIVE_RERANKER_SCORE) {
    return ordered;
  }
  const relevanceFloor = winnerScore * DECISIVE_RUNNER_UP_RATIO;
  const hasWeakRerankedAlternative = ordered.some(
    (candidate, index) =>
      index > 0 &&
      candidate.score.rerankerScore !== undefined &&
      candidate.score.rerankerScore < relevanceFloor,
  );
  if (!hasWeakRerankedAlternative) return ordered;
  return ordered.filter(
    (candidate, index) =>
      index === 0 ||
      isStrongExactMatch(candidate) ||
      (candidate.score.rerankerScore !== undefined &&
        candidate.score.rerankerScore >= relevanceFloor),
  );
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
    const inferredAtTime =
      options.atTime === undefined && options.timeWindows === undefined
        ? recordedSnapshotTime(options.query)
        : undefined;
    if (inferredAtTime) options = { ...options, atTime: inferredAtTime };
    const windows = (
      options.timeWindows ?? (options.atTime ? [] : queryTimeWindows(options.query))
    ).map((window) => ({
      start: new Date(window.start).toISOString(),
      end: new Date(window.end).toISOString(),
    }));
    if (windows.length > 4) throw new Error('timeWindows supports at most four windows');
    for (const window of windows) {
      if (
        !Number.isFinite(Date.parse(window.start)) ||
        !Number.isFinite(Date.parse(window.end)) ||
        window.start >= window.end
      ) {
        throw new Error('timeWindows must contain valid start < end timestamps');
      }
    }
    const temporal: SearchTemporalContext | undefined =
      windows.length > 0 || inferredAtTime
        ? {
            windows,
            source: options.timeWindows ? 'explicit' : 'query',
            ...(inferredAtTime ? { atTime: inferredAtTime } : {}),
          }
        : undefined;
    const mode = options.mode ?? 'auto';
    const topK = Math.max(1, Math.min(options.topK ?? DEFAULT_TOP_K, 50));
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
    const stages: SearchStageTimings = {};
    const exactStarted = performance.now();
    addRanking(
      fused,
      this.store.exactCandidates(options.query, filters, Math.min(topK * 2, 50)),
      'exact',
    );
    stages.exactMs = roundStageMs(performance.now() - exactStarted);

    // An exact key, ID, or title identifies its result without embedding or
    // reranking. Keep quality mode and relation expansion on their explicit paths.
    if (fused.size > 0 && mode !== 'quality' && !options.expandRelations) {
      const records = this.store.getMemoriesByRevisionIds(
        [...fused.values()].map((candidate) => candidate.revisionId),
        options.atTime,
        options.includeSourceMetadata ?? false,
      );
      const exactResults: SearchResult[] = [...fused.values()]
        .flatMap((candidate) => {
          const record = records.get(candidate.revisionId);
          if (!record) return [];
          const resolved = { ...candidate, record };
          return [
            {
              memory: record,
              excerpt: contextualExcerpt(resolved),
              segmentPath: candidate.path,
              score: candidate.score,
            },
          ];
        })
        .slice(0, topK);
      if (exactResults.length > 0) {
        return {
          query: options.query,
          mode,
          degraded: false,
          results: exactResults,
          timingMs: roundStageMs(performance.now() - started),
          stageTimings: stages,
          ...(temporal ? { temporal } : {}),
        };
      }
    }

    const semanticCandidates =
      mode === 'lexical'
        ? null
        : (async () => {
            if (!this.config.modelsEnabled || !this.store.vectorAvailable) {
              throw new Error('Semantic inference or vector storage is disabled');
            }
            const embedStarted = performance.now();
            const vector = await this.models.embedQuery(options.query);
            stages.embedMs = roundStageMs(performance.now() - embedStarted);
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
            const semanticStarted = performance.now();
            const candidates = this.store.semanticCandidates(
              vector,
              filters,
              this.config.semanticCandidates,
              modelProfileId,
            );
            stages.semanticMs = roundStageMs(performance.now() - semanticStarted);
            return candidates;
          })();

    if (mode !== 'semantic') {
      const lexicalStarted = performance.now();
      addRanking(
        fused,
        this.store.lexicalCandidates(options.query, filters, this.config.lexicalCandidates),
        'lexical',
      );
      stages.lexicalMs = roundStageMs(performance.now() - lexicalStarted);
    }

    if (semanticCandidates) {
      try {
        addRanking(fused, await semanticCandidates, 'semantic');
      } catch (error) {
        degraded = true;
        degradationReason = `Semantic retrieval unavailable: ${String(error)}`;
        this.logger.warn('Search degraded to lexical retrieval', { error: String(error) });
      }
    }

    if (windows.length > 0) {
      addRanking(
        fused,
        this.store.temporalCandidates(
          options.query,
          filters,
          windows,
          this.config.lexicalCandidates,
        ),
        'temporal',
      );
    }
    const queryNormalized = options.query.trim().toLocaleLowerCase();
    const recordsByRevisionId = this.store.getMemoriesByRevisionIds(
      [...fused.values()].map((candidate) => candidate.revisionId),
      options.atTime,
      options.includeSourceMetadata ?? false,
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
        for (const link of this.store.linksFor(
          seed.memoryId,
          options.validAt ?? options.atTime,
          100,
          options.relationSpaceIds,
        )) {
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
      .filter((candidate): candidate is FusedCandidate & { record: MemorySearchRecord } =>
        Boolean(candidate.record),
      )
      .sort((left, right) => right.score.fusedScore - left.score.fusedScore);

    const shouldRerank =
      mode === 'quality' ||
      (mode === 'auto' &&
        this.config.modelsEnabled &&
        autoSearchNeedsReranking(ordered, options.spaceIds !== undefined));
    if (shouldRerank) {
      const rerankSet = ordered.slice(0, rerankCandidateLimit(topK, this.config.rerankCandidates));
      try {
        const rerankStarted = performance.now();
        const scores = await this.models.rerank(
          options.query,
          rerankSet.map((candidate) => rerankDocument(candidate)),
        );
        stages.rerankMs = roundStageMs(performance.now() - rerankStarted);
        rerankSet.forEach((candidate, index) => {
          const score = scores[index];
          if (score === undefined) return;
          candidate.score.rerankerScore = score;
          candidate.score.fusedScore += score * 0.05;
        });
        ordered = ordered.sort((left, right) => right.score.fusedScore - left.score.fusedScore);
        ordered = pruneWeakRerankAlternatives(ordered);
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
      ...(temporal ? { temporal } : {}),
      query: options.query,
      mode,
      degraded,
      results,
      timingMs: Math.round((performance.now() - started) * 100) / 100,
    };
    if (Object.keys(stages).length > 0) response.stageTimings = stages;
    if (degradationReason) response.degradationReason = degradationReason;
    return response;
  }
}
