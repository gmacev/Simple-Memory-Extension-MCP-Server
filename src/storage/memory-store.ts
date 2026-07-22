import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import { MemoryIdentityConflictError } from '../domain/errors.js';
import { contentHash, parseJsonValue, stableStringify } from '../domain/json.js';
import type {
  ContentFeedbackSignal,
  FeedbackActorType,
  FeedbackStatus,
  IndexStatus,
  JsonObject,
  MemoryFeedback,
  MemoryFeedbackInput,
  MemoryFeedbackListFilters,
  MemoryFeedbackListPage,
  MemoryCreateInput,
  MemoryInput,
  MemoryLink,
  MemoryLinkDirection,
  MemoryListFilters,
  MemoryListPage,
  LogicalMemoryResolution,
  MemoryMergeInput,
  MemoryMergeResult,
  MemoryRecord,
  MemoryRevision,
  MemoryState,
  MemoryTraversalEntry,
  MemoryTraversalPathStep,
  SegmentRecord,
  SourceInput,
  StoredFeedbackScope,
  FeedbackSummary,
} from '../domain/types.js';
import { searchableProjection } from '../indexing/projector.js';
import type { Logger } from '../logger.js';
import { applyMigrations, type MigrationStatus } from './migrations/index.js';

type Row = Record<string, unknown>;

export interface RankedSegment {
  segmentId: string;
  memoryId: string;
  revisionId: string;
  text: string;
  path: string;
  rankValue: number;
}

interface CandidateFilters {
  spaceIds?: string[];
  states?: MemoryState[];
  kinds?: string[];
  tags?: string[];
  minConfidence?: number;
  minSalience?: number;
  atTime?: string;
  validAt?: string;
}

interface RevisionRelations {
  sourcesByRevisionId: Map<string, SourceInput[]>;
  tagsByRevisionId: Map<string, string[]>;
}

interface MemoryIdentityInfo {
  logicalKey: string | null;
  canonicalMemoryId: string | null;
  mergedMemoryCount: number;
}

const contentFeedbackSignals = [
  'verified',
  'correct',
  'incorrect',
  'stale',
  'contradicted',
] as const;
const retrievalFeedbackSignals = ['relevant', 'irrelevant', 'helpful', 'not_helpful'] as const;
const contentFeedbackSignalSchema = z.enum(contentFeedbackSignals);
const retrievalFeedbackSignalSchema = z.enum(retrievalFeedbackSignals);
const feedbackScopeSchema = z.enum(['content', 'retrieval']);
const feedbackActorTypeSchema = z.enum(['user', 'agent', 'system', 'external']);
const storedFeedbackScopeSchema = z.enum(['legacy', 'content', 'retrieval']);

const joinedMemoryColumns = `
  m.id AS memory_record_id,
  m.space_id AS memory_space_id,
  m.logical_key AS memory_logical_key,
  m.state AS memory_state,
  m.created_at AS memory_created_at,
  m.updated_at AS memory_updated_at,
  m.current_revision_id AS memory_current_revision_id,
  m.index_status AS memory_index_status`;

function now(): string {
  return new Date().toISOString();
}

function parseObject(value: unknown): JsonObject {
  if (typeof value !== 'string') return {};
  return z.record(z.string(), z.json()).parse(JSON.parse(value));
}

function optionalNumber(value: unknown): number | null {
  return typeof value === 'number' ? value : null;
}

function optionalString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function normalizeTags(tags: string[] | undefined): string[] {
  return [...new Set((tags ?? []).map((tag) => tag.trim().toLowerCase()).filter(Boolean))].sort();
}

function assertTemporalRange(validFrom: string | undefined, validTo: string | undefined): void {
  if (validFrom && validTo && validFrom >= validTo) {
    throw new Error('validFrom must be earlier than validTo');
  }
}

const listCursorSchema = z.object({ updatedAt: z.string(), id: z.string() });
const feedbackCursorSchema = z.object({ createdAt: z.string(), id: z.string() });

function decodeListCursor(cursor: string): z.infer<typeof listCursorSchema> {
  try {
    return listCursorSchema.parse(JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')));
  } catch {
    throw new Error('Invalid memory list cursor');
  }
}

function encodeListCursor(updatedAt: string, id: string): string {
  return Buffer.from(JSON.stringify({ updatedAt, id }), 'utf8').toString('base64url');
}

function normalizeLogicalKey(logicalKey: string | undefined): string | null {
  const normalized = logicalKey?.normalize('NFKC').trim();
  return normalized ? normalized : null;
}

function decodeFeedbackCursor(cursor: string): z.infer<typeof feedbackCursorSchema> {
  try {
    return feedbackCursorSchema.parse(
      JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')),
    );
  } catch {
    throw new Error('Invalid memory feedback cursor');
  }
}

function encodeFeedbackCursor(createdAt: string, id: string): string {
  return Buffer.from(JSON.stringify({ createdAt, id }), 'utf8').toString('base64url');
}

function emptyFeedbackSummary(revisionId: string): FeedbackSummary {
  return {
    revisionId,
    feedbackStatus: 'unreviewed',
    latestSignal: null,
    latestActorType: null,
    latestAt: null,
    contentEventCount: 0,
    retrievalEventCount: 0,
  };
}

function feedbackStatusForSignal(signal: ContentFeedbackSignal): FeedbackStatus {
  if (signal === 'verified') return 'verified';
  if (signal === 'correct') return 'supported';
  return 'needs-review';
}

function canonicalRevisionPayload(
  input: MemoryInput,
  tags: string[],
  metadata: JsonObject,
): JsonObject {
  return {
    confidence: input.confidence ?? null,
    content: input.content,
    expiresAt: input.expiresAt ?? null,
    kind: input.kind ?? null,
    metadata,
    observedAt: input.observedAt ?? null,
    reviewAfter: input.reviewAfter ?? null,
    salience: input.salience ?? null,
    sources: (input.sources ?? []).map((source) => ({
      label: source.label ?? null,
      metadata: source.metadata ?? {},
      observedAt: source.observedAt ?? null,
      type: source.type ?? null,
      uri: source.uri ?? null,
    })),
    tags,
    title: input.title ?? null,
    validFrom: input.validFrom ?? null,
    validTo: input.validTo ?? null,
  };
}

export class MemoryStore {
  private readonly database: Database.Database;
  private readonly migrations: MigrationStatus;
  public readonly vectorAvailable: boolean;

  public constructor(
    config: AppConfig,
    private readonly logger: Logger,
  ) {
    mkdirSync(path.dirname(config.databasePath), { recursive: true });
    this.database = new Database(config.databasePath);
    this.database.pragma('journal_mode = WAL');
    this.database.pragma('foreign_keys = ON');
    this.database.pragma('busy_timeout = 5000');
    let vectorExtensionLoaded = false;
    try {
      sqliteVec.load(this.database);
      vectorExtensionLoaded = true;
    } catch (error) {
      this.logger.warn('sqlite-vec unavailable; semantic search will degrade', {
        error: String(error),
      });
    }
    this.migrations = applyMigrations(this.database, config.databasePath, this.logger);
    this.database
      .prepare(
        `UPDATE index_jobs SET status = 'pending', updated_at = ?
         WHERE status = 'running' AND julianday(updated_at) < julianday('now', '-15 minutes')`,
      )
      .run(now());
    this.ensureDefaultSpace();
    let vectorAvailable = false;
    if (vectorExtensionLoaded) {
      this.database.exec(
        `CREATE VIRTUAL TABLE IF NOT EXISTS memory_vectors USING vec0(
          segment_id TEXT PRIMARY KEY,
          embedding float[${config.embeddingDimension}],
          model_profile_id TEXT PARTITION KEY
        )`,
      );
      vectorAvailable = true;
    }
    this.vectorAvailable = vectorAvailable;
  }

  private ensureDefaultSpace(): void {
    this.database
      .prepare(
        `INSERT OR IGNORE INTO spaces(id, name, description, metadata_json, created_at)
         VALUES ('default', 'Default', 'Default memory isolation space', '{}', ?)`,
      )
      .run(now());
  }

  private allRows(sql: string, ...parameters: unknown[]): Row[] {
    return this.database.prepare<unknown[], Row>(sql).all(...parameters);
  }

  private getRow(sql: string, ...parameters: unknown[]): Row | undefined {
    return this.database.prepare<unknown[], Row>(sql).get(...parameters);
  }

  private requireRow(sql: string, ...parameters: unknown[]): Row {
    const row = this.getRow(sql, ...parameters);
    if (!row) throw new Error('Database invariant failed: expected a row');
    return row;
  }

  public createSpace(input: {
    id?: string;
    name: string;
    description?: string;
    metadata?: JsonObject;
  }): Row {
    const id = input.id?.trim() || randomUUID();
    const createdAt = now();
    this.database
      .prepare(
        `INSERT INTO spaces(id, name, description, metadata_json, created_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.name.trim(),
        input.description ?? null,
        stableStringify(input.metadata ?? {}),
        createdAt,
      );
    return {
      id,
      name: input.name.trim(),
      description: input.description ?? null,
      metadata: input.metadata ?? {},
      createdAt,
    };
  }

  public listSpaces(spaceIds?: string[]): Row[] {
    const clauses: string[] = [];
    const parameters: unknown[] = [];
    if (spaceIds !== undefined) {
      if (spaceIds.length === 0) clauses.push('0 = 1');
      else {
        clauses.push(`id IN (${spaceIds.map(() => '?').join(',')})`);
        parameters.push(...spaceIds);
      }
    }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
    return this.allRows(
      `SELECT * FROM spaces ${where} ORDER BY created_at, id`,
      ...parameters,
    ).map((row) => ({
      id: row.id,
      name: row.name,
      description: row.description,
      metadata: parseObject(row.metadata_json),
      createdAt: row.created_at,
    }));
  }

  private assertSpace(spaceId: string): void {
    if (!this.database.prepare('SELECT 1 FROM spaces WHERE id = ?').get(spaceId)) {
      throw new Error(`Memory space not found: ${spaceId}`);
    }
  }

  public ensureModelProfile(input: {
    provider: string;
    model: string;
    modelRevision: string;
    dimensions: number;
    instructionHash: string;
  }): string {
    const identity = stableStringify({
      dimensions: input.dimensions,
      instructionHash: input.instructionHash,
      model: input.model,
      modelRevision: input.modelRevision,
      provider: input.provider,
    });
    const id = createHash('sha256').update(identity).digest('hex');
    this.database
      .prepare(
        `INSERT OR IGNORE INTO model_profiles(
          id, provider, model, model_revision, dimensions, instruction_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        id,
        input.provider,
        input.model,
        input.modelRevision,
        input.dimensions,
        input.instructionHash,
        now(),
      );
    return id;
  }

  public createMemory(input: MemoryCreateInput, actor: string | null = null): MemoryRecord {
    const spaceId = input.spaceId ?? 'default';
    this.assertSpace(spaceId);
    assertTemporalRange(input.validFrom, input.validTo);
    const logicalKey = normalizeLogicalKey(input.logicalKey);
    const memoryId = randomUUID();
    const revisionId = randomUUID();
    const timestamp = now();
    const tags = normalizeTags(input.tags);
    const metadata = input.metadata ?? {};
    const searchableText = searchableProjection({
      title: input.title ?? null,
      kind: input.kind ?? null,
      content: input.content,
      tags,
      metadata,
      sources: input.sources ?? [],
    });
    const transaction = this.database.transaction((): string => {
      if (input.idempotencyKey) {
        const existing = this.getRow(
          'SELECT id FROM memories WHERE space_id = ? AND idempotency_key = ?',
          spaceId,
          input.idempotencyKey,
        );
        if (existing) return String(existing.id);
      }
      if (logicalKey) {
        const existing = this.getRow(
          'SELECT * FROM memories WHERE space_id = ? AND logical_key = ?',
          spaceId,
          logicalKey,
        );
        if (existing) throw this.identityConflict(existing, logicalKey);
      }
      this.database
        .prepare(
          `INSERT INTO memories(
            id, space_id, state, current_revision_id, created_at, updated_at, index_status,
            idempotency_key, logical_key
          ) VALUES (?, ?, 'active', ?, ?, ?, 'pending', ?, ?)`,
        )
        .run(
          memoryId,
          spaceId,
          revisionId,
          timestamp,
          timestamp,
          input.idempotencyKey ?? null,
          logicalKey,
        );
      this.database
        .prepare(
          `INSERT INTO memory_state_events(id, memory_id, event_number, state, recorded_at)
           VALUES (?, ?, 1, 'active', ?)`,
        )
        .run(randomUUID(), memoryId, timestamp);
      this.insertRevision({
        id: revisionId,
        memoryId,
        revisionNumber: 1,
        parentRevisionId: null,
        input,
        tags,
        metadata,
        actor,
        recordedAt: timestamp,
        searchableText,
      });
      this.createIndexJob(revisionId, timestamp);
      return memoryId;
    });
    return this.getMemory(transaction.immediate());
  }

  public memorySpaceId(memoryId: string): string | null {
    const row = this.getRow('SELECT space_id FROM memories WHERE id = ?', memoryId);
    return row ? String(row.space_id) : null;
  }

  public linkSpaceId(linkId: string): string | null {
    const row = this.getRow('SELECT space_id FROM memory_links WHERE id = ?', linkId);
    return row ? String(row.space_id) : null;
  }

  public reviseMemory(
    memoryId: string,
    input: MemoryInput,
    expectedRevisionId: string,
    actor: string | null = null,
  ): MemoryRecord {
    const current = this.getMemory(memoryId);
    if (current.canonicalMemoryId) {
      throw new Error(
        `Merged memories cannot be revised; revise canonical memory ${current.canonicalMemoryId}`,
      );
    }
    if (current.state === 'deleted') throw new Error('Deleted memories cannot be revised');
    if (input.spaceId && input.spaceId !== current.spaceId) {
      throw new Error('A memory revision cannot move between spaces');
    }
    assertTemporalRange(input.validFrom, input.validTo);
    if (current.currentRevisionId !== expectedRevisionId) {
      throw new Error(
        `Revision conflict: expected ${expectedRevisionId}, current is ${current.currentRevisionId}`,
      );
    }
    const revisionId = randomUUID();
    const timestamp = now();
    const tags = normalizeTags(input.tags);
    const metadata = input.metadata ?? {};
    const searchableText = searchableProjection({
      title: input.title ?? null,
      kind: input.kind ?? null,
      content: input.content,
      tags,
      metadata,
      sources: input.sources ?? [],
    });
    const transaction = this.database.transaction(() => {
      const changed = this.database
        .prepare(
          `UPDATE memories SET current_revision_id = ?, updated_at = ?, index_status = 'pending'
           WHERE id = ? AND current_revision_id = ?
             AND NOT EXISTS (
               SELECT 1 FROM memory_redirect_events redirect
               WHERE redirect.source_memory_id = memories.id
             )`,
        )
        .run(revisionId, timestamp, memoryId, expectedRevisionId);
      if (changed.changes !== 1) {
        const canonicalMemoryId = this.redirectTarget(memoryId);
        if (canonicalMemoryId) {
          throw new Error(
            `Merged memories cannot be revised; revise canonical memory ${canonicalMemoryId}`,
          );
        }
        throw new Error('Revision conflict during update');
      }
      this.insertRevision({
        id: revisionId,
        memoryId,
        revisionNumber: current.revision.revisionNumber + 1,
        parentRevisionId: expectedRevisionId,
        input,
        tags,
        metadata,
        actor,
        recordedAt: timestamp,
        searchableText,
      });
      this.createIndexJob(revisionId, timestamp);
    });
    transaction();
    return this.getMemory(memoryId);
  }

  private insertRevision(args: {
    id: string;
    memoryId: string;
    revisionNumber: number;
    parentRevisionId: string | null;
    input: MemoryInput;
    tags: string[];
    metadata: JsonObject;
    actor: string | null;
    recordedAt: string;
    searchableText: string;
  }): void {
    const { input } = args;
    this.database
      .prepare(
        `INSERT INTO memory_revisions(
          id, memory_id, revision_number, parent_revision_id, title, kind, content_json,
          metadata_json, salience, confidence, observed_at, valid_from, valid_to, expires_at,
          review_after, recorded_at, actor, content_hash, searchable_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        args.id,
        args.memoryId,
        args.revisionNumber,
        args.parentRevisionId,
        input.title ?? null,
        input.kind ?? null,
        stableStringify(input.content),
        stableStringify(args.metadata),
        input.salience ?? null,
        input.confidence ?? null,
        input.observedAt ?? null,
        input.validFrom ?? null,
        input.validTo ?? null,
        input.expiresAt ?? null,
        input.reviewAfter ?? null,
        args.recordedAt,
        args.actor,
        contentHash(canonicalRevisionPayload(input, args.tags, args.metadata)),
        args.searchableText,
      );
    const insertTag = this.database.prepare(
      'INSERT INTO revision_tags(revision_id, tag) VALUES (?, ?)',
    );
    for (const tag of args.tags) insertTag.run(args.id, tag);
    const insertSource = this.database.prepare(
      `INSERT INTO revision_sources(
        id, revision_id, uri, label, type, observed_at, metadata_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    );
    for (const source of input.sources ?? []) {
      insertSource.run(
        randomUUID(),
        args.id,
        source.uri ?? null,
        source.label ?? null,
        source.type ?? null,
        source.observedAt ?? null,
        stableStringify(source.metadata ?? {}),
      );
    }
  }

  private createIndexJob(revisionId: string, timestamp: string): void {
    this.database
      .prepare(
        `INSERT INTO index_jobs(id, revision_id, status, attempts, created_at, updated_at)
         VALUES (?, ?, 'pending', 0, ?, ?)`,
      )
      .run(randomUUID(), revisionId, timestamp, timestamp);
  }

  private sourceFromRow(row: Row): SourceInput {
    const result: SourceInput = { metadata: parseObject(row.metadata_json) };
    if (typeof row.uri === 'string') result.uri = row.uri;
    if (typeof row.label === 'string') result.label = row.label;
    if (typeof row.type === 'string') result.type = row.type;
    if (typeof row.observed_at === 'string') result.observedAt = row.observed_at;
    return result;
  }

  private redirectTarget(memoryId: string, atTime?: string): string | null {
    const row = this.getRow(
      `SELECT canonical_memory_id FROM memory_redirect_events
       WHERE source_memory_id = ? ${atTime ? 'AND created_at <= ?' : ''}
       ORDER BY created_at DESC, id DESC LIMIT 1`,
      memoryId,
      ...(atTime ? [atTime] : []),
    );
    return row ? String(row.canonical_memory_id) : null;
  }

  private resolveCanonicalMemoryId(memoryId: string, atTime?: string): string {
    const visited = new Set<string>();
    let current = memoryId;
    while (true) {
      if (visited.has(current)) throw new Error('Memory redirect cycle detected');
      visited.add(current);
      const target = this.redirectTarget(current, atTime);
      if (!target) return current;
      current = target;
    }
  }

  private identityConflict(memory: Row, logicalKey: string): MemoryIdentityConflictError {
    const matchedMemoryId = String(memory.id);
    const canonicalMemoryId = this.resolveCanonicalMemoryId(matchedMemoryId);
    const canonical = this.requireRow('SELECT * FROM memories WHERE id = ?', canonicalMemoryId);
    return new MemoryIdentityConflictError({
      spaceId: String(memory.space_id),
      logicalKey,
      matchedMemoryId,
      canonicalMemoryId,
      currentRevisionId: String(canonical.current_revision_id),
      state: z.enum(['active', 'archived', 'deleted']).parse(canonical.state),
    });
  }

  private loadRevisionRelations(revisionIds: string[]): RevisionRelations {
    const uniqueIds = [...new Set(revisionIds)];
    const tagsByRevisionId = new Map<string, string[]>();
    const sourcesByRevisionId = new Map<string, SourceInput[]>();
    if (uniqueIds.length === 0) return { sourcesByRevisionId, tagsByRevisionId };

    for (let offset = 0; offset < uniqueIds.length; offset += 500) {
      const batch = uniqueIds.slice(offset, offset + 500);
      const placeholders = batch.map(() => '?').join(',');
      for (const row of this.allRows(
        `SELECT revision_id, tag FROM revision_tags
         WHERE revision_id IN (${placeholders}) ORDER BY revision_id, tag`,
        ...batch,
      )) {
        const revisionId = String(row.revision_id);
        const tags = tagsByRevisionId.get(revisionId) ?? [];
        tags.push(String(row.tag));
        tagsByRevisionId.set(revisionId, tags);
      }
      for (const row of this.allRows(
        `SELECT * FROM revision_sources
         WHERE revision_id IN (${placeholders}) ORDER BY revision_id, id`,
        ...batch,
      )) {
        const revisionId = String(row.revision_id);
        const sources = sourcesByRevisionId.get(revisionId) ?? [];
        sources.push(this.sourceFromRow(row));
        sourcesByRevisionId.set(revisionId, sources);
      }
    }
    return { sourcesByRevisionId, tagsByRevisionId };
  }

  private feedbackFromRow(row: Row): MemoryFeedback {
    const actorType = optionalString(row.actor_type);
    return {
      id: String(row.id),
      memoryId: String(row.memory_id),
      revisionId: optionalString(row.revision_id),
      scope: storedFeedbackScopeSchema.parse(row.scope),
      signal: String(row.signal),
      actorType: actorType === null ? null : feedbackActorTypeSchema.parse(actorType),
      actorId: optionalString(row.actor_id),
      query: optionalString(row.query),
      value: optionalNumber(row.value),
      note: optionalString(row.note),
      metadata: parseObject(row.metadata_json),
      createdAt: String(row.created_at),
    };
  }

  private loadFeedbackSummaries(
    revisionIds: string[],
    atTime?: string,
  ): Map<string, FeedbackSummary> {
    const uniqueIds = [...new Set(revisionIds)];
    const summaries = new Map<string, FeedbackSummary>();
    for (const revisionId of uniqueIds) summaries.set(revisionId, emptyFeedbackSummary(revisionId));
    for (let offset = 0; offset < uniqueIds.length; offset += 500) {
      const batch = uniqueIds.slice(offset, offset + 500);
      if (batch.length === 0) continue;
      const placeholders = batch.map(() => '?').join(',');
      const temporalClause = atTime ? 'AND created_at <= ?' : '';
      const rows = this.allRows(
        `WITH standardized AS (
           SELECT revision_id, scope, signal, actor_type, created_at, id,
                  COUNT(*) OVER (PARTITION BY revision_id, scope) AS event_count,
                  ROW_NUMBER() OVER (
                    PARTITION BY revision_id, scope ORDER BY created_at DESC, id DESC
                  ) AS event_rank
             FROM memory_feedback
            WHERE revision_id IN (${placeholders})
              ${temporalClause}
              AND (
                (scope = 'content' AND signal IN ('verified', 'correct', 'incorrect', 'stale', 'contradicted'))
                OR
                (scope = 'retrieval' AND signal IN ('relevant', 'irrelevant', 'helpful', 'not_helpful'))
              )
         )
         SELECT revision_id, scope, signal, actor_type, created_at, event_count
           FROM standardized WHERE event_rank = 1`,
        ...batch,
        ...(atTime ? [atTime] : []),
      );
      for (const row of rows) {
        const revisionId = String(row.revision_id);
        const summary = summaries.get(revisionId) ?? emptyFeedbackSummary(revisionId);
        if (row.scope === 'content') {
          const signal = contentFeedbackSignalSchema.parse(row.signal);
          const actorType = optionalString(row.actor_type);
          summary.feedbackStatus = feedbackStatusForSignal(signal);
          summary.latestSignal = signal;
          summary.latestActorType =
            actorType === null ? null : feedbackActorTypeSchema.parse(actorType);
          summary.latestAt = String(row.created_at);
          summary.contentEventCount = Number(row.event_count);
        } else if (row.scope === 'retrieval') {
          summary.retrievalEventCount = Number(row.event_count);
        }
        summaries.set(revisionId, summary);
      }
    }
    return summaries;
  }

  private revisionFromRow(row: Row, relations?: RevisionRelations): MemoryRevision {
    const revisionId = String(row.revision_id ?? row.id);
    const tags = relations
      ? (relations.tagsByRevisionId.get(revisionId) ?? [])
      : this.allRows(
          'SELECT tag FROM revision_tags WHERE revision_id = ? ORDER BY tag',
          revisionId,
        ).map((tag) => String(tag.tag));
    const sources = relations
      ? (relations.sourcesByRevisionId.get(revisionId) ?? [])
      : this.allRows(
          'SELECT * FROM revision_sources WHERE revision_id = ? ORDER BY id',
          revisionId,
        ).map((source) => this.sourceFromRow(source));
    return {
      id: revisionId,
      memoryId: String(row.memory_id),
      revisionNumber: Number(row.revision_number),
      parentRevisionId: optionalString(row.parent_revision_id),
      title: optionalString(row.title),
      kind: optionalString(row.kind),
      content: parseJsonValue(String(row.content_json)),
      tags,
      metadata: parseObject(row.metadata_json),
      sources,
      salience: optionalNumber(row.salience),
      confidence: optionalNumber(row.confidence),
      observedAt: optionalString(row.observed_at),
      validFrom: optionalString(row.valid_from),
      validTo: optionalString(row.valid_to),
      expiresAt: optionalString(row.expires_at),
      reviewAfter: optionalString(row.review_after),
      recordedAt: String(row.recorded_at),
      actor: optionalString(row.actor),
      contentHash: String(row.content_hash),
      searchableText: String(row.searchable_text),
    };
  }

  private loadIdentityInfo(memories: Row[], atTime?: string): Map<string, MemoryIdentityInfo> {
    const memoryIds = memories.map((memory) =>
      String(memory.memory_record_id ?? memory.id),
    );
    const identities = new Map<string, MemoryIdentityInfo>();
    for (const memory of memories) {
      const memoryId = String(memory.memory_record_id ?? memory.id);
      identities.set(memoryId, {
        logicalKey: optionalString(memory.memory_logical_key ?? memory.logical_key),
        canonicalMemoryId: null,
        mergedMemoryCount: 0,
      });
    }
    if (memoryIds.length === 0) return identities;

    const placeholders = memoryIds.map(() => '?').join(',');
    const temporalClause = atTime ? 'AND redirect.created_at <= ?' : '';
    const newerTemporalClause = atTime ? 'AND newer.created_at <= ?' : '';
    const latestClause = `NOT EXISTS (
      SELECT 1 FROM memory_redirect_events newer
      WHERE newer.source_memory_id = redirect.source_memory_id
        ${newerTemporalClause}
        AND (
          newer.created_at > redirect.created_at
          OR (newer.created_at = redirect.created_at AND newer.id > redirect.id)
        )
    )`;
    const parametersForTime = atTime ? [atTime, atTime] : [];
    const rows = this.allRows(
      `SELECT redirect.source_memory_id, redirect.canonical_memory_id
       FROM memory_redirect_events redirect
       WHERE (
         redirect.source_memory_id IN (${placeholders})
         OR redirect.canonical_memory_id IN (${placeholders})
       )
       ${temporalClause}
       AND ${latestClause}
       ORDER BY redirect.created_at, redirect.id`,
      ...memoryIds,
      ...memoryIds,
      ...parametersForTime,
    );
    for (const row of rows) {
      const sourceMemoryId = String(row.source_memory_id);
      const canonicalMemoryId = String(row.canonical_memory_id);
      const source = identities.get(sourceMemoryId);
      if (source) source.canonicalMemoryId = canonicalMemoryId;
      const canonical = identities.get(canonicalMemoryId);
      if (canonical) canonical.mergedMemoryCount += 1;
    }
    return identities;
  }

  private memoryFromRows(
    memory: Row,
    revision: Row,
    state = z.enum(['active', 'archived', 'deleted']).parse(memory.state),
    atTime?: string,
  ): MemoryRecord {
    const revisionId = String(revision.revision_id ?? revision.id);
    const identity = this.loadIdentityInfo([memory], atTime).get(String(memory.id));
    return {
      id: String(memory.id),
      spaceId: String(memory.space_id),
      logicalKey: identity?.logicalKey ?? null,
      canonicalMemoryId: identity?.canonicalMemoryId ?? null,
      mergedMemoryCount: identity?.mergedMemoryCount ?? 0,
      state,
      createdAt: String(memory.created_at),
      updatedAt: String(memory.updated_at),
      currentRevisionId: String(memory.current_revision_id),
      indexStatus: z
        .enum(['pending', 'ready', 'lexical-only', 'failed'])
        .parse(memory.index_status),
      revision: this.revisionFromRow(revision),
      feedbackSummary:
        this.loadFeedbackSummaries([revisionId], atTime).get(revisionId) ??
        emptyFeedbackSummary(revisionId),
    };
  }

  private memoryFromJoinedRow(
    row: Row,
    relations: RevisionRelations,
    feedbackSummaries: Map<string, FeedbackSummary>,
    identities: Map<string, MemoryIdentityInfo>,
    state?: MemoryState,
  ): MemoryRecord {
    const memory: Row = {
      id: row.memory_record_id,
      space_id: row.memory_space_id,
      logical_key: row.memory_logical_key,
      state: row.memory_state,
      created_at: row.memory_created_at,
      updated_at: row.memory_updated_at,
      current_revision_id: row.memory_current_revision_id,
      index_status: row.memory_index_status,
    };
    const resolvedState = state ?? z.enum(['active', 'archived', 'deleted']).parse(memory.state);
    const revisionId = String(row.revision_id);
    const identity = identities.get(String(memory.id));
    return {
      id: String(memory.id),
      spaceId: String(memory.space_id),
      logicalKey: identity?.logicalKey ?? null,
      canonicalMemoryId: identity?.canonicalMemoryId ?? null,
      mergedMemoryCount: identity?.mergedMemoryCount ?? 0,
      state: resolvedState,
      createdAt: String(memory.created_at),
      updatedAt: String(memory.updated_at),
      currentRevisionId: String(memory.current_revision_id),
      indexStatus: z
        .enum(['pending', 'ready', 'lexical-only', 'failed'])
        .parse(memory.index_status),
      revision: this.revisionFromRow(row, relations),
      feedbackSummary: feedbackSummaries.get(revisionId) ?? emptyFeedbackSummary(revisionId),
    };
  }

  public getMemory(
    memoryId: string,
    options: { revisionId?: string; atTime?: string } = {},
  ): MemoryRecord {
    const memory = this.getRow('SELECT * FROM memories WHERE id = ?', memoryId);
    if (!memory) throw new Error(`Memory not found: ${memoryId}`);
    let revision: Row | undefined;
    if (options.revisionId) {
      revision = this.getRow(
        'SELECT *, id AS revision_id FROM memory_revisions WHERE id = ? AND memory_id = ?',
        options.revisionId,
        memoryId,
      );
    } else if (options.atTime) {
      revision = this.getRow(
        `SELECT *, id AS revision_id FROM memory_revisions
           WHERE memory_id = ? AND recorded_at <= ?
           ORDER BY revision_number DESC LIMIT 1`,
        memoryId,
        options.atTime,
      );
    } else {
      revision = this.getRow(
        'SELECT *, id AS revision_id FROM memory_revisions WHERE id = ?',
        memory.current_revision_id,
      );
    }
    if (!revision) throw new Error('No memory revision exists at the requested point in time');
    let state: MemoryState | undefined;
    if (options.atTime) {
      const stateRow = this.getRow(
        `SELECT state FROM memory_state_events
         WHERE memory_id = ? AND recorded_at <= ?
         ORDER BY recorded_at DESC, event_number DESC LIMIT 1`,
        memoryId,
        options.atTime,
      );
      if (!stateRow) throw new Error('No memory state exists at the requested point in time');
      state = z.enum(['active', 'archived', 'deleted']).parse(stateRow.state);
    }
    return this.memoryFromRows(memory, revision, state, options.atTime);
  }

  public getMemoryByLogicalKey(
    spaceId: string,
    logicalKeyInput: string,
    atTime?: string,
  ): LogicalMemoryResolution {
    const logicalKey = normalizeLogicalKey(logicalKeyInput);
    if (!logicalKey) throw new Error('logicalKey must contain non-whitespace text');
    const matched = this.getRow(
      'SELECT * FROM memories WHERE space_id = ? AND logical_key = ?',
      spaceId,
      logicalKey,
    );
    if (!matched) {
      throw new Error(`Logical memory not found in space ${spaceId}: ${logicalKey}`);
    }
    const matchedMemoryId = String(matched.id);
    const canonicalMemoryId = this.resolveCanonicalMemoryId(matchedMemoryId, atTime);
    return {
      logicalKey,
      matchedMemoryId,
      redirected: matchedMemoryId !== canonicalMemoryId,
      memory: this.getMemory(canonicalMemoryId, atTime ? { atTime } : {}),
    };
  }

  public getMemoriesByRevisionIds(
    revisionIds: string[],
    atTime?: string,
  ): Map<string, MemoryRecord> {
    const uniqueRevisionIds = [...new Set(revisionIds)];
    const records = new Map<string, MemoryRecord>();
    if (uniqueRevisionIds.length === 0) return records;

    const revisionPlaceholders = uniqueRevisionIds.map(() => '?').join(',');
    const rows = this.allRows(
      `SELECT ${joinedMemoryColumns}, r.*, r.id AS revision_id
       FROM memory_revisions r
       JOIN memories m ON m.id = r.memory_id
       WHERE r.id IN (${revisionPlaceholders})`,
      ...uniqueRevisionIds,
    );
    const relations = this.loadRevisionRelations(
      rows.map((row) => String(row.revision_id)),
    );
    const feedbackSummaries = this.loadFeedbackSummaries(
      rows.map((row) => String(row.revision_id)),
      atTime,
    );
    const identities = this.loadIdentityInfo(rows, atTime);
    const statesByMemoryId = new Map<string, MemoryState>();
    if (atTime && rows.length > 0) {
      const memoryIds = [...new Set(rows.map((row) => String(row.memory_record_id)))];
      const memoryPlaceholders = memoryIds.map(() => '?').join(',');
      for (const row of this.allRows(
        `SELECT event.memory_id, event.state
         FROM memory_state_events event
         JOIN (
           SELECT memory_id, MAX(event_number) AS event_number
           FROM memory_state_events
           WHERE memory_id IN (${memoryPlaceholders}) AND recorded_at <= ?
           GROUP BY memory_id
         ) latest
           ON latest.memory_id = event.memory_id AND latest.event_number = event.event_number`,
        ...memoryIds,
        atTime,
      )) {
        statesByMemoryId.set(
          String(row.memory_id),
          z.enum(['active', 'archived', 'deleted']).parse(row.state),
        );
      }
    }

    for (const row of rows) {
      const memoryId = String(row.memory_record_id);
      const historicalState = atTime ? statesByMemoryId.get(memoryId) : undefined;
      if (atTime && !historicalState) continue;
      const record = this.memoryFromJoinedRow(
        row,
        relations,
        feedbackSummaries,
        identities,
        historicalState,
      );
      records.set(record.revision.id, record);
    }
    return records;
  }

  private getMemoriesByIdsAtTime(memoryIds: string[], atTime: string): Map<string, MemoryRecord> {
    const uniqueMemoryIds = [...new Set(memoryIds)];
    const records = new Map<string, MemoryRecord>();
    if (uniqueMemoryIds.length === 0) return records;

    const placeholders = uniqueMemoryIds.map(() => '?').join(',');
    const rows = this.allRows(
      `SELECT ${joinedMemoryColumns}, r.*, r.id AS revision_id
       FROM memories m
       JOIN (
         SELECT memory_id, MAX(revision_number) AS revision_number
         FROM memory_revisions
         WHERE memory_id IN (${placeholders}) AND recorded_at <= ?
         GROUP BY memory_id
       ) latest ON latest.memory_id = m.id
       JOIN memory_revisions r
         ON r.memory_id = latest.memory_id AND r.revision_number = latest.revision_number`,
      ...uniqueMemoryIds,
      atTime,
    );
    const relations = this.loadRevisionRelations(
      rows.map((row) => String(row.revision_id)),
    );
    const feedbackSummaries = this.loadFeedbackSummaries(
      rows.map((row) => String(row.revision_id)),
      atTime,
    );
    const identities = this.loadIdentityInfo(rows, atTime);
    const statesByMemoryId = new Map<string, MemoryState>();
    for (const row of this.allRows(
      `SELECT event.memory_id, event.state
       FROM memory_state_events event
       JOIN (
         SELECT memory_id, MAX(event_number) AS event_number
         FROM memory_state_events
         WHERE memory_id IN (${placeholders}) AND recorded_at <= ?
         GROUP BY memory_id
       ) latest
         ON latest.memory_id = event.memory_id AND latest.event_number = event.event_number`,
      ...uniqueMemoryIds,
      atTime,
    )) {
      statesByMemoryId.set(
        String(row.memory_id),
        z.enum(['active', 'archived', 'deleted']).parse(row.state),
      );
    }
    for (const row of rows) {
      const memoryId = String(row.memory_record_id);
      const state = statesByMemoryId.get(memoryId);
      if (!state) continue;
      records.set(
        memoryId,
        this.memoryFromJoinedRow(row, relations, feedbackSummaries, identities, state),
      );
    }
    return records;
  }

  public getHistory(memoryId: string): MemoryRevision[] {
    if (!this.database.prepare('SELECT 1 FROM memories WHERE id = ?').get(memoryId)) {
      throw new Error(`Memory not found: ${memoryId}`);
    }
    const rows = this.allRows(
      `SELECT *, id AS revision_id FROM memory_revisions
           WHERE memory_id = ? ORDER BY revision_number DESC`,
      memoryId,
    );
    const relations = this.loadRevisionRelations(rows.map((row) => String(row.revision_id)));
    return rows.map((row) => this.revisionFromRow(row, relations));
  }

  public listMemories(filters: MemoryListFilters = {}): MemoryListPage {
    const clauses: string[] = ['1=1'];
    const parameters: unknown[] = [];
    if (filters.spaceId) {
      clauses.push('m.space_id = ?');
      parameters.push(filters.spaceId);
    }
    if (filters.spaceIds !== undefined) {
      if (filters.spaceIds.length === 0) clauses.push('0 = 1');
      else {
        clauses.push(`m.space_id IN (${filters.spaceIds.map(() => '?').join(',')})`);
        parameters.push(...filters.spaceIds);
      }
    }
    if (filters.state) {
      clauses.push('m.state = ?');
      parameters.push(filters.state);
    } else {
      clauses.push("m.state = 'active'");
    }
    if (filters.kind) {
      clauses.push('r.kind = ?');
      parameters.push(filters.kind);
    }
    if (filters.cursor) {
      const cursor = decodeListCursor(filters.cursor);
      clauses.push('(m.updated_at < ? OR (m.updated_at = ? AND m.id < ?))');
      parameters.push(cursor.updatedAt, cursor.updatedAt, cursor.id);
    }
    for (const tag of normalizeTags(filters.tags)) {
      clauses.push(
        'EXISTS (SELECT 1 FROM revision_tags rt WHERE rt.revision_id = r.id AND rt.tag = ?)',
      );
      parameters.push(tag);
    }
    if (filters.feedbackStatus) {
      const latestContentSignal = `(SELECT feedback.signal FROM memory_feedback feedback
        WHERE feedback.revision_id = r.id
          AND feedback.scope = 'content'
          AND feedback.signal IN ('verified', 'correct', 'incorrect', 'stale', 'contradicted')
        ORDER BY feedback.created_at DESC, feedback.id DESC LIMIT 1)`;
      if (filters.feedbackStatus === 'unreviewed') {
        clauses.push(`${latestContentSignal} IS NULL`);
      } else if (filters.feedbackStatus === 'verified') {
        clauses.push(`${latestContentSignal} = 'verified'`);
      } else if (filters.feedbackStatus === 'supported') {
        clauses.push(`${latestContentSignal} = 'correct'`);
      } else {
        clauses.push(`${latestContentSignal} IN ('incorrect', 'stale', 'contradicted')`);
      }
    }
    const limit = Math.min(filters.limit ?? 50, 200);
    parameters.push(limit + 1);
    const rows = this.allRows(
      `SELECT ${joinedMemoryColumns}, r.*, r.id AS revision_id
         FROM memories m JOIN memory_revisions r ON r.id = m.current_revision_id
         WHERE ${clauses.join(' AND ')}
         ORDER BY m.updated_at DESC, m.id DESC LIMIT ?`,
      ...parameters,
    );
    const hasMore = rows.length > limit;
    const pageRows = rows.slice(0, limit);
    const relations = this.loadRevisionRelations(
      pageRows.map((row) => String(row.revision_id)),
    );
    const feedbackSummaries = this.loadFeedbackSummaries(
      pageRows.map((row) => String(row.revision_id)),
    );
    const identities = this.loadIdentityInfo(pageRows);
    const items = pageRows.map((row) =>
      this.memoryFromJoinedRow(row, relations, feedbackSummaries, identities),
    );
    const last = pageRows.at(-1);
    return {
      items,
      nextCursor:
        hasMore && last
          ? encodeListCursor(String(last.memory_updated_at), String(last.memory_record_id))
          : null,
    };
  }

  public setState(memoryId: string, state: MemoryState): MemoryRecord {
    const current = this.getMemory(memoryId);
    if (state === 'active' && current.canonicalMemoryId) {
      throw new Error(
        `Merged memories cannot be restored; use canonical memory ${current.canonicalMemoryId}`,
      );
    }
    if (current.state === 'deleted' && state !== 'deleted') {
      throw new Error('Deleted memories cannot be restored or archived');
    }
    if (current.state === state) return current;
    const latestEvent = this.requireRow(
      `SELECT recorded_at FROM memory_state_events
       WHERE memory_id = ? ORDER BY event_number DESC LIMIT 1`,
      memoryId,
    );
    const previousTimestamp = Date.parse(String(latestEvent.recorded_at));
    const timestamp = new Date(Math.max(Date.now(), previousTimestamp + 1)).toISOString();
    const transaction = this.database.transaction(() => {
      const redirectGuard =
        state === 'active'
          ? `AND NOT EXISTS (
               SELECT 1 FROM memory_redirect_events redirect
               WHERE redirect.source_memory_id = memories.id
             )`
          : '';
      const result = this.database
        .prepare(
          `UPDATE memories SET state = ?, updated_at = ?
           WHERE id = ? AND state = ? ${redirectGuard}`,
        )
        .run(state, timestamp, memoryId, current.state);
      if (result.changes !== 1) {
        const canonicalMemoryId = state === 'active' ? this.redirectTarget(memoryId) : null;
        if (canonicalMemoryId) {
          throw new Error(
            `Merged memories cannot be restored; use canonical memory ${canonicalMemoryId}`,
          );
        }
        throw new Error('Memory state changed concurrently');
      }
      this.database
        .prepare(
          `INSERT INTO memory_state_events(id, memory_id, event_number, state, recorded_at)
           SELECT ?, ?, COALESCE(MAX(event_number), 0) + 1, ?, ?
           FROM memory_state_events WHERE memory_id = ?`,
        )
        .run(randomUUID(), memoryId, state, timestamp, memoryId);
    });
    transaction();
    return this.getMemory(memoryId);
  }

  public markIndexStatus(revisionId: string, status: IndexStatus, error?: string): void {
    const timestamp = now();
    const transaction = this.database.transaction(() => {
      this.database
        .prepare(
          `UPDATE memories SET index_status = ?, updated_at = updated_at
           WHERE current_revision_id = ?`,
        )
        .run(status, revisionId);
      this.database
        .prepare(
          `UPDATE index_jobs SET status = ?, error = ?, updated_at = ?
           WHERE revision_id = ? AND status IN ('pending', 'running')`,
        )
        .run(
          status === 'ready' || status === 'lexical-only' ? 'complete' : 'failed',
          error ?? null,
          timestamp,
          revisionId,
        );
    });
    transaction();
  }

  public indexSegments(
    revisionId: string,
    segments: SegmentRecord[],
    title: string | null,
    tags: string[],
  ): void {
    const transaction = this.database.transaction(() => {
      if (this.vectorAvailable) {
        const priorSegmentIds = this.allRows(
          'SELECT id FROM memory_segments WHERE revision_id = ?',
          revisionId,
        ).map((row) => String(row.id));
        const removeVector = this.database.prepare(
          'DELETE FROM memory_vectors WHERE segment_id = ?',
        );
        for (const segmentId of priorSegmentIds) removeVector.run(segmentId);
      }
      this.database.prepare('DELETE FROM memory_fts WHERE revision_id = ?').run(revisionId);
      this.database.prepare('DELETE FROM memory_segments WHERE revision_id = ?').run(revisionId);
      const insertSegment = this.database.prepare(
        `INSERT INTO memory_segments(
          id, memory_id, revision_id, space_id, ordinal, path, text, token_count, content_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      );
      const insertFts = this.database.prepare(
        `INSERT INTO memory_fts(segment_id, memory_id, revision_id, space_id, title, text, tags)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      );
      for (const segment of segments) {
        insertSegment.run(
          segment.id,
          segment.memoryId,
          segment.revisionId,
          segment.spaceId,
          segment.ordinal,
          segment.path,
          segment.text,
          segment.tokenCount,
          segment.contentHash,
        );
        insertFts.run(
          segment.id,
          segment.memoryId,
          segment.revisionId,
          segment.spaceId,
          title ?? '',
          segment.text,
          tags.join(' '),
        );
      }
    });
    transaction();
  }

  public indexVectors(
    segments: SegmentRecord[],
    vectors: number[][],
    modelProfileId: string,
  ): void {
    if (!this.vectorAvailable) throw new Error('sqlite-vec is unavailable');
    if (segments.length !== vectors.length) throw new Error('Segment/vector count mismatch');
    const transaction = this.database.transaction(() => {
      const remove = this.database.prepare('DELETE FROM memory_vectors WHERE segment_id = ?');
      const segmentExists = this.database.prepare(
        'SELECT 1 FROM memory_segments WHERE id = ? AND revision_id = ?',
      );
      const insert = this.database.prepare(
        `INSERT INTO memory_vectors(segment_id, embedding, model_profile_id)
         VALUES (?, ?, ?)`,
      );
      const markProfile = this.database.prepare(
        'UPDATE memory_segments SET model_profile_id = ? WHERE id = ?',
      );
      for (let index = 0; index < segments.length; index += 1) {
        const segment = segments[index];
        const vector = vectors[index];
        if (!segment || !vector) throw new Error('Missing segment or vector');
        if (!segmentExists.get(segment.id, segment.revisionId)) {
          throw new Error('Memory revision was deleted while semantic indexing was running');
        }
        remove.run(segment.id);
        insert.run(segment.id, Buffer.from(new Float32Array(vector).buffer), modelProfileId);
        markProfile.run(modelProfileId, segment.id);
      }
    });
    transaction();
  }

  public markRevisionIndexRunning(revisionId: string): void {
    this.database
      .prepare(
        `UPDATE index_jobs
         SET status = 'running', attempts = attempts + 1, error = NULL, updated_at = ?
         WHERE revision_id = ? AND status = 'pending'`,
      )
      .run(now(), revisionId);
  }

  public claimNextPendingRevision(): string | null {
    const transaction = this.database.transaction(() => {
      const job = this.getRow(
        `SELECT id, revision_id FROM index_jobs
         WHERE status = 'pending' ORDER BY created_at, id LIMIT 1`,
      );
      if (!job) return null;
      const claimed = this.database
        .prepare(
          `UPDATE index_jobs
           SET status = 'running', attempts = attempts + 1, error = NULL, updated_at = ?
           WHERE id = ? AND status = 'pending'`,
        )
        .run(now(), job.id);
      return claimed.changes === 1 ? String(job.revision_id) : null;
    });
    return transaction();
  }

  public revisionForIndex(revisionId: string): MemoryRecord {
    const row = this.getRow('SELECT memory_id FROM memory_revisions WHERE id = ?', revisionId);
    if (!row) throw new Error(`Revision not found: ${revisionId}`);
    return this.getMemory(String(row.memory_id), { revisionId });
  }

  private candidateClauses(
    filters: CandidateFilters,
    revisionAlias = 'r',
  ): {
    sql: string;
    parameters: unknown[];
  } {
    const clauses: string[] = [];
    const parameters: unknown[] = [];
    const states = filters.states && filters.states.length > 0 ? filters.states : ['active'];
    const statePlaceholders = states.map(() => '?').join(',');
    if (filters.atTime) {
      clauses.push(
        `(SELECT state FROM memory_state_events state_event
          WHERE state_event.memory_id = m.id AND state_event.recorded_at <= ?
          ORDER BY state_event.recorded_at DESC, state_event.event_number DESC LIMIT 1)
          IN (${statePlaceholders})`,
      );
      parameters.push(filters.atTime, ...states);
    } else {
      clauses.push(`m.state IN (${statePlaceholders})`);
      parameters.push(...states);
    }
    if (filters.atTime) {
      clauses.push(`${revisionAlias}.recorded_at <= ?`);
      parameters.push(filters.atTime);
      clauses.push(
        `${revisionAlias}.revision_number = (SELECT MAX(r2.revision_number) FROM memory_revisions r2 WHERE r2.memory_id = m.id AND r2.recorded_at <= ?)`,
      );
      parameters.push(filters.atTime);
    } else {
      clauses.push(`m.current_revision_id = ${revisionAlias}.id`);
    }
    const validityPoint = filters.validAt ?? (filters.atTime ? undefined : now());
    if (validityPoint) {
      clauses.push(`(${revisionAlias}.valid_from IS NULL OR ${revisionAlias}.valid_from <= ?)`);
      clauses.push(`(${revisionAlias}.valid_to IS NULL OR ${revisionAlias}.valid_to > ?)`);
      parameters.push(validityPoint, validityPoint);
    }
    const expiryPoint = filters.atTime ?? now();
    clauses.push(`(${revisionAlias}.expires_at IS NULL OR ${revisionAlias}.expires_at > ?)`);
    parameters.push(expiryPoint);
    if (filters.spaceIds !== undefined) {
      if (filters.spaceIds.length === 0) clauses.push('0 = 1');
      else {
        clauses.push(`m.space_id IN (${filters.spaceIds.map(() => '?').join(',')})`);
        parameters.push(...filters.spaceIds);
      }
    }
    if (filters.kinds && filters.kinds.length > 0) {
      clauses.push(`${revisionAlias}.kind IN (${filters.kinds.map(() => '?').join(',')})`);
      parameters.push(...filters.kinds);
    }
    if (filters.minConfidence !== undefined) {
      clauses.push(`${revisionAlias}.confidence >= ?`);
      parameters.push(filters.minConfidence);
    }
    if (filters.minSalience !== undefined) {
      clauses.push(`${revisionAlias}.salience >= ?`);
      parameters.push(filters.minSalience);
    }
    for (const tag of normalizeTags(filters.tags)) {
      clauses.push(
        `EXISTS (SELECT 1 FROM revision_tags rt WHERE rt.revision_id = ${revisionAlias}.id AND rt.tag = ?)`,
      );
      parameters.push(tag);
    }
    return { sql: clauses.join(' AND '), parameters };
  }

  public exactCandidates(query: string, filters: CandidateFilters, limit: number): RankedSegment[] {
    const normalized = query.normalize('NFKC').trim();
    if (!normalized) return [];
    const escaped = normalized
      .replaceAll('\\', '\\\\')
      .replaceAll('%', '\\%')
      .replaceAll('_', '\\_');
    const candidates = this.candidateClauses(filters);
    const rows = this.allRows(
      `SELECT s.id AS segment_id, s.memory_id, s.revision_id, s.text, s.path,
              CASE
                WHEN m.logical_key = ? THEN 0
                WHEN m.id = ? THEN 1
                WHEN lower(r.title) = lower(?) THEN 2
                ELSE 3
              END AS rank_value
       FROM memories m
       JOIN memory_revisions r ON r.memory_id = m.id
       JOIN memory_segments s ON s.revision_id = r.id
         AND s.ordinal = (
           SELECT MIN(s2.ordinal) FROM memory_segments s2 WHERE s2.revision_id = r.id
         )
         WHERE ${candidates.sql}
         AND (
           m.logical_key = ? OR m.id = ? OR lower(r.title) = lower(?)
           OR lower(r.title) LIKE lower(?) ESCAPE '\\'
         )
       ORDER BY rank_value, r.recorded_at DESC
       LIMIT ?`,
      normalized,
      normalized,
      normalized,
      ...candidates.parameters,
      normalized,
      normalized,
      normalized,
      `%${escaped}%`,
      limit,
    );
    return rows.map((row) => ({
      segmentId: String(row.segment_id),
      memoryId: String(row.memory_id),
      revisionId: String(row.revision_id),
      text: String(row.text),
      path: String(row.path),
      rankValue: Number(row.rank_value),
    }));
  }

  public lexicalCandidates(
    query: string,
    filters: CandidateFilters,
    limit: number,
  ): RankedSegment[] {
    const terms = query.normalize('NFKC').match(/[\p{L}\p{N}_-]+/gu) ?? [];
    if (terms.length === 0) return [];
    const match = [...new Set(terms)].map((term) => `"${term.replaceAll('"', '""')}"`).join(' OR ');
    const candidates = this.candidateClauses(filters);
    const rows = this.allRows(
      `SELECT f.segment_id, f.memory_id, f.revision_id, s.text, s.path,
                bm25(memory_fts, 0.0, 0.0, 0.0, 0.0, 2.0, 1.0, 0.5) AS rank_value
         FROM memory_fts f
         JOIN memory_segments s ON s.id = f.segment_id
         JOIN memories m ON m.id = f.memory_id
         JOIN memory_revisions r ON r.id = f.revision_id
         WHERE memory_fts MATCH ? AND ${candidates.sql}
         ORDER BY rank_value LIMIT ?`,
      match,
      ...candidates.parameters,
      limit,
    );
    return rows.map((row) => ({
      segmentId: String(row.segment_id),
      memoryId: String(row.memory_id),
      revisionId: String(row.revision_id),
      text: String(row.text),
      path: String(row.path),
      rankValue: Number(row.rank_value),
    }));
  }

  public semanticCandidates(
    vector: number[],
    filters: CandidateFilters,
    limit: number,
    modelProfileId: string,
  ): RankedSegment[] {
    if (!this.vectorAvailable) return [];
    const broadLimit = Math.min(Math.max(limit * 5, limit), 500);
    const nearest = this.allRows(
      `SELECT segment_id, distance AS rank_value FROM memory_vectors
         WHERE embedding MATCH ? AND k = ? AND model_profile_id = ? ORDER BY distance`,
      Buffer.from(new Float32Array(vector).buffer),
      broadLimit,
      modelProfileId,
    );
    if (nearest.length === 0) return [];
    const ids = nearest.map((row) => String(row.segment_id));
    const distance = new Map(ids.map((id, index) => [id, Number(nearest[index]?.rank_value ?? 0)]));
    const candidates = this.candidateClauses(filters);
    const rows = this.allRows(
      `SELECT s.id AS segment_id, s.memory_id, s.revision_id, s.text, s.path
         FROM memory_segments s
         JOIN memories m ON m.id = s.memory_id
         JOIN memory_revisions r ON r.id = s.revision_id
         WHERE s.id IN (${ids.map(() => '?').join(',')}) AND ${candidates.sql}`,
      ...ids,
      ...candidates.parameters,
    );
    return rows
      .map((row) => ({
        segmentId: String(row.segment_id),
        memoryId: String(row.memory_id),
        revisionId: String(row.revision_id),
        text: String(row.text),
        path: String(row.path),
        rankValue: distance.get(String(row.segment_id)) ?? Number.POSITIVE_INFINITY,
      }))
      .sort((left, right) => left.rankValue - right.rankValue)
      .slice(0, limit);
  }

  private mergeResult(operationId: string): MemoryMergeResult {
    const operation = this.requireRow(
      'SELECT * FROM memory_merge_operations WHERE id = ?',
      operationId,
    );
    const mergedMemoryIds = this.allRows(
      `SELECT duplicate_memory_id FROM memory_merge_members
       WHERE operation_id = ? ORDER BY duplicate_memory_id`,
      operationId,
    ).map((row) => String(row.duplicate_memory_id));
    const redirectedMemoryIds = this.allRows(
      `SELECT source_memory_id FROM memory_redirect_events
       WHERE operation_id = ? ORDER BY source_memory_id`,
      operationId,
    ).map((row) => String(row.source_memory_id));
    return {
      operationId,
      canonicalMemory: this.getMemory(String(operation.canonical_memory_id)),
      mergedMemoryIds,
      redirectedMemoryIds,
      actorId: optionalString(operation.actor_id),
      reason: optionalString(operation.reason),
      createdAt: String(operation.created_at),
    };
  }

  public mergeMemories(input: MemoryMergeInput): MemoryMergeResult {
    if (input.duplicates.length === 0) throw new Error('At least one duplicate memory is required');
    const actorId = input.actorId?.trim() || null;
    const reason = input.reason?.trim() || null;
    const idempotencyKey = input.idempotencyKey?.trim() || null;
    const metadata = input.metadata ?? {};
    const duplicateRevisions = new Map<string, string>();
    for (const duplicate of input.duplicates) {
      const existingRevision = duplicateRevisions.get(duplicate.memoryId);
      if (existingRevision && existingRevision !== duplicate.expectedRevisionId) {
        throw new Error(`Duplicate memory has conflicting expected revisions: ${duplicate.memoryId}`);
      }
      duplicateRevisions.set(duplicate.memoryId, duplicate.expectedRevisionId);
    }
    const duplicates = [...duplicateRevisions]
      .map(([memoryId, expectedRevisionId]) => ({ memoryId, expectedRevisionId }))
      .sort((left, right) => left.memoryId.localeCompare(right.memoryId));
    const canonicalRequest = {
      actorId,
      canonicalMemoryId: input.canonicalMemoryId,
      duplicates,
      expectedCanonicalRevisionId: input.expectedCanonicalRevisionId,
      metadata,
      reason,
    };
    const requestHash = createHash('sha256')
      .update(stableStringify(canonicalRequest), 'utf8')
      .digest('hex');

    const transaction = this.database.transaction((): string => {
      if (idempotencyKey) {
        const existing = this.getRow(
          `SELECT id, request_hash FROM memory_merge_operations
           WHERE canonical_memory_id = ? AND idempotency_key = ?`,
          input.canonicalMemoryId,
          idempotencyKey,
        );
        if (existing) {
          if (String(existing.request_hash) !== requestHash) {
            throw new Error(`Memory merge idempotency conflict for key: ${idempotencyKey}`);
          }
          return String(existing.id);
        }
      }

      const canonical = this.getRow('SELECT * FROM memories WHERE id = ?', input.canonicalMemoryId);
      if (!canonical) throw new Error(`Memory not found: ${input.canonicalMemoryId}`);
      if (canonical.state !== 'active') throw new Error('Canonical memory must be active');
      if (String(canonical.current_revision_id) !== input.expectedCanonicalRevisionId) {
        throw new Error(
          `Revision conflict: expected ${input.expectedCanonicalRevisionId}, current is ${String(canonical.current_revision_id)}`,
        );
      }
      const canonicalRedirect = this.redirectTarget(input.canonicalMemoryId);
      if (canonicalRedirect) {
        throw new Error(
          `Canonical memory is already merged; use canonical memory ${canonicalRedirect}`,
        );
      }

      const duplicateRows = new Map<string, Row>();
      for (const duplicate of duplicates) {
        if (duplicate.memoryId === input.canonicalMemoryId) {
          throw new Error('A canonical memory cannot be merged into itself');
        }
        const memory = this.getRow('SELECT * FROM memories WHERE id = ?', duplicate.memoryId);
        if (!memory) throw new Error(`Memory not found: ${duplicate.memoryId}`);
        if (String(memory.space_id) !== String(canonical.space_id)) {
          throw new Error('Merged memories must belong to the same space');
        }
        if (String(memory.current_revision_id) !== duplicate.expectedRevisionId) {
          throw new Error(
            `Revision conflict for ${duplicate.memoryId}: expected ${duplicate.expectedRevisionId}, current is ${String(memory.current_revision_id)}`,
          );
        }
        const existingRedirect = this.redirectTarget(duplicate.memoryId);
        if (existingRedirect) {
          throw new Error(
            `Memory ${duplicate.memoryId} is already merged into ${existingRedirect}`,
          );
        }
        duplicateRows.set(duplicate.memoryId, memory);
      }

      const duplicateIds = [...duplicateRows.keys()];
      const duplicatePlaceholders = duplicateIds.map(() => '?').join(',');
      const inheritedSourceIds =
        duplicateIds.length === 0
          ? []
          : this.allRows(
              `SELECT redirect.source_memory_id
               FROM memory_redirect_events redirect
               WHERE redirect.canonical_memory_id IN (${duplicatePlaceholders})
                 AND NOT EXISTS (
                   SELECT 1 FROM memory_redirect_events newer
                   WHERE newer.source_memory_id = redirect.source_memory_id
                     AND (
                       newer.created_at > redirect.created_at
                       OR (newer.created_at = redirect.created_at AND newer.id > redirect.id)
                     )
                 )
               ORDER BY redirect.source_memory_id`,
              ...duplicateIds,
            ).map((row) => String(row.source_memory_id));

      const latestState = this.getRow(
        `SELECT MAX(recorded_at) AS recorded_at FROM memory_state_events
         WHERE memory_id IN (${duplicatePlaceholders})`,
        ...duplicateIds,
      );
      const previousTimestamp = latestState?.recorded_at
        ? Date.parse(String(latestState.recorded_at))
        : 0;
      const timestamp = new Date(Math.max(Date.now(), previousTimestamp + 1)).toISOString();
      const operationId = randomUUID();
      this.database
        .prepare(
          `INSERT INTO memory_merge_operations(
             id, space_id, canonical_memory_id, canonical_revision_id, actor_id, reason,
             metadata_json, idempotency_key, request_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          operationId,
          canonical.space_id,
          input.canonicalMemoryId,
          input.expectedCanonicalRevisionId,
          actorId,
          reason,
          stableStringify(metadata),
          idempotencyKey,
          requestHash,
          timestamp,
        );
      const insertMember = this.database.prepare(
        `INSERT INTO memory_merge_members(
           operation_id, duplicate_memory_id, duplicate_revision_id
         ) VALUES (?, ?, ?)`,
      );
      const insertRedirect = this.database.prepare(
        `INSERT INTO memory_redirect_events(
           id, source_memory_id, canonical_memory_id, operation_id, direct, created_at
         ) VALUES (?, ?, ?, ?, ?, ?)`,
      );
      const archive = this.database.prepare(
        `UPDATE memories SET state = 'archived', updated_at = ?
         WHERE id = ? AND state = 'active' AND current_revision_id = ?`,
      );
      const insertStateEvent = this.database.prepare(
        `INSERT INTO memory_state_events(id, memory_id, event_number, state, recorded_at)
         SELECT ?, ?, COALESCE(MAX(event_number), 0) + 1, 'archived', ?
         FROM memory_state_events WHERE memory_id = ?`,
      );

      for (const duplicate of duplicates) {
        insertMember.run(operationId, duplicate.memoryId, duplicate.expectedRevisionId);
        insertRedirect.run(
          randomUUID(),
          duplicate.memoryId,
          input.canonicalMemoryId,
          operationId,
          1,
          timestamp,
        );
        const memory = duplicateRows.get(duplicate.memoryId);
        if (memory?.state === 'active') {
          const archived = archive.run(
            timestamp,
            duplicate.memoryId,
            duplicate.expectedRevisionId,
          );
          if (archived.changes !== 1) {
            throw new Error(`Memory changed concurrently during merge: ${duplicate.memoryId}`);
          }
          insertStateEvent.run(randomUUID(), duplicate.memoryId, timestamp, duplicate.memoryId);
        }
      }
      for (const sourceMemoryId of inheritedSourceIds) {
        insertRedirect.run(
          randomUUID(),
          sourceMemoryId,
          input.canonicalMemoryId,
          operationId,
          0,
          timestamp,
        );
      }
      return operationId;
    });

    return this.mergeResult(transaction.immediate());
  }

  public createLink(input: {
    fromMemoryId: string;
    toMemoryId: string;
    relation: string;
    metadata?: JsonObject;
    validFrom?: string;
    validTo?: string;
  }): MemoryLink {
    const from = this.getMemory(input.fromMemoryId);
    const to = this.getMemory(input.toMemoryId);
    if (from.spaceId !== to.spaceId) throw new Error('Links cannot cross memory spaces');
    if (from.state === 'deleted' || to.state === 'deleted') {
      throw new Error('Deleted memories cannot be linked');
    }
    assertTemporalRange(input.validFrom, input.validTo);
    const relation = input.relation.trim();
    if (!relation) throw new Error('relation must contain non-whitespace text');
    if (relation.toLocaleLowerCase() === 'merged-into') {
      throw new Error('The merged-into relationship is reserved; use memory_merge');
    }
    const metadata = input.metadata ?? {};
    const metadataJson = stableStringify(metadata);
    const validFrom = input.validFrom ?? null;
    const validTo = input.validTo ?? null;
    const create = this.database.transaction((): MemoryLink => {
      const existing = this.getRow(
        `SELECT * FROM memory_links
         WHERE space_id = ? AND from_memory_id = ? AND to_memory_id = ?
           AND relation = ? COLLATE NOCASE AND metadata_json = ?
           AND valid_from IS ? AND valid_to IS ? AND deleted_at IS NULL
         ORDER BY created_at, id LIMIT 1`,
        from.spaceId,
        from.id,
        to.id,
        relation,
        metadataJson,
        validFrom,
        validTo,
      );
      if (existing) return this.linkFromRow(existing);
      const link: MemoryLink = {
        id: randomUUID(),
        spaceId: from.spaceId,
        fromMemoryId: from.id,
        toMemoryId: to.id,
        relation,
        metadata,
        validFrom,
        validTo,
        createdAt: now(),
        deletedAt: null,
      };
      this.database
        .prepare(
          `INSERT INTO memory_links(
            id, space_id, from_memory_id, to_memory_id, relation, metadata_json,
            valid_from, valid_to, created_at, deleted_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
        )
        .run(
          link.id,
          link.spaceId,
          link.fromMemoryId,
          link.toMemoryId,
          link.relation,
          metadataJson,
          link.validFrom,
          link.validTo,
          link.createdAt,
        );
      return link;
    });
    return create.immediate();
  }

  private linkFromRow(row: Row): MemoryLink {
    return {
      id: String(row.id),
      spaceId: String(row.space_id),
      fromMemoryId: String(row.from_memory_id),
      toMemoryId: String(row.to_memory_id),
      relation: String(row.relation),
      metadata: parseObject(row.metadata_json),
      validFrom: optionalString(row.valid_from),
      validTo: optionalString(row.valid_to),
      createdAt: String(row.created_at),
      deletedAt: optionalString(row.deleted_at),
    };
  }

  public unlink(linkId: string): MemoryLink {
    const existing = this.getRow('SELECT * FROM memory_links WHERE id = ?', linkId);
    if (!existing) {
      if (this.getRow('SELECT 1 FROM memory_redirect_events WHERE id = ?', linkId)) {
        throw new Error('Merge redirects cannot be unlinked');
      }
      throw new Error(`Link not found: ${linkId}`);
    }
    if (existing.deleted_at !== null) return this.linkFromRow(existing);
    this.database
      .prepare('UPDATE memory_links SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL')
      .run(now(), linkId);
    const row = this.requireRow('SELECT * FROM memory_links WHERE id = ?', linkId);
    return this.linkFromRow(row);
  }

  public linksFor(memoryId: string, atTime = now()): MemoryLink[] {
    const links = this.allRows(
      `SELECT * FROM memory_links
           WHERE (from_memory_id = ? OR to_memory_id = ?)
             AND created_at <= ?
             AND (deleted_at IS NULL OR deleted_at > ?)
             AND (valid_from IS NULL OR valid_from <= ?)
             AND (valid_to IS NULL OR valid_to > ?)
           ORDER BY created_at, id`,
      memoryId,
      memoryId,
      atTime,
      atTime,
      atTime,
      atTime,
    ).map((row) => this.linkFromRow(row));
    links.push(
      ...this.redirectLinksForMany([memoryId], {
        atTime,
        relations: [],
        direction: 'both',
      }),
    );
    return links.sort(
      (left, right) => left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
    );
  }

  private redirectLinksForMany(
    memoryIds: string[],
    options: {
      atTime: string;
      relations: string[];
      direction: MemoryLinkDirection;
    },
  ): MemoryLink[] {
    if (memoryIds.length === 0) return [];
    if (
      options.relations.length > 0 &&
      !options.relations.some((relation) => relation.toLocaleLowerCase() === 'merged-into')
    ) {
      return [];
    }
    const placeholders = memoryIds.map(() => '?').join(',');
    const endpointClause =
      options.direction === 'outgoing'
        ? `redirect.source_memory_id IN (${placeholders})`
        : options.direction === 'incoming'
          ? `redirect.canonical_memory_id IN (${placeholders})`
          : `(redirect.source_memory_id IN (${placeholders})
             OR redirect.canonical_memory_id IN (${placeholders}))`;
    const endpointParameters =
      options.direction === 'both' ? [...memoryIds, ...memoryIds] : memoryIds;
    return this.allRows(
      `SELECT redirect.*, source.space_id,
              operation.actor_id AS merge_actor_id,
              operation.reason AS merge_reason,
              operation.metadata_json AS merge_metadata_json
       FROM memory_redirect_events redirect
       JOIN memories source ON source.id = redirect.source_memory_id
       JOIN memory_merge_operations operation ON operation.id = redirect.operation_id
       WHERE ${endpointClause}
         AND redirect.created_at <= ?
         AND NOT EXISTS (
           SELECT 1 FROM memory_redirect_events newer
           WHERE newer.source_memory_id = redirect.source_memory_id
             AND newer.created_at <= ?
             AND (
               newer.created_at > redirect.created_at
               OR (newer.created_at = redirect.created_at AND newer.id > redirect.id)
             )
         )
       ORDER BY redirect.created_at, redirect.id`,
      ...endpointParameters,
      options.atTime,
      options.atTime,
    ).map((row) => {
      const metadata: JsonObject = {
        mergeOperationId: String(row.operation_id),
        direct: Number(row.direct) === 1,
      };
      const actorId = optionalString(row.merge_actor_id);
      const reason = optionalString(row.merge_reason);
      const operationMetadata = parseObject(row.merge_metadata_json);
      if (actorId) metadata.actorId = actorId;
      if (reason) metadata.reason = reason;
      if (Object.keys(operationMetadata).length > 0) metadata.operationMetadata = operationMetadata;
      return {
        id: String(row.id),
        spaceId: String(row.space_id),
        fromMemoryId: String(row.source_memory_id),
        toMemoryId: String(row.canonical_memory_id),
        relation: 'merged-into',
        metadata,
        validFrom: null,
        validTo: null,
        createdAt: String(row.created_at),
        deletedAt: null,
      };
    });
  }

  private linksForMany(
    memoryIds: string[],
    options: {
      atTime: string;
      relations: string[];
      direction: MemoryLinkDirection;
    },
  ): Map<string, MemoryLink[]> {
    const linksByMemoryId = new Map<string, MemoryLink[]>(
      memoryIds.map((memoryId) => [memoryId, []]),
    );
    if (memoryIds.length === 0) return linksByMemoryId;
    const placeholders = memoryIds.map(() => '?').join(',');
    const endpointClause =
      options.direction === 'outgoing'
        ? `from_memory_id IN (${placeholders})`
        : options.direction === 'incoming'
          ? `to_memory_id IN (${placeholders})`
          : `(from_memory_id IN (${placeholders}) OR to_memory_id IN (${placeholders}))`;
    const endpointParameters =
      options.direction === 'both' ? [...memoryIds, ...memoryIds] : memoryIds;
    const relationClause =
      options.relations.length > 0
        ? `AND relation COLLATE NOCASE IN (${options.relations.map(() => '?').join(',')})`
        : '';
    const rows = this.allRows(
      `SELECT * FROM memory_links
       WHERE ${endpointClause}
         AND created_at <= ?
         AND (deleted_at IS NULL OR deleted_at > ?)
         AND (valid_from IS NULL OR valid_from <= ?)
         AND (valid_to IS NULL OR valid_to > ?)
         ${relationClause}
       ORDER BY created_at, id`,
      ...endpointParameters,
      options.atTime,
      options.atTime,
      options.atTime,
      options.atTime,
      ...options.relations,
    );
    const links = rows.map((row) => this.linkFromRow(row));
    links.push(...this.redirectLinksForMany(memoryIds, options));
    links.sort(
      (left, right) => left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
    );
    for (const link of links) {
      if (options.direction !== 'incoming' && linksByMemoryId.has(link.fromMemoryId)) {
        linksByMemoryId.get(link.fromMemoryId)?.push(link);
      }
      if (
        options.direction !== 'outgoing' &&
        link.toMemoryId !== link.fromMemoryId &&
        linksByMemoryId.has(link.toMemoryId)
      ) {
        linksByMemoryId.get(link.toMemoryId)?.push(link);
      }
    }
    return linksByMemoryId;
  }

  public traverseCandidates(options: {
    memoryId: string;
    maxDepth: number;
    atTime: string;
    relations: string[];
    direction: MemoryLinkDirection;
    maxResults: number;
  }): { items: MemoryTraversalEntry[]; truncated: boolean } {
    this.getMemory(options.memoryId, { atTime: options.atTime });
    const discoveries: Array<{
      memoryId: string;
      depth: number;
      via: MemoryLink | null;
      path: MemoryTraversalPathStep[];
    }> = [{ memoryId: options.memoryId, depth: 0, via: null, path: [] }];
    const paths = new Map<string, MemoryTraversalPathStep[]>([[options.memoryId, []]]);
    const visited = new Set([options.memoryId]);
    let frontier = [options.memoryId];
    const targetCount = Math.max(1, options.maxResults) + 1;
    let stoppedAtLimit = false;

    traversal: for (let depth = 1; depth <= options.maxDepth; depth += 1) {
      const next: string[] = [];
      const linksByMemoryId = this.linksForMany(frontier, options);
      for (const sourceId of frontier) {
        for (const link of linksByMemoryId.get(sourceId) ?? []) {
          const outgoing = link.fromMemoryId === sourceId;
          const adjacent = outgoing ? link.toMemoryId : link.fromMemoryId;
          if (visited.has(adjacent)) continue;
          visited.add(adjacent);
          next.push(adjacent);
          const step: MemoryTraversalPathStep = {
            link,
            direction: outgoing ? 'outgoing' : 'incoming',
          };
          const path = [...(paths.get(sourceId) ?? []), step];
          paths.set(adjacent, path);
          discoveries.push({ memoryId: adjacent, depth, via: link, path });
          if (discoveries.length >= targetCount) {
            stoppedAtLimit = true;
            break traversal;
          }
        }
      }
      frontier = next;
      if (frontier.length === 0) break;
    }

    const selected = discoveries.slice(0, options.maxResults);
    const records = this.getMemoriesByIdsAtTime(
      selected.map((entry) => entry.memoryId),
      options.atTime,
    );
    return {
      items: selected.flatMap((entry) => {
        const memory = records.get(entry.memoryId);
        return memory ? [{ memory, depth: entry.depth, via: entry.via, path: entry.path }] : [];
      }),
      truncated: stoppedAtLimit,
    };
  }

  public recordFeedback(input: MemoryFeedbackInput): MemoryFeedback {
    const scope = feedbackScopeSchema.parse(input.scope);
    const actorType = feedbackActorTypeSchema.parse(input.actorType);
    if (scope === 'content') {
      contentFeedbackSignalSchema.parse(input.signal);
      if (input.query !== undefined) throw new Error('Content feedback must not include query');
    } else {
      retrievalFeedbackSignalSchema.parse(input.signal);
      if (!input.revisionId) throw new Error('Retrieval feedback requires revisionId');
      if (!input.query?.trim()) throw new Error('Retrieval feedback requires query');
    }
    if (input.actorId !== undefined && !input.actorId.trim()) {
      throw new Error('actorId must contain non-whitespace text');
    }
    if (input.idempotencyKey !== undefined && !input.idempotencyKey.trim()) {
      throw new Error('idempotencyKey must contain non-whitespace text');
    }
    const actorId = input.actorId?.trim() || null;
    const query = scope === 'retrieval' ? (input.query?.trim() ?? null) : null;
    const note = input.note?.trim() || null;
    const metadata = input.metadata ?? {};
    const idempotencyKey = input.idempotencyKey?.trim() || null;

    const transaction = this.database.transaction((): MemoryFeedback => {
      const existing = idempotencyKey
        ? this.getRow(
            'SELECT * FROM memory_feedback WHERE memory_id = ? AND idempotency_key = ?',
            input.memoryId,
            idempotencyKey,
          )
        : undefined;
      const memory = this.getRow('SELECT current_revision_id FROM memories WHERE id = ?', input.memoryId);
      if (!memory) throw new Error(`Memory not found: ${input.memoryId}`);
      const existingRevisionId = existing ? optionalString(existing.revision_id) : null;
      const revisionId =
        input.revisionId ?? existingRevisionId ?? String(memory.current_revision_id);
      if (
        !this.getRow(
          'SELECT 1 FROM memory_revisions WHERE id = ? AND memory_id = ?',
          revisionId,
          input.memoryId,
        )
      ) {
        throw new Error(`Revision ${revisionId} does not belong to memory ${input.memoryId}`);
      }
      const canonicalRequest = {
        actorId,
        actorType,
        memoryId: input.memoryId,
        metadata,
        note,
        query,
        revisionId,
        scope,
        signal: input.signal,
      };
      const requestHash = createHash('sha256')
        .update(stableStringify(canonicalRequest), 'utf8')
        .digest('hex');
      if (existing) {
        if (optionalString(existing.request_hash) !== requestHash) {
          throw new Error(`Feedback idempotency conflict for key: ${idempotencyKey}`);
        }
        return this.feedbackFromRow(existing);
      }

      const id = randomUUID();
      const createdAt = now();
      this.database
        .prepare(
          `INSERT INTO memory_feedback(
             id, memory_id, revision_id, scope, signal, value, actor_type, actor_id, query,
             note, metadata_json, idempotency_key, request_hash, created_at
           ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          id,
          input.memoryId,
          revisionId,
          scope,
          input.signal,
          actorType,
          actorId,
          query,
          note,
          stableStringify(metadata),
          idempotencyKey,
          requestHash,
          createdAt,
        );
      return this.feedbackFromRow(
        this.requireRow('SELECT * FROM memory_feedback WHERE id = ?', id),
      );
    });
    return transaction.immediate();
  }

  public listFeedback(filters: MemoryFeedbackListFilters): MemoryFeedbackListPage {
    if (!this.getRow('SELECT 1 FROM memories WHERE id = ?', filters.memoryId)) {
      throw new Error(`Memory not found: ${filters.memoryId}`);
    }
    if (
      filters.revisionId &&
      !this.getRow(
        'SELECT 1 FROM memory_revisions WHERE id = ? AND memory_id = ?',
        filters.revisionId,
        filters.memoryId,
      )
    ) {
      throw new Error(`Revision ${filters.revisionId} does not belong to memory ${filters.memoryId}`);
    }
    const clauses = ['memory_id = ?'];
    const parameters: unknown[] = [filters.memoryId];
    if (filters.revisionId) {
      clauses.push('revision_id = ?');
      parameters.push(filters.revisionId);
    }
    if (filters.scope) {
      clauses.push('scope = ?');
      parameters.push(filters.scope);
    }
    if (filters.atTime) {
      clauses.push('created_at <= ?');
      parameters.push(filters.atTime);
    }
    if (filters.cursor) {
      const cursor = decodeFeedbackCursor(filters.cursor);
      clauses.push('(created_at < ? OR (created_at = ? AND id < ?))');
      parameters.push(cursor.createdAt, cursor.createdAt, cursor.id);
    }
    const limit = Math.min(filters.limit ?? 20, 100);
    parameters.push(limit + 1);
    const rows = this.allRows(
      `SELECT * FROM memory_feedback WHERE ${clauses.join(' AND ')}
       ORDER BY created_at DESC, id DESC LIMIT ?`,
      ...parameters,
    );
    const hasMore = rows.length > limit;
    const pageRows = rows.slice(0, limit);
    const last = pageRows.at(-1);
    return {
      items: pageRows.map((row) => this.feedbackFromRow(row)),
      nextCursor:
        hasMore && last
          ? encodeFeedbackCursor(String(last.created_at), String(last.id))
          : null,
    };
  }

  public status(spaceIds?: string[]): Row {
    const scopedParameters = spaceIds ?? [];
    const scopedWhere = (column: string): string => {
      if (spaceIds === undefined) return '';
      if (spaceIds.length === 0) return ' WHERE 0 = 1';
      return ` WHERE ${column} IN (${spaceIds.map(() => '?').join(',')})`;
    };
    const scopedAnd = (column: string): string => {
      if (spaceIds === undefined) return '';
      if (spaceIds.length === 0) return ' AND 0 = 1';
      return ` AND ${column} IN (${spaceIds.map(() => '?').join(',')})`;
    };
    const counts = this.requireRow(
      `SELECT
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END), 0) AS active,
          COALESCE(SUM(CASE WHEN state = 'archived' THEN 1 ELSE 0 END), 0) AS archived,
          COALESCE(SUM(CASE WHEN state = 'deleted' THEN 1 ELSE 0 END), 0) AS deleted,
          COALESCE(SUM(CASE WHEN index_status = 'ready' THEN 1 ELSE 0 END), 0) AS indexed,
          COALESCE(SUM(CASE WHEN index_status = 'lexical-only' THEN 1 ELSE 0 END), 0) AS lexical_only
         FROM memories${scopedWhere('space_id')}`,
      ...scopedParameters,
    );
    const spaceCount = this.requireRow(
      `SELECT COUNT(*) AS count FROM spaces${scopedWhere('id')}`,
      ...scopedParameters,
    );
    const revisionCount = this.requireRow(
      `SELECT COUNT(*) AS count
       FROM memory_revisions revision
       JOIN memories memory ON memory.id = revision.memory_id${scopedWhere('memory.space_id')}`,
      ...scopedParameters,
    );
    const segmentCount = this.requireRow(
      `SELECT COUNT(*) AS count FROM memory_segments${scopedWhere('space_id')}`,
      ...scopedParameters,
    );
    const modelProfileCount = this.requireRow('SELECT COUNT(*) AS count FROM model_profiles');
    const stateEventCount = this.requireRow(
      `SELECT COUNT(*) AS count
       FROM memory_state_events event
       JOIN memories memory ON memory.id = event.memory_id${scopedWhere('memory.space_id')}`,
      ...scopedParameters,
    );
    const pendingCount = this.requireRow(
      `SELECT COUNT(*) AS count
       FROM index_jobs job
       JOIN memory_revisions revision ON revision.id = job.revision_id
       JOIN memories memory ON memory.id = revision.memory_id
       WHERE job.status = 'pending'${scopedAnd('memory.space_id')}`,
      ...scopedParameters,
    );
    const logicalKeyCount = this.requireRow(
      `SELECT COUNT(*) AS count FROM memories
       WHERE logical_key IS NOT NULL${scopedAnd('space_id')}`,
      ...scopedParameters,
    );
    const mergeOperationCount = this.requireRow(
      `SELECT COUNT(*) AS count FROM memory_merge_operations${scopedWhere('space_id')}`,
      ...scopedParameters,
    );
    const redirectCounts = this.requireRow(
      `SELECT COUNT(*) AS events, COUNT(DISTINCT redirect.source_memory_id) AS current
       FROM memory_redirect_events redirect
       JOIN memories memory ON memory.id = redirect.source_memory_id${scopedWhere('memory.space_id')}`,
      ...scopedParameters,
    );
    return {
      database: this.database.name,
      schemaVersion: this.migrations.toVersion,
      vectorAvailable: this.vectorAvailable,
      memories: counts,
      spaces: Number(spaceCount.count),
      revisions: Number(revisionCount.count),
      segments: Number(segmentCount.count),
      stateEvents: Number(stateEventCount.count),
      modelProfiles: Number(modelProfileCount.count),
      pendingJobs: Number(pendingCount.count),
      logicalKeys: Number(logicalKeyCount.count),
      merges: {
        operations: Number(mergeOperationCount.count),
        currentRedirects: Number(redirectCounts.current),
        redirectEvents: Number(redirectCounts.events),
      },
    };
  }

  public migrationStatus(): MigrationStatus {
    return {
      ...this.migrations,
      applied: this.migrations.applied.map((migration) => ({ ...migration })),
    };
  }

  public queueAllCurrentForReindex(): number {
    const rows = this.allRows("SELECT current_revision_id FROM memories WHERE state != 'deleted'");
    const timestamp = now();
    let queued = 0;
    const transaction = this.database.transaction(() => {
      const insert = this.database.prepare(
        `INSERT INTO index_jobs(id, revision_id, status, attempts, created_at, updated_at)
         VALUES (?, ?, 'pending', 0, ?, ?)`,
      );
      const mark = this.database.prepare(
        "UPDATE memories SET index_status = 'pending' WHERE current_revision_id = ?",
      );
      for (const row of rows) {
        const revisionId = String(row.current_revision_id);
        const existing = this.getRow(
          "SELECT 1 FROM index_jobs WHERE revision_id = ? AND status IN ('pending', 'running')",
          revisionId,
        );
        if (existing) continue;
        insert.run(randomUUID(), revisionId, timestamp, timestamp);
        mark.run(revisionId);
        queued += 1;
      }
    });
    transaction();
    return queued;
  }

  public exportSnapshot(): Row {
    const memoryIds = this.allRows('SELECT id FROM memories ORDER BY created_at, id').map((row) =>
      String(row.id),
    );
    const links = this.allRows('SELECT * FROM memory_links ORDER BY created_at, id').map((row) =>
      this.linkFromRow(row),
    );
    const feedback = this.allRows('SELECT * FROM memory_feedback ORDER BY created_at, id').map(
      (row) => this.feedbackFromRow(row),
    );
    const stateEvents = this.allRows(
      'SELECT * FROM memory_state_events ORDER BY recorded_at, memory_id, event_number',
    ).map((row) => ({
      id: String(row.id),
      memoryId: String(row.memory_id),
      eventNumber: Number(row.event_number),
      state: String(row.state),
      recordedAt: String(row.recorded_at),
    }));
    const mergeOperations = this.allRows(
      'SELECT * FROM memory_merge_operations ORDER BY created_at, id',
    ).map((operation) => ({
      id: String(operation.id),
      spaceId: String(operation.space_id),
      canonicalMemoryId: String(operation.canonical_memory_id),
      canonicalRevisionId: String(operation.canonical_revision_id),
      actorId: optionalString(operation.actor_id),
      reason: optionalString(operation.reason),
      metadata: parseObject(operation.metadata_json),
      createdAt: String(operation.created_at),
      members: this.allRows(
        `SELECT duplicate_memory_id, duplicate_revision_id FROM memory_merge_members
         WHERE operation_id = ? ORDER BY duplicate_memory_id`,
        operation.id,
      ).map((member) => ({
        memoryId: String(member.duplicate_memory_id),
        revisionId: String(member.duplicate_revision_id),
      })),
    }));
    const redirectEvents = this.allRows(
      'SELECT * FROM memory_redirect_events ORDER BY created_at, id',
    ).map((redirect) => ({
      id: String(redirect.id),
      sourceMemoryId: String(redirect.source_memory_id),
      canonicalMemoryId: String(redirect.canonical_memory_id),
      operationId: String(redirect.operation_id),
      direct: Number(redirect.direct) === 1,
      createdAt: String(redirect.created_at),
    }));
    return {
      exportedAt: now(),
      spaces: this.listSpaces(),
      memories: memoryIds.map((memoryId) => {
        const memory = this.getMemory(memoryId);
        return {
          ...memory,
          history: this.getHistory(memory.id),
        };
      }),
      links,
      feedback,
      stateEvents,
      mergeOperations,
      redirectEvents,
    };
  }

  public compact(): void {
    this.database.pragma('wal_checkpoint(TRUNCATE)');
    this.database.exec('VACUUM');
  }

  private deleteMemoryRows(memoryId: string): void {
    const segmentIds = this.allRows(
      'SELECT id FROM memory_segments WHERE memory_id = ?',
      memoryId,
    ).map((row) => String(row.id));
    this.database.prepare('DELETE FROM memory_fts WHERE memory_id = ?').run(memoryId);
    if (this.vectorAvailable) {
      const removeVector = this.database.prepare('DELETE FROM memory_vectors WHERE segment_id = ?');
      for (const segmentId of segmentIds) removeVector.run(segmentId);
    }
    this.database
      .prepare(
        `DELETE FROM memory_redirect_events
         WHERE source_memory_id = ? OR canonical_memory_id = ?`,
      )
      .run(memoryId, memoryId);
    this.database
      .prepare('DELETE FROM memory_merge_members WHERE duplicate_memory_id = ?')
      .run(memoryId);
    this.database
      .prepare('DELETE FROM memory_merge_operations WHERE canonical_memory_id = ?')
      .run(memoryId);
    this.database
      .prepare(
        `DELETE FROM memory_merge_operations
         WHERE NOT EXISTS (
           SELECT 1 FROM memory_merge_members member
           WHERE member.operation_id = memory_merge_operations.id
         )`,
      )
      .run();
    this.database
      .prepare('DELETE FROM memory_links WHERE from_memory_id = ? OR to_memory_id = ?')
      .run(memoryId, memoryId);
    this.database.prepare('DELETE FROM memory_feedback WHERE memory_id = ?').run(memoryId);
    this.database.prepare('DELETE FROM memory_state_events WHERE memory_id = ?').run(memoryId);
    this.database
      .prepare(
        'DELETE FROM index_jobs WHERE revision_id IN (SELECT id FROM memory_revisions WHERE memory_id = ?)',
      )
      .run(memoryId);
    this.database.prepare('DELETE FROM memory_segments WHERE memory_id = ?').run(memoryId);
    this.database
      .prepare(
        'DELETE FROM revision_sources WHERE revision_id IN (SELECT id FROM memory_revisions WHERE memory_id = ?)',
      )
      .run(memoryId);
    this.database
      .prepare(
        'DELETE FROM revision_tags WHERE revision_id IN (SELECT id FROM memory_revisions WHERE memory_id = ?)',
      )
      .run(memoryId);
    this.database.prepare('DELETE FROM memory_revisions WHERE memory_id = ?').run(memoryId);
    this.database.prepare('DELETE FROM memories WHERE id = ?').run(memoryId);
  }

  public deleteMemory(memoryId: string): boolean {
    const exists = this.getRow('SELECT 1 FROM memories WHERE id = ?', memoryId) !== undefined;
    if (!exists) return false;
    const transaction = this.database.transaction(() => this.deleteMemoryRows(memoryId));
    transaction();
    return true;
  }

  public purgeDeleted(): number {
    const ids = this.allRows("SELECT id FROM memories WHERE state = 'deleted'").map((row) =>
      String(row.id),
    );
    const transaction = this.database.transaction(() => {
      for (const memoryId of ids) this.deleteMemoryRows(memoryId);
    });
    transaction();
    return ids.length;
  }

  public close(): void {
    this.database.close();
  }
}
