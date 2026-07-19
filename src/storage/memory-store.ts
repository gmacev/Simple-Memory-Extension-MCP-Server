import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import * as z from 'zod/v4';
import type { AppConfig } from '../config.js';
import { contentHash, parseJsonValue, stableStringify } from '../domain/json.js';
import type {
  IndexStatus,
  JsonObject,
  MemoryInput,
  MemoryLink,
  MemoryListFilters,
  MemoryListPage,
  MemoryRecord,
  MemoryRevision,
  MemoryState,
  SegmentRecord,
  SourceInput,
} from '../domain/types.js';
import { searchableProjection } from '../indexing/projector.js';
import type { Logger } from '../logger.js';
import { schemaSql } from './schema.js';

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
    this.database.exec(schemaSql);
    this.database
      .prepare(
        `UPDATE index_jobs SET status = 'pending', updated_at = ?
         WHERE status = 'running' AND julianday(updated_at) < julianday('now', '-15 minutes')`,
      )
      .run(now());
    this.ensureDefaultSpace();
    let vectorAvailable = false;
    try {
      sqliteVec.load(this.database);
      this.database.exec(
        `CREATE VIRTUAL TABLE IF NOT EXISTS memory_vectors USING vec0(
          segment_id TEXT PRIMARY KEY,
          embedding float[${config.embeddingDimension}],
          model_profile_id TEXT PARTITION KEY
        )`,
      );
      vectorAvailable = true;
    } catch (error) {
      this.logger.warn('sqlite-vec unavailable; semantic search will degrade', {
        error: String(error),
      });
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

  public listSpaces(): Row[] {
    return this.allRows('SELECT * FROM spaces ORDER BY created_at, id').map((row) => ({
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

  public createMemory(input: MemoryInput, actor: string | null = null): MemoryRecord {
    const spaceId = input.spaceId ?? 'default';
    this.assertSpace(spaceId);
    assertTemporalRange(input.validFrom, input.validTo);
    if (input.idempotencyKey) {
      const existing = this.getRow(
        'SELECT id FROM memories WHERE space_id = ? AND idempotency_key = ?',
        spaceId,
        input.idempotencyKey,
      );
      if (existing && typeof existing.id === 'string') return this.getMemory(existing.id);
    }
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
    const transaction = this.database.transaction(() => {
      this.database
        .prepare(
          `INSERT INTO memories(
            id, space_id, state, current_revision_id, created_at, updated_at, index_status,
            idempotency_key
          ) VALUES (?, ?, 'active', ?, ?, ?, 'pending', ?)`,
        )
        .run(memoryId, spaceId, revisionId, timestamp, timestamp, input.idempotencyKey ?? null);
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
    });
    transaction();
    return this.getMemory(memoryId);
  }

  public reviseMemory(
    memoryId: string,
    input: MemoryInput,
    expectedRevisionId: string,
    actor: string | null = null,
  ): MemoryRecord {
    const current = this.getMemory(memoryId);
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
           WHERE id = ? AND current_revision_id = ?`,
        )
        .run(revisionId, timestamp, memoryId, expectedRevisionId);
      if (changed.changes !== 1) throw new Error('Revision conflict during update');
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

  private revisionFromRow(row: Row): MemoryRevision {
    const revisionId = String(row.revision_id ?? row.id);
    const tags = this.allRows(
      'SELECT tag FROM revision_tags WHERE revision_id = ? ORDER BY tag',
      revisionId,
    ).map((tag) => String(tag.tag));
    const sources = this.allRows(
      'SELECT * FROM revision_sources WHERE revision_id = ? ORDER BY id',
      revisionId,
    ).map((source): SourceInput => {
      const result: SourceInput = { metadata: parseObject(source.metadata_json) };
      if (typeof source.uri === 'string') result.uri = source.uri;
      if (typeof source.label === 'string') result.label = source.label;
      if (typeof source.type === 'string') result.type = source.type;
      if (typeof source.observed_at === 'string') result.observedAt = source.observed_at;
      return result;
    });
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

  private memoryFromRows(
    memory: Row,
    revision: Row,
    state = z.enum(['active', 'archived', 'deleted']).parse(memory.state),
  ): MemoryRecord {
    return {
      id: String(memory.id),
      spaceId: String(memory.space_id),
      state,
      createdAt: String(memory.created_at),
      updatedAt: String(memory.updated_at),
      currentRevisionId: String(memory.current_revision_id),
      indexStatus: z
        .enum(['pending', 'ready', 'lexical-only', 'failed'])
        .parse(memory.index_status),
      revision: this.revisionFromRow(revision),
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
    return this.memoryFromRows(memory, revision, state);
  }

  public getHistory(memoryId: string): MemoryRevision[] {
    if (!this.database.prepare('SELECT 1 FROM memories WHERE id = ?').get(memoryId)) {
      throw new Error(`Memory not found: ${memoryId}`);
    }
    return this.allRows(
      `SELECT *, id AS revision_id FROM memory_revisions
           WHERE memory_id = ? ORDER BY revision_number DESC`,
      memoryId,
    ).map((row) => this.revisionFromRow(row));
  }

  public listMemories(filters: MemoryListFilters = {}): MemoryListPage {
    const clauses: string[] = ['1=1'];
    const parameters: unknown[] = [];
    if (filters.spaceId) {
      clauses.push('m.space_id = ?');
      parameters.push(filters.spaceId);
    }
    if (filters.state) {
      clauses.push('m.state = ?');
      parameters.push(filters.state);
    } else {
      clauses.push("m.state != 'deleted'");
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
    const limit = Math.min(filters.limit ?? 50, 200);
    parameters.push(limit + 1);
    const rows = this.allRows(
      `SELECT m.id, m.updated_at
         FROM memories m JOIN memory_revisions r ON r.id = m.current_revision_id
         WHERE ${clauses.join(' AND ')}
         ORDER BY m.updated_at DESC, m.id DESC LIMIT ?`,
      ...parameters,
    );
    const hasMore = rows.length > limit;
    const pageRows = rows.slice(0, limit);
    const items = pageRows.map((row) => this.getMemory(String(row.id)));
    const last = pageRows.at(-1);
    return {
      items,
      nextCursor:
        hasMore && last ? encodeListCursor(String(last.updated_at), String(last.id)) : null,
    };
  }

  public setState(memoryId: string, state: MemoryState): MemoryRecord {
    const current = this.getMemory(memoryId);
    if (current.state === state) return current;
    const latestEvent = this.requireRow(
      `SELECT recorded_at FROM memory_state_events
       WHERE memory_id = ? ORDER BY event_number DESC LIMIT 1`,
      memoryId,
    );
    const previousTimestamp = Date.parse(String(latestEvent.recorded_at));
    const timestamp = new Date(Math.max(Date.now(), previousTimestamp + 1)).toISOString();
    const transaction = this.database.transaction(() => {
      const result = this.database
        .prepare('UPDATE memories SET state = ?, updated_at = ? WHERE id = ? AND state = ?')
        .run(state, timestamp, memoryId, current.state);
      if (result.changes !== 1) throw new Error('Memory state changed concurrently');
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
    if (filters.spaceIds && filters.spaceIds.length > 0) {
      clauses.push(`m.space_id IN (${filters.spaceIds.map(() => '?').join(',')})`);
      parameters.push(...filters.spaceIds);
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
                WHEN m.id = ? THEN 0
                WHEN lower(r.title) = lower(?) THEN 1
                ELSE 2
              END AS rank_value
       FROM memories m
       JOIN memory_revisions r ON r.memory_id = m.id
       JOIN memory_segments s ON s.revision_id = r.id
         AND s.ordinal = (
           SELECT MIN(s2.ordinal) FROM memory_segments s2 WHERE s2.revision_id = r.id
         )
       WHERE ${candidates.sql}
         AND (
           m.id = ? OR lower(r.title) = lower(?)
           OR lower(r.title) LIKE lower(?) ESCAPE '\\'
         )
       ORDER BY rank_value, r.recorded_at DESC
       LIMIT ?`,
      normalized,
      normalized,
      ...candidates.parameters,
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
    const link: MemoryLink = {
      id: randomUUID(),
      spaceId: from.spaceId,
      fromMemoryId: from.id,
      toMemoryId: to.id,
      relation: input.relation.trim(),
      metadata: input.metadata ?? {},
      validFrom: input.validFrom ?? null,
      validTo: input.validTo ?? null,
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
        stableStringify(link.metadata),
        link.validFrom,
        link.validTo,
        link.createdAt,
      );
    return link;
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
    if (!existing) throw new Error(`Link not found: ${linkId}`);
    if (existing.deleted_at !== null) return this.linkFromRow(existing);
    this.database
      .prepare('UPDATE memory_links SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL')
      .run(now(), linkId);
    const row = this.requireRow('SELECT * FROM memory_links WHERE id = ?', linkId);
    return this.linkFromRow(row);
  }

  public linksFor(memoryId: string, atTime = now()): MemoryLink[] {
    return this.allRows(
      `SELECT * FROM memory_links
           WHERE (from_memory_id = ? OR to_memory_id = ?)
             AND created_at <= ?
             AND (deleted_at IS NULL OR deleted_at > ?)
             AND (valid_from IS NULL OR valid_from <= ?)
             AND (valid_to IS NULL OR valid_to > ?)
           ORDER BY created_at`,
      memoryId,
      memoryId,
      atTime,
      atTime,
      atTime,
      atTime,
    ).map((row) => this.linkFromRow(row));
  }

  public traverse(
    memoryId: string,
    maxDepth = 2,
    atTime = now(),
  ): Array<{ memory: MemoryRecord; depth: number; via: MemoryLink | null }> {
    const boundedDepth = Math.max(0, Math.min(maxDepth, 5));
    const output: Array<{ memory: MemoryRecord; depth: number; via: MemoryLink | null }> = [
      { memory: this.getMemory(memoryId, { atTime }), depth: 0, via: null },
    ];
    const visited = new Set([memoryId]);
    let frontier = [memoryId];
    for (let depth = 1; depth <= boundedDepth; depth += 1) {
      const next: string[] = [];
      for (const sourceId of frontier) {
        for (const link of this.linksFor(sourceId, atTime)) {
          const adjacent = link.fromMemoryId === sourceId ? link.toMemoryId : link.fromMemoryId;
          if (visited.has(adjacent)) continue;
          visited.add(adjacent);
          next.push(adjacent);
          output.push({ memory: this.getMemory(adjacent, { atTime }), depth, via: link });
        }
      }
      frontier = next;
      if (frontier.length === 0) break;
    }
    return output;
  }

  public recordFeedback(input: {
    memoryId: string;
    signal: string;
    value?: number;
    note?: string;
    metadata?: JsonObject;
  }): Row {
    this.getMemory(input.memoryId);
    const result = {
      id: randomUUID(),
      memoryId: input.memoryId,
      signal: input.signal,
      value: input.value ?? null,
      note: input.note ?? null,
      metadata: input.metadata ?? {},
      createdAt: now(),
    };
    this.database
      .prepare(
        `INSERT INTO memory_feedback(id, memory_id, signal, value, note, metadata_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        result.id,
        result.memoryId,
        result.signal,
        result.value,
        result.note,
        stableStringify(result.metadata),
        result.createdAt,
      );
    return result;
  }

  public status(): Row {
    const counts = this.requireRow(
      `SELECT
          COUNT(*) AS total,
          COALESCE(SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END), 0) AS active,
          COALESCE(SUM(CASE WHEN state = 'archived' THEN 1 ELSE 0 END), 0) AS archived,
          COALESCE(SUM(CASE WHEN state = 'deleted' THEN 1 ELSE 0 END), 0) AS deleted,
          COALESCE(SUM(CASE WHEN index_status = 'ready' THEN 1 ELSE 0 END), 0) AS indexed,
          COALESCE(SUM(CASE WHEN index_status = 'lexical-only' THEN 1 ELSE 0 END), 0) AS lexical_only
         FROM memories`,
    );
    const spaceCount = this.requireRow('SELECT COUNT(*) AS count FROM spaces');
    const revisionCount = this.requireRow('SELECT COUNT(*) AS count FROM memory_revisions');
    const segmentCount = this.requireRow('SELECT COUNT(*) AS count FROM memory_segments');
    const modelProfileCount = this.requireRow('SELECT COUNT(*) AS count FROM model_profiles');
    const stateEventCount = this.requireRow('SELECT COUNT(*) AS count FROM memory_state_events');
    const pendingCount = this.requireRow(
      "SELECT COUNT(*) AS count FROM index_jobs WHERE status = 'pending'",
    );
    return {
      database: this.database.name,
      vectorAvailable: this.vectorAvailable,
      memories: counts,
      spaces: Number(spaceCount.count),
      revisions: Number(revisionCount.count),
      segments: Number(segmentCount.count),
      stateEvents: Number(stateEventCount.count),
      modelProfiles: Number(modelProfileCount.count),
      pendingJobs: Number(pendingCount.count),
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
      (row) => ({
        id: String(row.id),
        memoryId: String(row.memory_id),
        signal: String(row.signal),
        value: optionalNumber(row.value),
        note: optionalString(row.note),
        metadata: parseObject(row.metadata_json),
        createdAt: String(row.created_at),
      }),
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
    };
  }

  public compact(): void {
    this.database.pragma('wal_checkpoint(TRUNCATE)');
    this.database.exec('VACUUM');
  }

  public purgeDeleted(): number {
    const ids = this.allRows("SELECT id FROM memories WHERE state = 'deleted'").map((row) =>
      String(row.id),
    );
    const transaction = this.database.transaction(() => {
      for (const memoryId of ids) {
        const segmentIds = this.allRows(
          'SELECT id FROM memory_segments WHERE memory_id = ?',
          memoryId,
        ).map((row) => String(row.id));
        this.database.prepare('DELETE FROM memory_fts WHERE memory_id = ?').run(memoryId);
        if (this.vectorAvailable) {
          const removeVector = this.database.prepare(
            'DELETE FROM memory_vectors WHERE segment_id = ?',
          );
          for (const segmentId of segmentIds) removeVector.run(segmentId);
        }
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
    });
    transaction();
    return ids.length;
  }

  public close(): void {
    this.database.close();
  }
}
