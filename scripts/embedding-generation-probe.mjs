#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { MemoryStore } from '../dist/storage/memory-store.js';
import { linkTraversalIndexesSql } from '../dist/storage/migrations/002-link-traversal-indexes.js';
import { revisionAwareFeedbackSql } from '../dist/storage/migrations/003-revision-aware-feedback.js';
import { logicalIdentityAndMergesSql } from '../dist/storage/migrations/004-logical-identity-and-merges.js';
import { spaceLifecycleAndDiscoverySql } from '../dist/storage/migrations/005-space-lifecycle-and-discovery.js';
import { scalingIndexesAndSpaceSearchSql } from '../dist/storage/migrations/006-scaling-indexes-and-space-search.js';
import { schemaSql } from '../dist/storage/schema.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-embedding-generation-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Embedding generation probe assertion failed: ${message}`);
}

function checksum(sql) {
  return createHash('sha256').update(sql.replace(/\r\n?/gu, '\n').trim(), 'utf8').digest('hex');
}

function createV6Fixture(databasePath) {
  const database = new Database(databasePath);
  sqliteVec.load(database);
  const migrations = [
    ['initial-schema', schemaSql],
    ['link-traversal-indexes', linkTraversalIndexesSql],
    ['revision-aware-feedback', revisionAwareFeedbackSql],
    ['logical-identity-and-merges', logicalIdentityAndMergesSql],
    ['space-lifecycle-and-discovery', spaceLifecycleAndDiscoverySql],
    ['scaling-indexes-and-space-search', scalingIndexesAndSpaceSearchSql],
  ];
  database.exec(schemaSql);
  for (const [, sql] of migrations.slice(1)) database.exec(sql);
  database.exec(`
    CREATE TABLE schema_migrations (
      version INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      checksum TEXT NOT NULL,
      applied_at TEXT NOT NULL
    )
  `);
  const recordMigration = database.prepare(
    'INSERT INTO schema_migrations(version, name, checksum, applied_at) VALUES (?, ?, ?, ?)',
  );
  migrations.forEach(([name, sql], index) => {
    recordMigration.run(index + 1, name, checksum(sql), '2026-01-01T00:00:00.000Z');
  });
  database.exec(`
    CREATE VIRTUAL TABLE memory_vectors USING vec0(
      segment_id TEXT PRIMARY KEY,
      embedding float[1024],
      model_profile_id TEXT PARTITION KEY
    );
    CREATE VIRTUAL TABLE memory_current_vectors USING vec0(
      segment_id TEXT PRIMARY KEY,
      embedding float[1024],
      model_profile_id TEXT PARTITION KEY,
      memory_id TEXT,
      space_id TEXT,
      memory_state TEXT,
      space_state TEXT,
      kind TEXT,
      confidence FLOAT,
      salience FLOAT,
      valid_from TEXT,
      valid_to TEXT,
      expires_at TEXT,
      recorded_at TEXT
    );
  `);
  const timestamp = '2026-01-01T00:00:00.000Z';
  database
    .prepare(
      `INSERT INTO spaces(id, name, metadata_json, created_at, deleted_at)
       VALUES (?, ?, '{}', ?, ?)`,
    )
    .run('active-space', 'Active', timestamp, null);
  database
    .prepare(
      `INSERT INTO spaces(id, name, metadata_json, created_at, deleted_at)
       VALUES (?, ?, '{}', ?, ?)`,
    )
    .run('deleted-space', 'Deleted', timestamp, '2026-02-01T00:00:00.000Z');
  const insertMemory = database.prepare(
    `INSERT INTO memories(
       id, space_id, state, current_revision_id, created_at, updated_at, index_status
     ) VALUES (?, ?, ?, ?, ?, ?, 'ready')`,
  );
  insertMemory.run('active-memory', 'active-space', 'active', 'active-r2', timestamp, timestamp);
  insertMemory.run(
    'archived-memory',
    'active-space',
    'archived',
    'archived-r1',
    timestamp,
    timestamp,
  );
  insertMemory.run(
    'deleted-space-memory',
    'deleted-space',
    'active',
    'deleted-space-r1',
    timestamp,
    timestamp,
  );
  const insertRevision = database.prepare(
    `INSERT INTO memory_revisions(
       id, memory_id, revision_number, parent_revision_id, content_json, metadata_json,
       recorded_at, content_hash, searchable_text
     ) VALUES (?, ?, ?, ?, '"fixture"', '{}', ?, ?, 'fixture')`,
  );
  insertRevision.run('active-r1', 'active-memory', 1, null, timestamp, 'hash-1');
  insertRevision.run(
    'active-r2',
    'active-memory',
    2,
    'active-r1',
    '2026-01-02T00:00:00.000Z',
    'hash-2',
  );
  insertRevision.run('archived-r1', 'archived-memory', 1, null, timestamp, 'hash-3');
  insertRevision.run('deleted-space-r1', 'deleted-space-memory', 1, null, timestamp, 'hash-4');
  const insertJob = database.prepare(
    `INSERT INTO index_jobs(id, revision_id, status, attempts, created_at, updated_at)
     VALUES (?, ?, 'complete', 1, ?, ?)`,
  );
  for (const revisionId of ['active-r1', 'active-r2', 'archived-r1', 'deleted-space-r1']) {
    insertJob.run(`old-${revisionId}`, revisionId, timestamp, timestamp);
  }
  database.close();
}

function run() {
  const config = loadConfig();
  createV6Fixture(config.databasePath);
  const profile = {
    provider: 'huggingface',
    model: config.embeddingModel,
    modelRevision: config.embeddingRevision,
    dimensions: config.embeddingDimension,
    instructionHash: createHash('sha256').update(config.queryInstruction).digest('hex'),
  };
  const logger = new Logger('error');
  let store = new MemoryStore(config, logger);
  const migration = store.migrationStatus();
  assert(migration.fromVersion === 6 && migration.toVersion === 8, 'v6 should migrate through v8');
  assert(
    migration.backupPath && existsSync(migration.backupPath),
    'migration should create a backup',
  );

  const prepared = store.prepareEmbeddingGeneration(profile);
  assert(
    prepared.total === 4,
    'all current, historical, archived, and deleted-space revisions should queue',
  );
  assert(prepared.pending === 4, 'all generation jobs should begin pending');

  const firstClaim = store.claimNextPendingRevision(undefined, {
    embeddingGenerationId: prepared.id,
  });
  assert(firstClaim, 'the first generation job should be claimable');
  store.markIndexStatus(firstClaim.revisionId, 'ready', undefined, {
    id: firstClaim.id,
    status: 'complete',
  });
  const interruptedClaim = store.claimNextPendingRevision(undefined, {
    embeddingGenerationId: prepared.id,
  });
  assert(interruptedClaim, 'a second generation job should be claimable before interruption');
  const failedClaim = store.claimNextPendingRevision(undefined, {
    embeddingGenerationId: prepared.id,
  });
  assert(failedClaim, 'a third generation job should be claimable before interruption');
  store.failClaimedIndexJob(failedClaim.id, 'simulated failure');
  store.close();

  store = new MemoryStore(config, logger);
  const resumed = store.prepareEmbeddingGeneration(profile);
  assert(
    resumed.completed === 1 && resumed.pending === 3,
    'resume should retain completed work and retry unfinished jobs',
  );
  let claimed = store.claimNextPendingRevision(undefined, { embeddingGenerationId: prepared.id });
  while (claimed) {
    store.markIndexStatus(claimed.revisionId, 'ready', undefined, {
      id: claimed.id,
      status: 'complete',
    });
    claimed = store.claimNextPendingRevision(undefined, { embeddingGenerationId: prepared.id });
  }
  const complete = store.finishEmbeddingGeneration(profile);
  assert(complete.status === 'complete' && complete.completed === 4, 'generation should complete');
  store.close();

  store = new MemoryStore(config, logger);
  const repeated = store.prepareEmbeddingGeneration(profile);
  assert(repeated.status === 'complete', 'matching generation should be reused');
  assert(repeated.isCurrent, 'the reused generation should be the current physical index');
  assert(repeated.total === 4, 'repeated preparation should not duplicate jobs');

  const alternateProfile = { ...profile, model: `${profile.model}-alternate` };
  const alternate = store.prepareEmbeddingGeneration(alternateProfile);
  assert(alternate.isCurrent && alternate.pending === 4, 'a new profile should become current');
  let alternateJob = store.claimNextPendingRevision(undefined, {
    embeddingGenerationId: alternate.id,
  });
  while (alternateJob) {
    store.markIndexStatus(alternateJob.revisionId, 'ready', undefined, {
      id: alternateJob.id,
      status: 'complete',
    });
    alternateJob = store.claimNextPendingRevision(undefined, {
      embeddingGenerationId: alternate.id,
    });
  }
  store.finishEmbeddingGeneration(alternateProfile);
  const supersededOriginal = store.embeddingGenerationProgress(profile);
  assert(
    supersededOriginal?.status === 'complete' && !supersededOriginal.isCurrent,
    'a completed predecessor should remain historical rather than current',
  );

  const rollback = store.prepareEmbeddingGeneration(profile);
  assert(
    rollback.isCurrent && rollback.pending === 4 && rollback.completed === 0,
    'returning to an older profile should rebuild instead of reusing stale completion',
  );
  let rollbackJob = store.claimNextPendingRevision(undefined, {
    embeddingGenerationId: rollback.id,
  });
  while (rollbackJob) {
    store.markIndexStatus(rollbackJob.revisionId, 'ready', undefined, {
      id: rollbackJob.id,
      status: 'complete',
    });
    rollbackJob = store.claimNextPendingRevision(undefined, {
      embeddingGenerationId: rollback.id,
    });
  }
  const rollbackComplete = store.finishEmbeddingGeneration(profile);
  assert(rollbackComplete.status === 'complete', 'the rebuilt rollback generation should complete');
  store.close();

  const inspection = new Database(config.databasePath, { readonly: true });
  const vectorSql = inspection
    .prepare("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'memory_vectors'")
    .get().sql;
  const generationJobs = inspection
    .prepare('SELECT COUNT(*) AS count FROM index_jobs WHERE embedding_generation_id IS NOT NULL')
    .get().count;
  const currentGenerations = inspection
    .prepare('SELECT COUNT(*) AS count FROM embedding_index_generations WHERE is_current = 1')
    .get().count;
  inspection.close();
  assert(
    String(vectorSql).includes('float[896]'),
    'vector table should be recreated at 896 dimensions',
  );
  assert(generationJobs === 8, 'each retained profile should keep one job per revision');
  assert(currentGenerations === 1, 'exactly one embedding generation should be current');
  return {
    ok: true,
    migration,
    generation: rollbackComplete,
    generationJobs,
    currentGenerations,
  };
}

let result;
try {
  result = run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
