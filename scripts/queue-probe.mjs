#!/usr/bin/env node
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-queue-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Queue probe assertion failed: ${message}`);
}

function run() {
  const config = loadConfig();
  const logger = new Logger('error');
  const first = new MemoryStore(config, logger);
  const second = new MemoryStore(config, logger);
  let third;
  let fourth;
  let inspection;
  try {
    const memory = first.createMemory({
      title: 'Durable queue probe',
      content: 'This pending revision exists only to test atomic queue leases.',
    });
    const claimedByFirst = first.claimNextPendingRevision();
    assert(claimedByFirst === memory.currentRevisionId, 'first connection should claim the job');
    assert(
      second.claimNextPendingRevision() === null,
      'second connection must not steal a live job',
    );
    assert(first.queueAllCurrentForReindex() === 0, 'running job must not be duplicated');

    first.close();
    second.close();

    inspection = new Database(config.databasePath);
    inspection
      .prepare(
        "UPDATE index_jobs SET updated_at = '2000-01-01T00:00:00.000Z' WHERE status = 'running'",
      )
      .run();
    inspection.close();
    inspection = undefined;

    third = new MemoryStore(config, logger);
    const recovered = third.claimNextPendingRevision();
    assert(
      recovered === memory.currentRevisionId,
      'stale running job should be recovered and claimed',
    );
    third.markIndexStatus(recovered, 'lexical-only');
    assert(third.claimNextPendingRevision() === null, 'completed job must leave no pending work');
    assert(
      third.queueAllCurrentForReindex() === 1,
      'explicit reindex should queue completed work once',
    );
    assert(
      third.queueAllCurrentForReindex() === 0,
      'second reindex request must not duplicate pending work',
    );
    const explicitClaim = third.claimNextPendingRevision();
    assert(
      explicitClaim === memory.currentRevisionId,
      'explicitly queued revision should be claimable',
    );

    inspection = new Database(config.databasePath, { readonly: true });
    const jobs = inspection
      .prepare('SELECT status, attempts FROM index_jobs ORDER BY created_at, id')
      .all();
    assert(jobs.length === 2, 'one original and one explicit reindex job should exist');
    assert(jobs[0]?.status === 'complete', 'recovered original job should complete');
    assert(jobs[0]?.attempts === 2, 'recovered original job should record both claims');
    assert(jobs[1]?.status === 'running', 'explicit reindex claim should hold a live lease');
    assert(jobs[1]?.attempts === 1, 'explicit reindex should have one claim attempt');
    inspection.close();
    inspection = undefined;

    third.close();
    third = undefined;
    fourth = new MemoryStore(config, logger);
    assert(
      fourth.claimNextPendingRevision() === null,
      'fresh live lease must survive another open',
    );

    return { ok: true, jobs };
  } finally {
    if (inspection) inspection.close();
    try {
      first.close();
    } catch {}
    try {
      second.close();
    } catch {}
    try {
      third?.close();
    } catch {}
    try {
      fourth?.close();
    } catch {}
  }
}

let outcome;
try {
  outcome = run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
