#!/usr/bin/env node
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { createMemoryService } from '../dist/application/create-service.js';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-startup-index-drain-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Startup index drain probe assertion failed: ${message}`);
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function run() {
  const config = loadConfig();
  const logger = new Logger('error');
  const crashedStore = new MemoryStore(config, logger);
  let service;
  let laterStore;
  let inspection;

  try {
    const recoveredMemory = crashedStore.createMemory({
      title: 'Recovered startup indexing job',
      content: 'This revision should be indexed automatically after its stale lease is recovered.',
    });
    assert(
      crashedStore.claimNextPendingRevision() === recoveredMemory.currentRevisionId,
      'the simulated crashed worker should claim the original job',
    );
    crashedStore.close();

    inspection = new Database(config.databasePath);
    inspection
      .prepare(
        "UPDATE index_jobs SET updated_at = '2000-01-01T00:00:00.000Z' WHERE revision_id = ? AND status = 'running'",
      )
      .run(recoveredMemory.currentRevisionId);
    inspection.close();
    inspection = undefined;

    const startupCutoff = new Date().toISOString();
    service = createMemoryService(config);

    await sleep(20);
    laterStore = new MemoryStore(config, logger);
    const laterMemory = laterStore.createMemory({
      title: 'Post-startup indexing job',
      content:
        'This revision was created after startup and must not be claimed by startup recovery.',
    });
    laterStore.close();
    laterStore = undefined;

    const recovered = await service.reindexPending(startupCutoff);
    assert(recovered.indexed === 1, 'startup recovery should index exactly one old job');
    assert(recovered.failed === 0, 'startup recovery should not fail the recovered job');

    inspection = new Database(config.databasePath, { readonly: true });
    const jobsAfterStartupDrain = inspection
      .prepare('SELECT revision_id, status, attempts FROM index_jobs ORDER BY created_at, id')
      .all();
    const recoveredSegments = inspection
      .prepare('SELECT COUNT(*) AS count FROM memory_segments WHERE revision_id = ?')
      .get(recoveredMemory.currentRevisionId).count;
    const laterSegments = inspection
      .prepare('SELECT COUNT(*) AS count FROM memory_segments WHERE revision_id = ?')
      .get(laterMemory.currentRevisionId).count;
    inspection.close();
    inspection = undefined;

    assert(jobsAfterStartupDrain.length === 2, 'the probe should contain exactly two jobs');
    assert(
      jobsAfterStartupDrain[0]?.revision_id === recoveredMemory.currentRevisionId &&
        jobsAfterStartupDrain[0]?.status === 'complete' &&
        jobsAfterStartupDrain[0]?.attempts === 2,
      'the stale job should be recovered, claimed a second time, and completed',
    );
    assert(
      jobsAfterStartupDrain[1]?.revision_id === laterMemory.currentRevisionId &&
        jobsAfterStartupDrain[1]?.status === 'pending' &&
        jobsAfterStartupDrain[1]?.attempts === 0,
      'post-startup work should remain pending for its owning request or an explicit drain',
    );
    assert(recoveredSegments > 0, 'the recovered revision should receive lexical segments');
    assert(
      laterSegments === 0,
      'the post-startup revision should not be indexed by startup recovery',
    );

    const remaining = await service.reindexPending();
    assert(remaining.indexed === 1, 'an unrestricted drain should process the later job');
    assert(remaining.failed === 0, 'the unrestricted drain should not fail');

    return {
      ok: true,
      startupDrain: recovered,
      laterDrain: remaining,
      recoveredJob: jobsAfterStartupDrain[0],
      postStartupJob: jobsAfterStartupDrain[1],
    };
  } finally {
    inspection?.close();
    try {
      crashedStore.close();
    } catch {}
    try {
      laterStore?.close();
    } catch {}
    await service?.close();
  }
}

let outcome;
try {
  outcome = await run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
