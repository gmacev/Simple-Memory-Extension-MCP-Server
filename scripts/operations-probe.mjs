#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { createMemoryService } from '../dist/application/create-service.js';
import { loadConfig } from '../dist/config.js';
import { backupDatabase, restoreDatabase } from '../dist/operations/database-operations.js';
import { acquireDatabaseServerLease } from '../dist/operations/database-runtime.js';
import { currentSchemaVersion } from '../dist/storage/migrations/index.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-operations-'));

function assert(condition, message) {
  if (!condition) throw new Error(`Operations probe failed: ${message}`);
}

function probeInvalidConfiguration() {
  const result = spawnSync(
    process.execPath,
    [path.join(root, 'dist', 'cli.js'), 'config', 'validate'],
    {
      cwd: root,
      env: {
        ...process.env,
        SIMPLE_MEMORY_DATA_DIR: dataDir,
        SIMPLE_MEMORY_MODELS: 'disabled',
        SIMPLE_MEMORY_INFERENCE_QUEUE_LIMIT: 'not-a-number',
      },
      encoding: 'utf8',
    },
  );
  assert(result.status !== 0, 'invalid configuration must fail');
  assert(
    result.stderr.includes('SIMPLE_MEMORY_INFERENCE_QUEUE_LIMIT must be a positive integer'),
    'invalid configuration must identify the setting',
  );
}

function probeCliSurface() {
  const environment = {
    ...process.env,
    SIMPLE_MEMORY_DATA_DIR: dataDir,
    SIMPLE_MEMORY_MODELS: 'disabled',
  };
  const help = spawnSync(process.execPath, [path.join(root, 'dist', 'cli.js'), '--help'], {
    cwd: root,
    env: environment,
    encoding: 'utf8',
  });
  assert(help.status === 0, 'CLI help must succeed');
  assert(help.stdout.includes('memoryctl backup'), 'CLI help must document backup');
  assert(help.stdout.includes('memoryctl restore'), 'CLI help must document restore');

  const jitlessVersion = spawnSync(
    process.execPath,
    ['--disallow-code-generation-from-strings', path.join(root, 'dist', 'cli.js'), '--version'],
    { cwd: root, env: environment, encoding: 'utf8' },
  );
  assert(jitlessVersion.status === 0, 'CLI must remain available when code generation is disabled');
  assert(jitlessVersion.stdout.trim() === '3.9.5', 'jitless CLI must report the current version');

  const show = spawnSync(process.execPath, [path.join(root, 'dist', 'cli.js'), 'config', 'show'], {
    cwd: root,
    env: environment,
    encoding: 'utf8',
  });
  assert(show.status === 0, 'config show must succeed');
  const shown = JSON.parse(show.stdout);
  assert(
    shown.databasePath.endsWith('memory.db'),
    'config show must expose the effective database',
  );

  const unconfirmed = spawnSync(
    process.execPath,
    [path.join(root, 'dist', 'cli.js'), 'restore', 'some-backup.db'],
    { cwd: root, env: environment, encoding: 'utf8' },
  );
  assert(unconfirmed.status !== 0, 'restore must require explicit confirmation');
}

async function run() {
  process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
  process.env.SIMPLE_MEMORY_MODELS = 'disabled';
  process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';
  delete process.env.SIMPLE_MEMORY_INFERENCE_QUEUE_LIMIT;
  const config = loadConfig();
  let service = createMemoryService(config);
  let first;
  let later;
  const backupPath = path.join(dataDir, 'backups', 'known-good.db');
  let backup;
  try {
    const globalSpace = service.listSpaces({ id: 'default' }).items[0];
    assert(globalSpace?.name === 'Global', 'default space must advertise its global role');
    assert(
      globalSpace.description ===
        'Broadly applicable preferences, working norms, and durable context shared across projects and domains. Context-specific information belongs in its own space.',
      'default space must describe its cross-context scope',
    );
    first = await service.createMemory({
      title: 'Backup baseline',
      content: { state: 'preserved' },
    });
    // Keep the WAL-backed service open to verify that backup is a consistent
    // online snapshot rather than a raw main-file copy.
    backup = await backupDatabase(config.databasePath, backupPath);
    later = await service.createMemory({
      title: 'Post-backup mutation',
      content: { state: 'must disappear after restore' },
    });
  } finally {
    await service.close();
  }

  assert(backup.schemaVersion === currentSchemaVersion, 'backup must report the current schema');
  assert(backup.bytes > 0 && existsSync(backupPath), 'backup file must be created');

  const invalidPath = path.join(dataDir, 'not-a-database.db');
  writeFileSync(invalidPath, 'not sqlite', 'utf8');
  const serverLease = await acquireDatabaseServerLease(config.databasePath);
  try {
    await restoreDatabase(config, invalidPath)
      .then(() => {
        throw new Error('restore unexpectedly proceeded while a server lease was active');
      })
      .catch((error) => {
        assert(
          String(error).includes('while Simple Memory is running'),
          'restore must refuse an active database owner',
        );
      });
  } finally {
    await serverLease.release();
  }
  await restoreDatabase(config, invalidPath)
    .then(() => {
      throw new Error('invalid restore unexpectedly succeeded');
    })
    .catch((error) => {
      assert(String(error).includes('validation failed'), 'invalid restore must fail validation');
    });

  const restored = await restoreDatabase(config, backupPath);
  assert(restored.schemaVersion === currentSchemaVersion, 'restore must retain the current schema');
  assert(
    restored.safetyBackupPath && existsSync(restored.safetyBackupPath),
    'restore must preserve a safety backup of the replaced database',
  );

  const legacyDatabase = new Database(config.databasePath);
  try {
    legacyDatabase
      .prepare(
        "UPDATE spaces SET name = 'Default', description = 'Default memory isolation space' WHERE id = 'default'",
      )
      .run();
  } finally {
    legacyDatabase.close();
  }

  service = createMemoryService(config);
  try {
    const normalizedGlobalSpace = service.listSpaces({ id: 'default' }).items[0];
    assert(normalizedGlobalSpace?.name === 'Global', 'legacy default space name must be updated');
    assert(
      normalizedGlobalSpace.description ===
        'Broadly applicable preferences, working norms, and durable context shared across projects and domains. Context-specific information belongs in its own space.',
      'legacy default space description must be updated',
    );
    assert(service.getMemory(first.id).id === first.id, 'baseline memory must survive restore');
    let missing = false;
    try {
      service.getMemory(later.id);
    } catch {
      missing = true;
    }
    assert(missing, 'post-backup mutation must not survive restore');
  } finally {
    await service.close();
  }

  probeInvalidConfiguration();
  probeCliSurface();
  return {
    ok: true,
    backupRestore: true,
    restoreOwnershipGuard: true,
    invalidRestoreValidation: true,
    strictConfiguration: true,
    cliSurface: true,
  };
}

let outcome;
try {
  outcome = await run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
