import { access, copyFile, mkdir, readdir, rename, rm, stat } from 'node:fs/promises';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import type { AppConfig } from '../config.js';
import { Logger } from '../logger.js';
import { applyMigrations } from '../storage/migrations/index.js';
import { acquireDatabaseRestoreLease } from './database-runtime.js';

export interface DatabaseBackupResult {
  path: string;
  bytes: number;
  schemaVersion: number;
  createdAt: string;
}

export interface DatabaseRestoreResult {
  restoredFrom: string;
  databasePath: string;
  safetyBackupPath: string | null;
  schemaVersion: number;
}

function timestamp(): string {
  return new Date().toISOString().replaceAll(':', '-');
}

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function defaultBackupPath(databasePath: string): string {
  return `${databasePath}.backup-${timestamp()}`;
}

function inspectOpenDatabase(database: Database.Database): number {
  sqliteVec.load(database);
  database.pragma('foreign_keys = ON');
  database.pragma('busy_timeout = 5000');
  const quickCheck = database.pragma('quick_check', { simple: true });
  if (quickCheck !== 'ok') throw new Error(`SQLite integrity check failed: ${String(quickCheck)}`);
  const table = database
    .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'")
    .get();
  if (!table) throw new Error('The file is not a Simple Memory database');
  const row = database.prepare('SELECT MAX(version) AS version FROM schema_migrations').get() as {
    version: number | null;
  };
  if (!Number.isInteger(row.version) || !row.version) {
    throw new Error('The Simple Memory database has no schema version');
  }
  return row.version;
}

export async function backupDatabase(
  databasePath: string,
  destination = defaultBackupPath(databasePath),
): Promise<DatabaseBackupResult> {
  const source = path.resolve(databasePath);
  const target = path.resolve(destination);
  if (source === target) throw new Error('Backup destination must differ from the active database');
  await mkdir(path.dirname(target), { recursive: true });
  await access(target)
    .then(() => {
      throw new Error(`Backup destination already exists: ${target}`);
    })
    .catch((error: unknown) => {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return;
      throw error;
    });
  const database = new Database(source, { readonly: true, fileMustExist: true });
  let schemaVersion: number;
  try {
    schemaVersion = inspectOpenDatabase(database);
    database.exec(`VACUUM INTO ${sqlString(target)}`);
  } catch (error) {
    await rm(target, { force: true });
    throw error;
  } finally {
    database.close();
  }
  const details = await stat(target);
  return {
    path: target,
    bytes: details.size,
    schemaVersion,
    createdAt: new Date().toISOString(),
  };
}

async function removeDatabaseFiles(databasePath: string): Promise<void> {
  for (const file of [`${databasePath}-wal`, `${databasePath}-shm`, databasePath]) {
    await rm(file, { force: true });
  }
}

async function removeDatabaseSidecars(databasePath: string): Promise<void> {
  for (const file of [`${databasePath}-wal`, `${databasePath}-shm`]) {
    await rm(file, { force: true });
  }
}

async function removeMigrationBackups(databasePath: string): Promise<void> {
  const directory = path.dirname(databasePath);
  const prefix = `${path.basename(databasePath)}.backup-`;
  let entries: Array<import('node:fs').Dirent> = [];
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return;
    throw error;
  }
  await Promise.all(
    entries
      .filter((entry) => entry.isFile() && entry.name.startsWith(prefix))
      .map((entry) => rm(path.join(directory, entry.name), { force: true })),
  );
}

async function removeStagedDatabase(databasePath: string): Promise<void> {
  await Promise.all([removeDatabaseFiles(databasePath), removeMigrationBackups(databasePath)]);
}

export async function restoreDatabase(
  config: AppConfig,
  sourcePath: string,
): Promise<DatabaseRestoreResult> {
  const source = path.resolve(sourcePath);
  const target = path.resolve(config.databasePath);
  if (source === target) throw new Error('Restore source must differ from the active database');
  const restoreLease = await acquireDatabaseRestoreLease(target);
  try {
    await stat(source);
    await mkdir(path.dirname(target), { recursive: true });

    const sourceDatabase = new Database(source, { readonly: true, fileMustExist: true });
    try {
      inspectOpenDatabase(sourceDatabase);
    } catch (error) {
      throw new Error(`Restore source validation failed: ${String(error)}`, { cause: error });
    } finally {
      sourceDatabase.close();
    }

    const staged = `${target}.restore-staged-${timestamp()}`;
    // VACUUM INTO creates a consistent snapshot and includes committed WAL pages.
    // A raw copy of the main file alone can omit committed changes still present
    // in a source database's -wal sidecar.
    await backupDatabase(source, staged);
    let schemaVersion = 0;
    try {
      const stagedDatabase = new Database(staged);
      try {
        sqliteVec.load(stagedDatabase);
        stagedDatabase.pragma('foreign_keys = ON');
        stagedDatabase.pragma('busy_timeout = 5000');
        const migration = applyMigrations(stagedDatabase, staged, new Logger(config.logLevel));
        schemaVersion = migration.toVersion;
        inspectOpenDatabase(stagedDatabase);
      } finally {
        stagedDatabase.close();
      }
    } catch (error) {
      await removeStagedDatabase(staged).catch(() => undefined);
      throw new Error(`Restore source validation failed: ${String(error)}`, { cause: error });
    }
    await removeMigrationBackups(staged);

    let safetyBackupPath: string | null = null;
    try {
      await stat(target);
      safetyBackupPath = `${target}.before-restore-${timestamp()}`;
      await backupDatabase(target, safetyBackupPath);
    } catch (error) {
      const code = error instanceof Error && 'code' in error ? String(error.code) : '';
      if (code !== 'ENOENT') {
        await removeStagedDatabase(staged).catch(() => undefined);
        throw new Error(
          `Could not prepare the existing database for replacement. Stop every Simple Memory process and retry: ${String(error)}`,
          { cause: error },
        );
      }
    }

    try {
      // POSIX rename replaces the main file atomically. Windows cannot rename
      // over an existing file, so it falls back to removal after the safety
      // snapshot has been created.
      await removeDatabaseSidecars(target);
      try {
        await rename(staged, target);
      } catch (error) {
        await rm(target, { force: true });
        await rename(staged, target).catch((replacementError) => {
          throw new Error(`Could not replace the active database: ${String(replacementError)}`, {
            cause: error,
          });
        });
      }
    } catch (error) {
      await removeStagedDatabase(staged).catch(() => undefined);
      let rollbackError: unknown = null;
      try {
        await removeDatabaseFiles(target);
        if (safetyBackupPath) {
          await copyFile(safetyBackupPath, target);
          const recovered = new Database(target, { readonly: true, fileMustExist: true });
          try {
            inspectOpenDatabase(recovered);
          } finally {
            recovered.close();
          }
        }
      } catch (restoreError) {
        rollbackError = restoreError;
      }
      const recovery = rollbackError
        ? ` Rollback failed; the safety backup remains at ${safetyBackupPath ?? 'the original source location'}.`
        : '';
      throw new Error(`Database replacement failed: ${String(error)}.${recovery}`, {
        cause: error,
      });
    }

    return { restoredFrom: source, databasePath: target, safetyBackupPath, schemaVersion };
  } finally {
    await restoreLease.release();
  }
}
