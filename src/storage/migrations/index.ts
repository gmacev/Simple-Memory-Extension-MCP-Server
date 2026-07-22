import { createHash } from 'node:crypto';
import type Database from 'better-sqlite3';
import * as z from 'zod/v4';
import type { Logger } from '../../logger.js';
import { schemaSql } from '../schema.js';
import { linkTraversalIndexesSql } from './002-link-traversal-indexes.js';
import { revisionAwareFeedbackSql } from './003-revision-aware-feedback.js';
import { logicalIdentityAndMergesSql } from './004-logical-identity-and-merges.js';

interface Migration {
  version: number;
  name: string;
  sql: string;
}

export interface MigrationStatus {
  fromVersion: number;
  toVersion: number;
  applied: Array<{ version: number; name: string }>;
  backupPath: string | null;
}

const migrationRowSchema = z.object({
  version: z.number().int().positive(),
  name: z.string(),
  checksum: z.string(),
});

const migrations = [
  { version: 1, name: 'initial-schema', sql: schemaSql },
  { version: 2, name: 'link-traversal-indexes', sql: linkTraversalIndexesSql },
  { version: 3, name: 'revision-aware-feedback', sql: revisionAwareFeedbackSql },
  { version: 4, name: 'logical-identity-and-merges', sql: logicalIdentityAndMergesSql },
] satisfies readonly Migration[];

export const currentSchemaVersion = migrations.at(-1)?.version ?? 0;

const migrationTableSql = `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);
`;

function checksum(sql: string): string {
  const normalized = sql.replace(/\r\n?/gu, '\n').trim();
  return createHash('sha256').update(normalized, 'utf8').digest('hex');
}

function tableExists(database: Database.Database, name: string): boolean {
  return (
    database
      .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
      .get(name) !== undefined
  );
}

function appliedMigrations(database: Database.Database): Map<number, z.infer<typeof migrationRowSchema>> {
  if (!tableExists(database, 'schema_migrations')) return new Map();
  const rows = z
    .array(migrationRowSchema)
    .parse(database.prepare('SELECT version, name, checksum FROM schema_migrations').all());
  return new Map(rows.map((row) => [row.version, row]));
}

function validateAppliedMigrations(
  applied: Map<number, z.infer<typeof migrationRowSchema>>,
): void {
  const known = new Map(migrations.map((migration) => [migration.version, migration]));
  for (const row of applied.values()) {
    const migration = known.get(row.version);
    if (!migration) {
      throw new Error(
        `Database schema version ${String(row.version)} is newer than this Simple Memory build`,
      );
    }
    if (row.name !== migration.name || row.checksum !== checksum(migration.sql)) {
      throw new Error(
        `Applied migration ${String(row.version)} (${row.name}) does not match this build`,
      );
    }
  }
}

function createBackup(
  database: Database.Database,
  databasePath: string,
  fromVersion: number,
  toVersion: number,
): string {
  const timestamp = new Date().toISOString().replaceAll(':', '-');
  const backupPath = `${databasePath}.backup-v${String(fromVersion)}-to-v${String(toVersion)}-${timestamp}`;
  const escapedPath = backupPath.replaceAll("'", "''");
  database.exec(`VACUUM INTO '${escapedPath}'`);
  return backupPath;
}

export function applyMigrations(
  database: Database.Database,
  databasePath: string,
  logger: Logger,
): MigrationStatus {
  const applied = appliedMigrations(database);
  validateAppliedMigrations(applied);
  const fromVersion = Math.max(0, ...applied.keys());
  const pending = migrations.filter((migration) => !applied.has(migration.version));
  if (pending.length === 0) {
    return { fromVersion, toVersion: fromVersion, applied: [], backupPath: null };
  }

  const hasExistingSchema = tableExists(database, 'memories');
  const backupPath = hasExistingSchema
    ? createBackup(database, databasePath, fromVersion, currentSchemaVersion)
    : null;
  if (backupPath) logger.info('Created pre-migration database backup', { backupPath });

  const appliedAt = new Date().toISOString();
  const migrate = database.transaction(() => {
    database.exec(migrationTableSql);
    const record = database.prepare(
      `INSERT INTO schema_migrations(version, name, checksum, applied_at)
       VALUES (?, ?, ?, ?)`,
    );
    for (const migration of pending) {
      database.exec(migration.sql);
      record.run(migration.version, migration.name, checksum(migration.sql), appliedAt);
    }
  });
  migrate.immediate();
  logger.info('Applied database migrations', {
    fromVersion,
    toVersion: currentSchemaVersion,
    migrations: pending.map((migration) => migration.name),
  });
  return {
    fromVersion,
    toVersion: currentSchemaVersion,
    applied: pending.map(({ version, name }) => ({ version, name })),
    backupPath,
  };
}
