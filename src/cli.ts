#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createMemoryService } from './application/create-service.js';
import { loadConfig, publicConfig } from './config.js';
import { backupDatabase, restoreDatabase } from './operations/database-operations.js';
import { SIMPLE_MEMORY_VERSION } from './version.js';

function print(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function withModelProgress<T>(
  operation: (reportProgress: (message: string) => void) => Promise<T>,
): Promise<T> {
  let currentStage = 'Starting model worker';
  let stageStartedAt = Date.now();
  const reportProgress = (message: string): void => {
    currentStage = message;
    stageStartedAt = Date.now();
    process.stderr.write(`[simple-memory models] ${message}\n`);
  };
  const heartbeat = setInterval(() => {
    const elapsedSeconds = Math.floor((Date.now() - stageStartedAt) / 1_000);
    process.stderr.write(
      `[simple-memory models] Still working: ${currentStage} (${String(elapsedSeconds)}s elapsed)\n`,
    );
  }, 15_000);
  heartbeat.unref();
  try {
    return await operation(reportProgress);
  } finally {
    clearInterval(heartbeat);
  }
}

async function main(): Promise<void> {
  const arguments_ = process.argv.slice(2);
  const [command = 'doctor', subcommand, argument] = arguments_;
  if (command === '--help' || command === '-h' || command === 'help') {
    process.stdout.write(`Simple Memory administration CLI

Usage:
  memoryctl doctor
  memoryctl config validate
  memoryctl config show
  memoryctl backup [file]
  memoryctl restore <file> --confirm
  memoryctl model fetch
  memoryctl embedding upgrade
  memoryctl migrate
  memoryctl reindex
  memoryctl export [file]
  memoryctl compact
  memoryctl purge --deleted
  memoryctl --version
`);
    return;
  }
  if (command === '--version' || command === '-v') {
    process.stdout.write(`${SIMPLE_MEMORY_VERSION}\n`);
    return;
  }
  const config = loadConfig();
  if (command === 'config') {
    if (subcommand === 'validate') {
      print({ valid: true, version: SIMPLE_MEMORY_VERSION });
      return;
    }
    if (subcommand === 'show') {
      print(publicConfig(config));
      return;
    }
    throw new Error('Usage: memoryctl config validate | config show');
  }
  if (command === 'backup') {
    print(await backupDatabase(config.databasePath, subcommand && path.resolve(subcommand)));
    return;
  }
  if (command === 'restore') {
    if (!subcommand || !arguments_.includes('--confirm')) {
      throw new Error(
        'Usage: memoryctl restore <file> --confirm. Stop every Simple Memory process before restoring.',
      );
    }
    print(await restoreDatabase(config, subcommand));
    return;
  }
  const showsModelProgress =
    (command === 'doctor' && config.modelsEnabled) ||
    (command === 'model' && subcommand === 'fetch') ||
    (command === 'embedding' && subcommand === 'upgrade' && config.modelsEnabled);
  const service = createMemoryService(config, { forwardModelStderr: showsModelProgress });
  try {
    if (command === 'doctor') {
      const status = await service.status(false);
      print(
        config.modelsEnabled
          ? {
              ...status,
              modelProbe: await withModelProgress((reportProgress) =>
                service.warmModels(reportProgress),
              ),
            }
          : { ...status, modelProbe: { skipped: 'models disabled' } },
      );
      return;
    }
    if (command === 'model' && subcommand === 'fetch') {
      print(await withModelProgress((reportProgress) => service.warmModels(reportProgress)));
      return;
    }
    if (command === 'migrate') {
      print(service.migrationStatus());
      return;
    }
    if (command === 'embedding' && subcommand === 'upgrade') {
      print(
        await withModelProgress((reportProgress) => service.upgradeEmbeddingIndex(reportProgress)),
      );
      return;
    }
    if (command === 'reindex') {
      print(await service.reindexAll());
      return;
    }
    if (command === 'export') {
      const destination = path.resolve(subcommand ?? argument ?? 'simple-memory-export.json');
      await writeFile(destination, JSON.stringify(service.exportSnapshot(), null, 2), 'utf8');
      print({ exported: destination });
      return;
    }
    if (command === 'compact') {
      service.compact();
      print({ compacted: true });
      return;
    }
    if (command === 'purge' && subcommand === '--deleted') {
      print({ purged: service.purgeDeleted() });
      return;
    }
    throw new Error('Unknown command. Run memoryctl --help for available commands.');
  } finally {
    await service.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
