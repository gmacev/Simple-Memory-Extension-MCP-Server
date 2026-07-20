#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createMemoryService } from './application/create-service.js';
import { loadConfig } from './config.js';

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
  const [, , command = 'doctor', subcommand, argument] = process.argv;
  const config = loadConfig();
  const showsModelProgress =
    (command === 'doctor' && config.modelsEnabled) ||
    (command === 'model' && subcommand === 'fetch');
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
      print(
        await withModelProgress((reportProgress) => service.warmModels(reportProgress)),
      );
      return;
    }
    if (command === 'migrate') {
      print(service.migrationStatus());
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
    throw new Error(
      'Usage: memoryctl doctor | model fetch | migrate | reindex | export [file] | compact | purge --deleted',
    );
  } finally {
    await service.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
