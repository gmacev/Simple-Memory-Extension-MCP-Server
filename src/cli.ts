#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createMemoryService } from './application/create-service.js';
import { loadConfig } from './config.js';

function print(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function main(): Promise<void> {
  const [, , command = 'doctor', subcommand, argument] = process.argv;
  const config = loadConfig();
  const service = createMemoryService(config);
  try {
    if (command === 'doctor') {
      const status = await service.status(false);
      print(
        config.modelsEnabled
          ? { ...status, modelProbe: await service.warmModels() }
          : { ...status, modelProbe: { skipped: 'models disabled' } },
      );
      return;
    }
    if (command === 'model' && subcommand === 'fetch') {
      print(await service.warmModels());
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
      'Usage: memoryctl doctor | model fetch | reindex | export [file] | compact | purge --deleted',
    );
  } finally {
    await service.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`${String(error)}\n`);
  process.exitCode = 1;
});
