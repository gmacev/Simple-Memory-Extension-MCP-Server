#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const npmCli = process.env.npm_execpath;
if (!npmCli) throw new Error('npm run verify must be launched through npm');
const checks = [
  [process.execPath, [npmCli, 'run', 'build']],
  [process.execPath, [npmCli, 'run', 'lint']],
  [process.execPath, ['scripts/protocol-probe.mjs']],
  [process.execPath, ['scripts/structured-output-probe.mjs']],
  [process.execPath, ['scripts/cross-space-links-probe.mjs']],
  [process.execPath, ['scripts/lexical-recall-probe.mjs']],
  [process.execPath, ['scripts/vector-metadata-probe.mjs']],
  [process.execPath, ['scripts/queue-probe.mjs']],
  [process.execPath, ['scripts/startup-index-drain-probe.mjs']],
  [process.execPath, ['scripts/embedding-generation-probe.mjs']],
  [process.execPath, ['scripts/embedding-upgrade-probe.mjs']],
  [process.execPath, ['scripts/inference-scheduler-probe.mjs']],
  [process.execPath, ['scripts/python-worker-protocol-probe.mjs']],
  [process.execPath, ['scripts/python-runtime-optimization-probe.mjs']],
  [process.execPath, ['scripts/rerank-cache-probe.mjs']],
  [process.execPath, ['scripts/retrieval-eval-probe.mjs']],
  [process.execPath, ['scripts/http-probe.mjs']],
  [process.execPath, ['scripts/operations-probe.mjs']],
  [process.execPath, ['scripts/live-probe.mjs', '--models-disabled']],
  [process.execPath, ['scripts/load-probe.mjs', '--models-disabled']],
];

for (const [command, arguments_] of checks) {
  process.stdout.write(
    `\n[simple-memory verify] ${path.basename(command)} ${arguments_.join(' ')}\n`,
  );
  const result = spawnSync(command, arguments_, { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

process.stdout.write('\n[simple-memory verify] All checks passed\n');
