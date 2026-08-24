#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const managedPython = path.join(
  root,
  'python',
  '.venv',
  process.platform === 'win32' ? 'Scripts' : 'bin',
  process.platform === 'win32' ? 'python.exe' : 'python',
);
const python =
  process.env.SIMPLE_MEMORY_PROBE_PYTHON?.trim() ||
  (existsSync(managedPython) ? managedPython : process.platform === 'win32' ? 'python' : 'python3');
const sourcePath = path.join(root, 'python', 'src');
const result = spawnSync(
  python,
  [path.join(root, 'python', 'tests', 'runtime_optimization_probe.py')],
  {
    cwd: root,
    encoding: 'utf8',
    stdio: 'inherit',
    env: {
      ...process.env,
      PYTHONPATH: process.env.PYTHONPATH
        ? `${sourcePath}${path.delimiter}${process.env.PYTHONPATH}`
        : sourcePath,
    },
  },
);
if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
