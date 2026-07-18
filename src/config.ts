import { homedir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface AppConfig {
  dataDir: string;
  databasePath: string;
  pythonProjectPath: string;
  pythonExecutablePath: string;
  modelsEnabled: boolean;
  embeddingDimension: number;
  modelTimeoutMs: number;
  rerankCandidates: number;
  lexicalCandidates: number;
  semanticCandidates: number;
  logLevel: 'debug' | 'info' | 'warn' | 'error';
}

function defaultDataDir(): string {
  if (process.platform === 'win32') {
    return path.join(
      process.env.LOCALAPPDATA ?? path.join(homedir(), 'AppData', 'Local'),
      'simple-memory',
    );
  }
  if (process.platform === 'darwin') {
    return path.join(homedir(), 'Library', 'Application Support', 'simple-memory');
  }
  return path.join(
    process.env.XDG_DATA_HOME ?? path.join(homedir(), '.local', 'share'),
    'simple-memory',
  );
}

function defaultPythonExecutable(pythonProjectPath: string): string {
  return path.join(
    pythonProjectPath,
    '.venv',
    process.platform === 'win32' ? 'Scripts' : 'bin',
    process.platform === 'win32' ? 'python.exe' : 'python',
  );
}

function integerEnvironment(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function loadConfig(): AppConfig {
  const dataDir = path.resolve(process.env.SIMPLE_MEMORY_DATA_DIR ?? defaultDataDir());
  const pythonProjectPath = path.resolve(
    process.env.SIMPLE_MEMORY_PYTHON_PROJECT ??
      fileURLToPath(new URL('../python', import.meta.url)),
  );
  const configuredLevel = process.env.SIMPLE_MEMORY_LOG_LEVEL;
  const logLevel =
    configuredLevel === 'debug' ||
    configuredLevel === 'info' ||
    configuredLevel === 'warn' ||
    configuredLevel === 'error'
      ? configuredLevel
      : 'info';
  return {
    dataDir,
    databasePath: path.resolve(
      process.env.SIMPLE_MEMORY_DB_PATH ?? path.join(dataDir, 'memory.db'),
    ),
    pythonProjectPath,
    pythonExecutablePath: path.resolve(
      process.env.SIMPLE_MEMORY_PYTHON ?? defaultPythonExecutable(pythonProjectPath),
    ),
    modelsEnabled: process.env.SIMPLE_MEMORY_MODELS !== 'disabled',
    embeddingDimension: integerEnvironment('SIMPLE_MEMORY_EMBEDDING_DIMENSION', 1024),
    modelTimeoutMs: integerEnvironment('SIMPLE_MEMORY_MODEL_TIMEOUT_MS', 600_000),
    rerankCandidates: integerEnvironment('SIMPLE_MEMORY_RERANK_CANDIDATES', 30),
    lexicalCandidates: integerEnvironment('SIMPLE_MEMORY_LEXICAL_CANDIDATES', 100),
    semanticCandidates: integerEnvironment('SIMPLE_MEMORY_SEMANTIC_CANDIDATES', 100),
    logLevel,
  };
}
