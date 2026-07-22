import { homedir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  type AccessConfiguration,
  parseFixedAccess,
} from './access/authorization.js';

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
  access: AccessConfiguration;
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

function booleanEnvironment(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined) return fallback;
  if (value === 'true' || value === '1') return true;
  if (value === 'false' || value === '0') return false;
  throw new Error(`${name} must be true, false, 1, or 0`);
}

function accessConfiguration(): AccessConfiguration {
  if (process.env.SIMPLE_MEMORY_HTTP_TOKEN !== undefined) {
    throw new Error(
      'SIMPLE_MEMORY_HTTP_TOKEN is no longer supported. Use SIMPLE_MEMORY_ACCESS_MODE=oauth for protected HTTP, or remove the variable for trusted loopback-only open mode.',
    );
  }
  const configuredMode = process.env.SIMPLE_MEMORY_ACCESS_MODE ?? 'open';
  if (configuredMode !== 'open' && configuredMode !== 'fixed' && configuredMode !== 'oauth') {
    throw new Error('SIMPLE_MEMORY_ACCESS_MODE must be open, fixed, or oauth');
  }
  const mode = configuredMode;
  const access: AccessConfiguration = {
    mode,
    oauthAccessClaim: process.env.SIMPLE_MEMORY_OAUTH_ACCESS_CLAIM?.trim() || 'simple_memory_access',
    allowUnauthenticatedNonLoopback: booleanEnvironment(
      'SIMPLE_MEMORY_HTTP_ALLOW_UNAUTHENTICATED_NON_LOOPBACK',
      false,
    ),
  };
  if (mode === 'fixed') {
    const principal = process.env.SIMPLE_MEMORY_FIXED_PRINCIPAL?.trim();
    const fixedAccess = process.env.SIMPLE_MEMORY_FIXED_ACCESS?.trim();
    if (!principal) throw new Error('SIMPLE_MEMORY_FIXED_PRINCIPAL is required in fixed mode');
    if (!fixedAccess) throw new Error('SIMPLE_MEMORY_FIXED_ACCESS is required in fixed mode');
    access.fixedPrincipal = principal;
    access.fixedGrants = parseFixedAccess(fixedAccess);
  }
  if (mode === 'oauth') {
    const publicUrl = process.env.SIMPLE_MEMORY_HTTP_PUBLIC_URL?.trim();
    const issuer = process.env.SIMPLE_MEMORY_OAUTH_ISSUER?.trim();
    if (!publicUrl) throw new Error('SIMPLE_MEMORY_HTTP_PUBLIC_URL is required in oauth mode');
    if (!issuer) throw new Error('SIMPLE_MEMORY_OAUTH_ISSUER is required in oauth mode');
    access.httpPublicUrl = publicUrl;
    access.oauthIssuer = issuer;
    access.oauthAudience = process.env.SIMPLE_MEMORY_OAUTH_AUDIENCE?.trim() || publicUrl;
  }
  return access;
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
    access: accessConfiguration(),
  };
}
