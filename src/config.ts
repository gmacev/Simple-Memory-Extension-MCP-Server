import { homedir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { type AccessConfiguration, parseFixedAccess } from './access/authorization.js';

export const DEFAULT_EMBEDDING_MODEL = 'codefuse-ai/F2LLM-v2-330M';
export const DEFAULT_EMBEDDING_REVISION = '1b8f03017b9f12220a3ab3a1d0b1fbe441cede93';
export const DEFAULT_EMBEDDING_DIMENSION = 896;
export const DEFAULT_QUERY_INSTRUCTION =
  'Given a memory query, retrieve stored information useful for answering the query or guiding an action.';
export const DEFAULT_RERANKER_MODEL = 'Qwen/Qwen3-Reranker-0.6B';
export const DEFAULT_RERANKER_REVISION = 'e61197ed45024b0ed8a2d74b80b4d909f1255473';
export const DEFAULT_RERANK_INSTRUCTION =
  'Given a memory query, determine whether the candidate memory contains information useful for answering or acting on it.';

export type TransportMode = 'stdio' | 'http';

export interface AppConfig {
  dataDir: string;
  databasePath: string;
  pythonProjectPath: string;
  pythonExecutablePath: string;
  transport: TransportMode;
  httpHost: string;
  httpPort: number;
  httpAllowedOrigins: string | undefined;
  modelsEnabled: boolean;
  device: string;
  localFilesOnly: boolean;
  embeddingModel: string;
  embeddingRevision: string;
  queryInstruction: string;
  rerankerModel: string;
  rerankerRevision: string;
  rerankInstruction: string;
  embeddingDimension: number;
  embeddingBatchSize: number;
  rerankBatchSize: number;
  modelTimeoutMs: number;
  inferenceQueueLimit: number;
  inferenceQueueTimeoutMs: number;
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
  const value = process.env[name];
  if (value === undefined) return fallback;
  if (!/^\d+$/u.test(value))
    throw new Error(`${name} must be a positive integer; received ${value}`);
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer; received ${value}`);
  }
  return parsed;
}

function portEnvironment(): number {
  const port = integerEnvironment('SIMPLE_MEMORY_HTTP_PORT', 3_000);
  if (port > 65_535) {
    throw new Error(`SIMPLE_MEMORY_HTTP_PORT must be from 1 to 65535; received ${String(port)}`);
  }
  return port;
}

function booleanEnvironment(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined) return fallback;
  if (value === 'true' || value === '1') return true;
  if (value === 'false' || value === '0') return false;
  throw new Error(`${name} must be true, false, 1, or 0`);
}

function nonEmptyEnvironment(name: string, fallback: string): string {
  const value = process.env[name];
  if (value === undefined) return fallback;
  if (!value.trim()) throw new Error(`${name} must not be empty`);
  return value;
}

function enumEnvironment<const T extends readonly string[]>(
  name: string,
  values: T,
  fallback: T[number],
): T[number] {
  const configured = process.env[name];
  if (configured === undefined) return fallback;
  if ((values as readonly string[]).includes(configured)) return configured as T[number];
  throw new Error(`${name} must be one of ${values.join(', ')}; received ${configured}`);
}

function deviceEnvironment(): string {
  const device = nonEmptyEnvironment('SIMPLE_MEMORY_DEVICE', 'auto').trim().toLowerCase();
  if (/^(?:auto|cpu|mps|cuda(?::\d+)?|xpu(?::\d+)?)$/u.test(device)) return device;
  throw new Error(
    `SIMPLE_MEMORY_DEVICE must be auto, cpu, mps, cuda[:index], or xpu[:index]; received ${device}`,
  );
}

function httpOriginsEnvironment(): string | undefined {
  const configured = process.env.SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS;
  if (configured === undefined) return undefined;
  const values = configured
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (values.length === 0) {
    throw new Error('SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS must contain at least one origin');
  }
  for (const value of values) {
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      throw new Error(`SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS contains an invalid URL: ${value}`);
    }
    if (
      (url.protocol !== 'http:' && url.protocol !== 'https:') ||
      url.username ||
      url.password ||
      url.pathname !== '/' ||
      url.search ||
      url.hash
    ) {
      throw new Error(`SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS must contain origins only: ${value}`);
    }
  }
  return configured;
}

function secureServiceUrl(value: string, name: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be a valid URL`);
  }
  const loopback =
    url.hostname === '127.0.0.1' || url.hostname === 'localhost' || url.hostname === '[::1]';
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && loopback)) {
    throw new Error(`${name} must use HTTPS outside loopback development`);
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error(`${name} must not contain credentials, a query, or a fragment`);
  }
  return url;
}

function isLoopbackHost(host: string): boolean {
  const normalized = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
  return normalized === '127.0.0.1' || normalized === 'localhost' || normalized === '::1';
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
    oauthAccessClaim:
      process.env.SIMPLE_MEMORY_OAUTH_ACCESS_CLAIM?.trim() || 'simple_memory_access',
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
    const parsedPublicUrl = secureServiceUrl(publicUrl, 'SIMPLE_MEMORY_HTTP_PUBLIC_URL');
    secureServiceUrl(issuer, 'SIMPLE_MEMORY_OAUTH_ISSUER');
    if (parsedPublicUrl.pathname !== '/mcp') {
      throw new Error('SIMPLE_MEMORY_HTTP_PUBLIC_URL must identify the /mcp endpoint');
    }
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
  const transport = enumEnvironment('SIMPLE_MEMORY_TRANSPORT', ['stdio', 'http'] as const, 'stdio');
  const logLevel = enumEnvironment(
    'SIMPLE_MEMORY_LOG_LEVEL',
    ['debug', 'info', 'warn', 'error'] as const,
    'info',
  );
  const modelMode = enumEnvironment(
    'SIMPLE_MEMORY_MODELS',
    ['enabled', 'disabled'] as const,
    'enabled',
  );
  const access = accessConfiguration();
  if (access.mode === 'fixed' && transport !== 'stdio') {
    throw new Error('SIMPLE_MEMORY_ACCESS_MODE=fixed requires SIMPLE_MEMORY_TRANSPORT=stdio');
  }
  if (access.mode === 'oauth' && transport !== 'http') {
    throw new Error('SIMPLE_MEMORY_ACCESS_MODE=oauth requires SIMPLE_MEMORY_TRANSPORT=http');
  }
  const httpHost = nonEmptyEnvironment('SIMPLE_MEMORY_HTTP_HOST', '127.0.0.1').trim();
  const httpPort = portEnvironment();
  const httpAllowedOrigins = httpOriginsEnvironment();
  if (transport === 'http' && (httpHost === '0.0.0.0' || httpHost === '::')) {
    if (httpAllowedOrigins === undefined) {
      throw new Error(
        'SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS is required when HTTP binds to a wildcard interface',
      );
    }
  }
  if (
    transport === 'http' &&
    access.mode === 'open' &&
    !isLoopbackHost(httpHost) &&
    !access.allowUnauthenticatedNonLoopback
  ) {
    throw new Error(
      'Open HTTP access may only bind to loopback. Use SIMPLE_MEMORY_ACCESS_MODE=oauth, or explicitly set SIMPLE_MEMORY_HTTP_ALLOW_UNAUTHENTICATED_NON_LOOPBACK=true only behind a trusted external security boundary.',
    );
  }
  return {
    dataDir,
    databasePath: path.resolve(
      process.env.SIMPLE_MEMORY_DB_PATH ?? path.join(dataDir, 'memory.db'),
    ),
    pythonProjectPath,
    pythonExecutablePath: path.resolve(
      process.env.SIMPLE_MEMORY_PYTHON ?? defaultPythonExecutable(pythonProjectPath),
    ),
    transport,
    httpHost,
    httpPort,
    httpAllowedOrigins,
    modelsEnabled: modelMode === 'enabled',
    device: deviceEnvironment(),
    localFilesOnly: booleanEnvironment('SIMPLE_MEMORY_LOCAL_FILES_ONLY', false),
    embeddingModel: nonEmptyEnvironment('SIMPLE_MEMORY_EMBEDDING_MODEL', DEFAULT_EMBEDDING_MODEL),
    embeddingRevision: nonEmptyEnvironment(
      'SIMPLE_MEMORY_EMBEDDING_REVISION',
      DEFAULT_EMBEDDING_REVISION,
    ),
    queryInstruction: nonEmptyEnvironment(
      'SIMPLE_MEMORY_QUERY_INSTRUCTION',
      DEFAULT_QUERY_INSTRUCTION,
    ),
    rerankerModel: nonEmptyEnvironment('SIMPLE_MEMORY_RERANKER_MODEL', DEFAULT_RERANKER_MODEL),
    rerankerRevision: nonEmptyEnvironment(
      'SIMPLE_MEMORY_RERANKER_REVISION',
      DEFAULT_RERANKER_REVISION,
    ),
    rerankInstruction: nonEmptyEnvironment(
      'SIMPLE_MEMORY_RERANK_INSTRUCTION',
      DEFAULT_RERANK_INSTRUCTION,
    ),
    embeddingDimension: integerEnvironment(
      'SIMPLE_MEMORY_EMBEDDING_DIMENSION',
      DEFAULT_EMBEDDING_DIMENSION,
    ),
    embeddingBatchSize: integerEnvironment('SIMPLE_MEMORY_EMBED_BATCH_SIZE', 8),
    rerankBatchSize: integerEnvironment('SIMPLE_MEMORY_RERANK_BATCH_SIZE', 4),
    modelTimeoutMs: integerEnvironment('SIMPLE_MEMORY_MODEL_TIMEOUT_MS', 600_000),
    inferenceQueueLimit: integerEnvironment('SIMPLE_MEMORY_INFERENCE_QUEUE_LIMIT', 128),
    inferenceQueueTimeoutMs: integerEnvironment('SIMPLE_MEMORY_INFERENCE_QUEUE_TIMEOUT_MS', 30_000),
    rerankCandidates: integerEnvironment('SIMPLE_MEMORY_RERANK_CANDIDATES', 30),
    lexicalCandidates: integerEnvironment('SIMPLE_MEMORY_LEXICAL_CANDIDATES', 100),
    semanticCandidates: integerEnvironment('SIMPLE_MEMORY_SEMANTIC_CANDIDATES', 100),
    logLevel,
    access,
  };
}

export function publicConfig(config: AppConfig): Record<string, unknown> {
  return {
    dataDir: config.dataDir,
    databasePath: config.databasePath,
    pythonProjectPath: config.pythonProjectPath,
    pythonExecutablePath: config.pythonExecutablePath,
    transport: config.transport,
    http: {
      host: config.httpHost,
      port: config.httpPort,
      allowedOrigins: config.httpAllowedOrigins ?? null,
    },
    models: {
      enabled: config.modelsEnabled,
      device: config.device,
      localFilesOnly: config.localFilesOnly,
      embeddingModel: config.embeddingModel,
      embeddingRevision: config.embeddingRevision,
      embeddingDimension: config.embeddingDimension,
      queryInstruction: config.queryInstruction,
      rerankerModel: config.rerankerModel,
      rerankerRevision: config.rerankerRevision,
      rerankInstruction: config.rerankInstruction,
      embeddingBatchSize: config.embeddingBatchSize,
      rerankBatchSize: config.rerankBatchSize,
    },
    retrieval: {
      lexicalCandidates: config.lexicalCandidates,
      semanticCandidates: config.semanticCandidates,
      rerankCandidates: config.rerankCandidates,
    },
    inference: {
      modelTimeoutMs: config.modelTimeoutMs,
      queueLimit: config.inferenceQueueLimit,
      queueTimeoutMs: config.inferenceQueueTimeoutMs,
    },
    access: {
      mode: config.access.mode,
      fixedPrincipal: config.access.fixedPrincipal ?? null,
      fixedGrants: config.access.fixedGrants ?? null,
      httpPublicUrl: config.access.httpPublicUrl ?? null,
      oauthIssuer: config.access.oauthIssuer ?? null,
      oauthAudience: config.access.oauthAudience ?? null,
      oauthAccessClaim: config.access.oauthAccessClaim,
      allowUnauthenticatedNonLoopback: config.access.allowUnauthenticatedNonLoopback,
    },
    logLevel: config.logLevel,
  };
}
