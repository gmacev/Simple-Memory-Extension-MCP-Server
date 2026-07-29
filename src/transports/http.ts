import {
  createMcpExpressApp,
  getOAuthProtectedResourceMetadataUrl,
  mcpAuthMetadataRouter,
  requireBearerAuth,
} from '@modelcontextprotocol/express';
import { toNodeHandler } from '@modelcontextprotocol/node';
import { createMcpHandler } from '@modelcontextprotocol/server';
import type { RequestHandler } from 'express';
import type { Server as HttpServer } from 'node:http';
import {
  memoryScopes,
  type MemoryScope,
  type AuthorizationService,
  type SpaceAccessLevel,
} from '../access/authorization.js';
import { createOAuthRuntime } from '../access/oauth.js';
import type { MemoryService } from '../application/memory-service.js';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { buildMcpServer, mcpToolAccessLevels } from '../mcp/server.js';

export interface HttpServerHandle {
  close(): Promise<void>;
}

function isLoopback(host: string): boolean {
  const normalized = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
  return normalized === '127.0.0.1' || normalized === 'localhost' || normalized === '::1';
}

function requireSecureRemoteUrl(value: string, label: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${label} must be a valid URL`);
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error(`${label} must not contain credentials, a query, or a fragment`);
  }
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && isLoopback(url.hostname))) {
    throw new Error(`${label} must use HTTPS outside loopback development`);
  }
  return url;
}

function normalizeOrigin(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`Invalid HTTP origin: ${value}`);
  }
  if (
    (url.protocol !== 'http:' && url.protocol !== 'https:') ||
    url.username !== '' ||
    url.password !== '' ||
    url.pathname !== '/' ||
    url.search !== '' ||
    url.hash !== ''
  ) {
    throw new Error(`HTTP origins must contain only an http(s) scheme, host, and port: ${value}`);
  }
  return url.origin;
}

function urlHost(host: string): string {
  return host.includes(':') && !host.startsWith('[') ? `[${host}]` : host;
}

function allowedOrigins(host: string, port: number): string[] {
  const configured = process.env.SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS;
  if (configured !== undefined) {
    const origins = [
      ...new Set(
        configured
          .split(',')
          .map((value) => value.trim())
          .filter((value) => value.length > 0)
          .map(normalizeOrigin),
      ),
    ];
    if (origins.length === 0) {
      throw new Error('SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS must contain at least one origin');
    }
    return origins;
  }
  if (host === '0.0.0.0' || host === '::') {
    throw new Error(
      'SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS is required when HTTP binds to a wildcard interface',
    );
  }
  if (isLoopback(host)) {
    return [
      normalizeOrigin(`http://127.0.0.1:${String(port)}`),
      normalizeOrigin(`http://localhost:${String(port)}`),
      normalizeOrigin(`http://[::1]:${String(port)}`),
    ];
  }
  return [normalizeOrigin(`http://${urlHost(host)}:${String(port)}`)];
}

function decodeMcpHeader(value: string): string | null {
  const prefix = '=?base64?';
  const suffix = '?=';
  if (!value.startsWith(prefix) || !value.endsWith(suffix)) return value;
  const encoded = value.slice(prefix.length, -suffix.length);
  try {
    return Buffer.from(encoded, 'base64').toString('utf8');
  } catch {
    return null;
  }
}

function scopeForLevel(level: SpaceAccessLevel): MemoryScope {
  if (level === 'manage') return 'memory:manage';
  if (level === 'write') return 'memory:write';
  return 'memory:read';
}

function toolAccessLevel(name: string): SpaceAccessLevel | null {
  for (const [toolName, level] of Object.entries(mcpToolAccessLevels)) {
    if (toolName === name) return level;
  }
  return null;
}

function requiredScope(request: Parameters<RequestHandler>[0]): MemoryScope {
  if (request.header('Mcp-Method') !== 'tools/call') return 'memory:read';
  const rawName = request.header('Mcp-Name');
  if (!rawName) return 'memory:read';
  const name = decodeMcpHeader(rawName);
  if (!name) return 'memory:read';
  const level = toolAccessLevel(name);
  return level ? scopeForLevel(level) : 'memory:read';
}

function closeHttpServer(server: HttpServer): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

export async function startHttpServer(
  config: AppConfig,
  service: MemoryService,
  authorization: AuthorizationService,
  logger: Logger,
): Promise<HttpServerHandle> {
  const host = process.env.SIMPLE_MEMORY_HTTP_HOST ?? '127.0.0.1';
  const port = Number.parseInt(process.env.SIMPLE_MEMORY_HTTP_PORT ?? '3000', 10);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error('SIMPLE_MEMORY_HTTP_PORT must be an integer from 1 to 65535');
  }
  const origins = allowedOrigins(host, port);
  if (
    config.access.mode === 'open' &&
    !isLoopback(host) &&
    !config.access.allowUnauthenticatedNonLoopback
  ) {
    throw new Error(
      'Open HTTP access may only bind to loopback. Use SIMPLE_MEMORY_ACCESS_MODE=oauth, or explicitly set SIMPLE_MEMORY_HTTP_ALLOW_UNAUTHENTICATED_NON_LOOPBACK=true only behind a trusted external security boundary.',
    );
  }
  if (config.access.mode === 'open' && !isLoopback(host)) {
    logger.warn('Unauthenticated Streamable HTTP is enabled on a non-loopback interface', { host });
  }

  const app = createMcpExpressApp({
    host,
    allowedOrigins: [...new Set(origins.map((origin) => new URL(origin).hostname))],
    jsonLimit: '2mb',
  });
  app.use((request, response, next) => {
    const origin = request.headers.origin;
    if (origin === undefined || origins.includes(origin)) {
      next();
      return;
    }
    response.status(403).json({
      jsonrpc: '2.0',
      error: { code: -32000, message: 'Forbidden: invalid Origin header' },
      id: null,
    });
  });

  if (config.access.mode === 'oauth') {
    if (!config.access.httpPublicUrl || !config.access.oauthIssuer) {
      throw new Error('OAuth HTTP configuration is incomplete');
    }
    const publicUrl = requireSecureRemoteUrl(
      config.access.httpPublicUrl,
      'SIMPLE_MEMORY_HTTP_PUBLIC_URL',
    );
    if (publicUrl.pathname !== '/mcp') {
      throw new Error('SIMPLE_MEMORY_HTTP_PUBLIC_URL must identify the /mcp endpoint');
    }
    const issuer = requireSecureRemoteUrl(
      config.access.oauthIssuer,
      'SIMPLE_MEMORY_OAUTH_ISSUER',
    );
    const oauth = await createOAuthRuntime(config.access);
    const resourceMetadataUrl = getOAuthProtectedResourceMetadataUrl(publicUrl);
    app.use(
      mcpAuthMetadataRouter({
        oauthMetadata: oauth.metadata,
        resourceServerUrl: publicUrl,
        scopesSupported: [...memoryScopes],
        resourceName: 'Simple Memory',
        dangerouslyAllowInsecureIssuerUrl: issuer.protocol === 'http:',
      }),
    );
    const scopeMiddleware = new Map<MemoryScope, RequestHandler>(
      memoryScopes.map((scope) => [
        scope,
        requireBearerAuth({
          verifier: oauth.verifier,
          requiredScopes: [scope],
          resourceMetadataUrl,
        }),
      ]),
    );
    app.use('/mcp', (request, response, next) => {
      const middleware = scopeMiddleware.get(requiredScope(request));
      if (!middleware) {
        response.status(500).json({ error: 'server_error' });
        return;
      }
      middleware(request, response, next);
    });
  }

  const handler = createMcpHandler(
    ({ authInfo }) =>
      buildMcpServer(service, authorization, authorization.context(authInfo)),
    {
      legacy: 'stateless',
      responseMode: 'auto',
      onerror: (error) => logger.error('MCP HTTP request failed', { error: error.message }),
    },
  );
  const nodeHandler = toNodeHandler(handler, {
    onerror: (error) => logger.error('MCP Node HTTP adapter failed', { error: error.message }),
  });
  app.all('/mcp', (request, response) => {
    void nodeHandler(request, response, request.body);
  });

  const listener = await new Promise<HttpServer>((resolve, reject) => {
    const server = app.listen(port, host, () => resolve(server));
    server.once('error', reject);
  });
  logger.info('Simple Memory MCP listening with stateless Streamable HTTP', {
    host,
    port,
    path: '/mcp',
    accessMode: config.access.mode,
    allowedOrigins: origins,
  });

  let closed = false;
  return {
    close: async () => {
      if (closed) return;
      closed = true;
      const listenerClosed = closeHttpServer(listener);
      await handler.close();
      await listenerClosed;
    },
  };
}
