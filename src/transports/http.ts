import { createMcpExpressApp } from '@modelcontextprotocol/sdk/server/express.js';
import { InsufficientScopeError } from '@modelcontextprotocol/sdk/server/auth/errors.js';
import { requireBearerAuth } from '@modelcontextprotocol/sdk/server/auth/middleware/bearerAuth.js';
import {
  getOAuthProtectedResourceMetadataUrl,
  mcpAuthMetadataRouter,
} from '@modelcontextprotocol/sdk/server/auth/router.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type {
  Transport,
  TransportSendOptions,
} from '@modelcontextprotocol/sdk/shared/transport.js';
import type { JSONRPCMessage, MessageExtraInfo } from '@modelcontextprotocol/sdk/types.js';
import type { NextFunction, Request, Response } from 'express';
import { memoryScopes, type AuthorizationService } from '../access/authorization.js';
import { createOAuthRuntime } from '../access/oauth.js';
import type { MemoryService } from '../application/memory-service.js';
import type { AppConfig } from '../config.js';
import type { Logger } from '../logger.js';
import { buildMcpServer } from '../mcp/server.js';

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

class ExactOptionalTransport implements Transport {
  public onclose?: () => void;
  public onerror?: (error: Error) => void;
  public onmessage?: <T extends JSONRPCMessage>(message: T, extra?: MessageExtraInfo) => void;

  public constructor(public readonly inner: StreamableHTTPServerTransport) {}

  public async start(): Promise<void> {
    this.inner.onclose = () => this.onclose?.();
    this.inner.onerror = (error) => this.onerror?.(error);
    this.inner.onmessage = (message, extra) => this.onmessage?.(message, extra);
    await this.inner.start();
  }

  public send(message: JSONRPCMessage, options?: TransportSendOptions): Promise<void> {
    return this.inner.send(message, options);
  }

  public close(): Promise<void> {
    return this.inner.close();
  }
}

export async function startHttpServer(
  config: AppConfig,
  service: MemoryService,
  authorization: AuthorizationService,
  logger: Logger,
): Promise<void> {
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
  const app = createMcpExpressApp({ host });
  app.use((request: Request, response: Response, next: NextFunction) => {
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
    requireSecureRemoteUrl(config.access.oauthIssuer, 'SIMPLE_MEMORY_OAUTH_ISSUER');
    const oauth = await createOAuthRuntime(config.access);
    const resourceMetadataUrl = getOAuthProtectedResourceMetadataUrl(publicUrl);
    app.use(
      mcpAuthMetadataRouter({
        oauthMetadata: oauth.metadata,
        resourceServerUrl: publicUrl,
        scopesSupported: [...memoryScopes],
        resourceName: 'Simple Memory',
      }),
    );
    app.use(
      '/mcp',
      requireBearerAuth({
        verifier: oauth.verifier,
        resourceMetadataUrl,
      }),
      (request: Request, response: Response, next: NextFunction) => {
        const scopes = request.auth?.scopes ?? [];
        if (memoryScopes.some((scope) => scopes.includes(scope))) {
          next();
          return;
        }
        const error = new InsufficientScopeError(
          'At least one Simple Memory OAuth scope is required',
        );
        response.set(
          'WWW-Authenticate',
          `Bearer error="insufficient_scope", error_description="${error.message}", scope="${memoryScopes.join(' ')}", resource_metadata="${resourceMetadataUrl}"`,
        );
        response.status(403).json({
          error: 'insufficient_scope',
          error_description: error.message,
        });
      },
    );
  }
  app.post('/mcp', async (request: Request, response: Response) => {
    const server = buildMcpServer(service, authorization);
    const nativeTransport = new StreamableHTTPServerTransport({
      allowedOrigins: origins,
    });
    const transport = new ExactOptionalTransport(nativeTransport);
    response.on('close', () => {
      void transport.close();
      void server.close();
    });
    try {
      await server.connect(transport);
      await nativeTransport.handleRequest(request, response, request.body);
    } catch (error) {
      logger.error('Streamable HTTP request failed', { error: String(error) });
      if (!response.headersSent) {
        response.status(500).json({
          jsonrpc: '2.0',
          error: { code: -32603, message: 'Internal server error' },
          id: null,
        });
      }
    }
  });
  app.get('/mcp', (_request: Request, response: Response) => {
    response.status(405).json({ error: 'Stateless MCP endpoint accepts POST requests' });
  });
  app.delete('/mcp', (_request: Request, response: Response) => {
    response.status(405).json({ error: 'Stateless MCP endpoint has no sessions to delete' });
  });
  await new Promise<void>((resolve, reject) => {
    const listener = app.listen(port, host, () => {
      logger.info('Simple Memory MCP listening with Streamable HTTP', {
        host,
        port,
        path: '/mcp',
        accessMode: config.access.mode,
        allowedOrigins: origins,
      });
      resolve();
    });
    listener.once('error', reject);
  });
}
