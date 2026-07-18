import { createMcpExpressApp } from '@modelcontextprotocol/sdk/server/express.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import type {
  Transport,
  TransportSendOptions,
} from '@modelcontextprotocol/sdk/shared/transport.js';
import type { JSONRPCMessage, MessageExtraInfo } from '@modelcontextprotocol/sdk/types.js';
import type { NextFunction, Request, Response } from 'express';
import type { MemoryService } from '../application/memory-service.js';
import type { Logger } from '../logger.js';
import { buildMcpServer } from '../mcp/server.js';

function isLoopback(host: string): boolean {
  return host === '127.0.0.1' || host === 'localhost' || host === '::1';
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

export async function startHttpServer(service: MemoryService, logger: Logger): Promise<void> {
  const host = process.env.SIMPLE_MEMORY_HTTP_HOST ?? '127.0.0.1';
  const port = Number.parseInt(process.env.SIMPLE_MEMORY_HTTP_PORT ?? '3000', 10);
  const token = process.env.SIMPLE_MEMORY_HTTP_TOKEN?.trim() || undefined;
  const origins = allowedOrigins(host, port);
  if (!isLoopback(host) && !token) {
    logger.warn('Streamable HTTP is exposed without authentication', {
      host,
      recommendation: 'Set SIMPLE_MEMORY_HTTP_TOKEN unless access is protected elsewhere',
    });
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
  app.use((request: Request, response: Response, next: NextFunction) => {
    if (!token || request.headers.authorization === `Bearer ${token}`) {
      next();
      return;
    }
    response.status(401).json({ error: 'Unauthorized' });
  });
  app.post('/mcp', async (request: Request, response: Response) => {
    const server = buildMcpServer(service);
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
        allowedOrigins: origins,
      });
      resolve();
    });
    listener.once('error', reject);
  });
}
