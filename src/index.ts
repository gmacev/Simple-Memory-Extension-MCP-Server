#!/usr/bin/env node
import { serveStdio } from '@modelcontextprotocol/server/stdio';
import { AuthorizationService } from './access/authorization.js';
import { createMemoryService } from './application/create-service.js';
import { loadConfig } from './config.js';
import { Logger } from './logger.js';
import { buildMcpServer } from './mcp/server.js';
import { acquireDatabaseServerLease } from './operations/database-runtime.js';
import { startHttpServer } from './transports/http.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = new Logger(config.logLevel);
  const authorization = new AuthorizationService(config.access);
  const startupIndexCutoff = new Date().toISOString();
  const serverLease = await acquireDatabaseServerLease(config.databasePath);
  let service: ReturnType<typeof createMemoryService>;
  try {
    service = createMemoryService(config);
  } catch (error) {
    await serverLease.release();
    throw error;
  }
  let closeTransport = async (): Promise<void> => {};
  let pendingIndexDrain = Promise.resolve();
  const startPendingIndexDrain = (): void => {
    pendingIndexDrain = service
      .reindexPending(startupIndexCutoff)
      .then(({ indexed, failed }) => {
        if (indexed > 0 || failed > 0) {
          logger.info('Finished recovered indexing work', { indexed, failed });
        }
      })
      .catch((error: unknown) => {
        logger.warn('Recovered indexing work remains pending', { error: String(error) });
      });
  };
  let closing = false;
  const close = async (): Promise<void> => {
    if (closing) return;
    closing = true;
    try {
      await closeTransport();
      await pendingIndexDrain;
    } finally {
      try {
        await service.close();
      } finally {
        await serverLease.release();
      }
    }
  };
  process.once('SIGINT', () => void close().finally(() => process.exit(0)));
  process.once('SIGTERM', () => void close().finally(() => process.exit(0)));
  process.once('beforeExit', () => void close());

  if (config.transport === 'http') {
    if (config.access.mode === 'fixed') {
      throw new Error('SIMPLE_MEMORY_ACCESS_MODE=fixed is only supported with stdio transport');
    }
    const http = await startHttpServer(config, service, authorization, logger);
    closeTransport = () => http.close();
    startPendingIndexDrain();
    return;
  }
  if (config.access.mode === 'oauth') {
    throw new Error('SIMPLE_MEMORY_ACCESS_MODE=oauth requires Streamable HTTP transport');
  }
  const accessContext = authorization.context();
  const stdio = serveStdio(() => buildMcpServer(service, authorization, accessContext), {
    legacy: 'serve',
    onerror: (error) => logger.error('MCP stdio request failed', { error: error.message }),
  });
  closeTransport = () => stdio.close();
  process.stdin.once('end', () => void close());
  logger.info('Simple Memory MCP listening on stdio');
  startPendingIndexDrain();
}

main().catch((error: unknown) => {
  process.stderr.write(`Simple Memory failed: ${String(error)}\n`);
  process.exitCode = 1;
});
