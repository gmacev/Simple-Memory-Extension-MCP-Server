#!/usr/bin/env node
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { AuthorizationService } from './access/authorization.js';
import { createMemoryService } from './application/create-service.js';
import { loadConfig } from './config.js';
import { Logger } from './logger.js';
import { buildMcpServer } from './mcp/server.js';
import { startHttpServer } from './transports/http.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = new Logger(config.logLevel);
  const authorization = new AuthorizationService(config.access);
  const service = createMemoryService(config);
  let closing = false;
  const close = async (): Promise<void> => {
    if (closing) return;
    closing = true;
    await service.close();
  };
  process.once('SIGINT', () => void close().finally(() => process.exit(0)));
  process.once('SIGTERM', () => void close().finally(() => process.exit(0)));
  process.once('beforeExit', () => void close());

  if (process.env.SIMPLE_MEMORY_TRANSPORT === 'http') {
    if (config.access.mode === 'fixed') {
      throw new Error('SIMPLE_MEMORY_ACCESS_MODE=fixed is only supported with stdio transport');
    }
    await startHttpServer(config, service, authorization, logger);
    return;
  }
  if (config.access.mode === 'oauth') {
    throw new Error('SIMPLE_MEMORY_ACCESS_MODE=oauth requires Streamable HTTP transport');
  }
  const server = buildMcpServer(service, authorization);
  await server.connect(new StdioServerTransport());
  logger.info('Simple Memory MCP listening on stdio');
}

main().catch((error: unknown) => {
  process.stderr.write(`Simple Memory failed: ${String(error)}\n`);
  process.exitCode = 1;
});
