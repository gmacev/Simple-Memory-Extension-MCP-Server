#!/usr/bin/env node

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import * as dotenv from 'dotenv';
import { SQLiteDataStore } from './lib/sqlite-store.js';
import { registerTools } from './lib/tool-implementations.js';
import { logger, LogLevel } from './lib/logger.js';
import { setupHttpServer, closeAllTransports } from './lib/http-server.js';
import http from 'http';
import path from 'path';

// Load environment variables from .env file
dotenv.config();

// Configure logging level from environment variable
const configuredLogLevel = process.env.LOG_LEVEL;
if (configuredLogLevel) {
  switch (configuredLogLevel.toLowerCase()) {
    case 'debug':
      logger.setLogLevel(LogLevel.DEBUG);
      break;
    case 'info':
      logger.setLogLevel(LogLevel.INFO);
      break;
    case 'warn':
      logger.setLogLevel(LogLevel.WARN);
      break;
    case 'error':
      logger.setLogLevel(LogLevel.ERROR);
      break;
    default:
      logger.warn(`Unknown log level: ${configuredLogLevel}, using default`);
  }
}

/**
 * Main entry point for the server
 */
async function main() {
  let db: SQLiteDataStore;
  let server: McpServer;

  try {
    logger.info('Initializing EnhancedPersistentKeyValueStoreMCPServer...');

    // Get database path from environment variable or use default path
    const dbPath = process.env.DB_PATH || path.join(process.cwd(), 'data', 'memory.db');
    logger.info(`Using database at: ${dbPath}`);

    // Initialize the SQLite database
    try {
      db = new SQLiteDataStore({
        dbPath: dbPath,
        enableChunking: true,
      });
      logger.info('Database initialized successfully');
    } catch (dbError) {
      logger.error('Failed to initialize database', dbError);
      process.exit(1);
    }

    // Create an MCP server
    server = new McpServer({
      name: 'EnhancedPersistentKeyValueStoreMCPServer',
      version: '1.0.0',
    });

    // Register all tools with the server using schemas from tool-definitions.ts
    registerTools(server, db);
    logger.info('All tools registered with the server');

    // Check if we should use HTTP server
    const useHttpSSE = process.env.USE_HTTP_SSE === 'true';
    const port = parseInt(process.env.PORT || '3000', 10);

    if (useHttpSSE) {
      // Set up HTTP server SSE transport
      const httpServer = setupHttpServer(server, port);

      // Setup graceful shutdown for HTTP server
      setupGracefulShutdown(db, httpServer);
    } else {
      // Use standard stdio transport
      const transport = new StdioServerTransport();
      await server.connect(transport);
      logger.info('Server ready to receive requests via stdio');

      // Setup graceful shutdown
      setupGracefulShutdown(db);
    }
  } catch (error) {
    logger.error('Error initializing server', error);
    process.exit(1);
  }
}

/**
 * Set up graceful shutdown to properly close database connections
 */
function setupGracefulShutdown(db: SQLiteDataStore, httpServer?: http.Server) {
  const shutdown = async () => {
    logger.info('Shutting down server...');

    // Set a timeout to force exit if graceful shutdown takes too long
    const forceExitTimeout = setTimeout(() => {
      logger.error('Forced shutdown after timeout');
      process.exit(1);
    }, 5000); // Force exit after 5 seconds

    try {
      // Close all active SSE transports
      await closeAllTransports();

      if (httpServer) {
        await new Promise<void>((resolve) => {
          httpServer.close(() => {
            logger.info('HTTP server closed');
            resolve();
          });
        });
      }

      if (db) {
        // Get underlying DB and close it
        const sqliteDb = await db.getDb();
        if (sqliteDb) {
          await new Promise<void>((resolve) => {
            sqliteDb.close(() => {
              logger.info('Database connection closed');
              resolve();
            });
          });
        }
      }

      logger.info('Server shutdown complete');
      clearTimeout(forceExitTimeout);
      process.exit(0);
    } catch (error) {
      logger.error('Error during shutdown', error);
      clearTimeout(forceExitTimeout);
      process.exit(1);
    }
  };

  // Register shutdown handlers
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Promise rejection', { reason, promise });
  });
}

// Start the server
main().catch((error) => {
  logger.error('Unhandled error in main function', error);
  process.exit(1);
});
