import http from 'http';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { logger } from './logger.js';
import { URL } from 'url';

// Store active SSE transports by session ID
const activeTransports = new Map<string, SSEServerTransport>();

/**
 * Set up an HTTP server with SSE transport for the MCP server
 */
export function setupHttpServer(server: McpServer, port: number): http.Server {
  // Create HTTP server
  const httpServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    // Handle preflight requests
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    // Parse URL
    const reqUrl = new URL(req.url || '/', `http://localhost:${port}`);
    const path = reqUrl.pathname;

    // Health check endpoint
    if (path === '/health' && req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    // SSE endpoint
    if (path === '/sse' && req.method === 'GET') {
      logger.info('New SSE connection established');

      // DO NOT set SSE headers here - let the SSEServerTransport handle it

      // Create SSE transport with a fixed endpoint path
      // The path here is the endpoint where clients will POST messages to
      const sseTransport = new SSEServerTransport('message', res);

      // Store the transport in our map
      const sessionId = sseTransport.sessionId;
      activeTransports.set(sessionId, sseTransport);

      logger.debug(`Created new SSE transport with session ID: ${sessionId}`);

      // Connect server to transport
      server.connect(sseTransport).catch((error) => {
        logger.error('Failed to connect to SSE transport', error);
      });

      // Handle connection close
      req.on('close', () => {
        logger.info(`SSE connection closed for session: ${sessionId}`);
        activeTransports.delete(sessionId);
        sseTransport.close().catch((error) => {
          logger.error('Error closing SSE transport', error);
        });
      });

      return;
    }

    // Message endpoint
    if (path === '/message' && req.method === 'POST') {
      const urlParams = new URLSearchParams(reqUrl.search);
      const sessionId = urlParams.get('sessionId');

      if (!sessionId) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Missing sessionId parameter' }));
        return;
      }

      const transport = activeTransports.get(sessionId);
      if (!transport) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Session not found or expired' }));
        return;
      }

      // Pass the request to the SSE transport's handlePostMessage method
      try {
        transport.handlePostMessage(req, res);
      } catch (error) {
        logger.error(`Error handling message for session ${sessionId}`, error);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Internal server error' }));
        }
      }

      return;
    }

    // Not found
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  });

  // Start HTTP server
  httpServer.listen(port, () => {
    logger.info(`HTTP server listening on port ${port}`);
    logger.info('Server ready to receive requests via HTTP');
    logger.info(`SSE endpoint: http://localhost:${port}/sse`);
    logger.info(`Message endpoint: http://localhost:${port}/message?sessionId=<SESSION_ID>`);
  });

  return httpServer;
}

/**
 * Close all active SSE transports
 */
export async function closeAllTransports(): Promise<void> {
  logger.info(`Closing ${activeTransports.size} active SSE transports`);

  // Use Promise.allSettled to ensure we attempt to close all transports
  // even if some fail
  const closePromises = Array.from(activeTransports.entries()).map(
    async ([sessionId, transport]) => {
      try {
        logger.debug(`Closing SSE transport for session: ${sessionId}`);
        await Promise.race([
          transport.close(),
          // Add a timeout to prevent hanging
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Transport close timeout')), 1000)
          ),
        ]);
        return { sessionId, success: true };
      } catch (error) {
        logger.error(`Error closing SSE transport for session ${sessionId}`, error);
        return { sessionId, success: false };
      }
    }
  );

  await Promise.allSettled(closePromises);

  // Clear the map regardless of success/failure
  activeTransports.clear();
  logger.info('All SSE transports closed or timed out');
}
