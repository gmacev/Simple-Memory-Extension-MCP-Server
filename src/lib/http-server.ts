import http from 'http';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { logger } from './logger.js';
import { URL } from 'url';

// Store active SSE transports by session ID
const activeTransports = new Map<string, SSEServerTransport>();

// Store heartbeat intervals by session ID
const heartbeatIntervals = new Map<string, NodeJS.Timeout>();

/**
 * Get transport by session ID - useful for reconnection
 */
export function getTransport(sessionId: string): SSEServerTransport | undefined {
  return activeTransports.get(sessionId);
}

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

      // Prevent timeout for long-running connections
      req.socket.setTimeout(0);
      req.socket.setKeepAlive(true);
      req.socket.setMaxListeners(20);

      // Check if there's a session ID for reconnection
      const urlParams = new URLSearchParams(reqUrl.search);
      const existingSessionId = urlParams.get('sessionId');

      // Create SSE transport with a fixed endpoint path
      const sseTransport = new SSEServerTransport('message', res);

      // If reconnecting with an existing session ID, store it
      if (existingSessionId) {
        logger.debug(`Attempting to reconnect with existing session ID: ${existingSessionId}`);
      }

      // Store the transport in our map
      const sessionId = sseTransport.sessionId;
      activeTransports.set(sessionId, sseTransport);

      logger.debug(`Created new SSE transport with session ID: ${sessionId}`);

      // Set up heartbeat to keep connection alive
      // This is critical for preventing connection timeouts and ensuring we don't lose message correlation
      // The heartbeat sends an empty comment every 30 seconds to keep the connection open
      const heartbeatInterval = setInterval(() => {
        if (!res.writableEnded) {
          try {
            // Send a comment as heartbeat
            res.write(`:heartbeat\n\n`);
            logger.debug(`Sent heartbeat to session ${sessionId}`);
          } catch (err) {
            logger.error(`Failed to send heartbeat to session ${sessionId}`, err);
            clearInterval(heartbeatInterval);
            activeTransports.delete(sessionId);
          }
        } else {
          // Connection already ended
          clearInterval(heartbeatInterval);
          activeTransports.delete(sessionId);
        }
      }, 30000);

      // Store the heartbeat interval
      heartbeatIntervals.set(sessionId, heartbeatInterval);

      // Connect server to transport
      server.connect(sseTransport).catch((error) => {
        logger.error('Failed to connect to SSE transport', error);
        clearInterval(heartbeatIntervals.get(sessionId)!);
        heartbeatIntervals.delete(sessionId);
        activeTransports.delete(sessionId);
      });

      // Handle connection close
      req.on('close', () => {
        logger.info(`SSE connection closed for session: ${sessionId}`);

        // Clean up resources
        const interval = heartbeatIntervals.get(sessionId);
        if (interval) {
          clearInterval(interval);
          heartbeatIntervals.delete(sessionId);
        }

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
        res.end(JSON.stringify({
          error: 'Session not found or expired',
          reconnect: true
        }));
        return;
      }

      // Pass the request to the SSE transport's handlePostMessage method
      try {
        transport.handlePostMessage(req, res);
      } catch (error) {
        logger.error(`Error handling message for session ${sessionId}`, error);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            error: 'Internal server error',
            details: error instanceof Error ? error.message : String(error)
          }));
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

  // Clear all heartbeat intervals
  for (const [sessionId, interval] of heartbeatIntervals.entries()) {
    clearInterval(interval);
    heartbeatIntervals.delete(sessionId);
  }

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
