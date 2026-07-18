#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-http-'));
const token = 'simple-memory-live-probe-token';

function assert(condition, message) {
  if (!condition) throw new Error(`HTTP probe assertion failed: ${message}`);
}

function toolResult(response) {
  const text = response.content.find((item) => item.type === 'text');
  if (!text) throw new Error('HTTP tool did not return JSON text content');
  const parsed = JSON.parse(text.text);
  assert(text.text === JSON.stringify(parsed), 'HTTP tool result must be minified JSON');
  assert(response.structuredContent === undefined, 'HTTP tool result must not be duplicated');
  return parsed;
}

async function availablePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  const port = typeof address === 'object' && address ? address.port : undefined;
  await new Promise((resolve) => server.close(resolve));
  if (!port) throw new Error('Could not reserve an HTTP probe port');
  return port;
}

function launch(overrides) {
  return spawn(process.execPath, [path.join(root, 'dist', 'index.js')], {
    cwd: root,
    env: {
      ...process.env,
      SIMPLE_MEMORY_DATA_DIR: dataDir,
      SIMPLE_MEMORY_LOG_LEVEL: 'info',
      SIMPLE_MEMORY_MODELS: 'disabled',
      SIMPLE_MEMORY_TRANSPORT: 'http',
      ...overrides,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
}

async function waitForListening(child) {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('HTTP server startup timed out')), 15_000);
    let stderr = '';
    const finish = (callback) => {
      clearTimeout(timer);
      callback();
    };
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
      if (stderr.includes('listening with Streamable HTTP')) finish(resolve);
    });
    child.once('exit', (code) =>
      finish(() => reject(new Error(`HTTP server exited with ${String(code)}: ${stderr}`))),
    );
    child.once('error', (error) => finish(() => reject(error)));
  });
}

async function stop(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill();
  await new Promise((resolve) => {
    const timer = setTimeout(resolve, 5_000);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function probeUnprotectedServer(port) {
  const allowedOrigin = `http://127.0.0.1:${port}`;
  const child = launch({
    SIMPLE_MEMORY_HTTP_HOST: '0.0.0.0',
    SIMPLE_MEMORY_HTTP_PORT: String(port),
    SIMPLE_MEMORY_HTTP_TOKEN: '',
    SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS: allowedOrigin,
  });
  let client;
  try {
    await waitForListening(child);
    const endpoint = new URL(`http://127.0.0.1:${port}/mcp`);
    const forbidden = await fetch(endpoint, {
      method: 'POST',
      headers: { Origin: 'https://attacker.example' },
    });
    assert(forbidden.status === 403, 'an unapproved Origin header must be rejected');
    client = new Client({ name: 'simple-memory-http-unprotected-probe', version: '2.0.0' });
    await client.connect(
      new StreamableHTTPClientTransport(endpoint, {
        requestInit: { headers: { Origin: allowedOrigin } },
      }),
    );
    const response = await client.callTool({
      name: 'memory_status',
      arguments: { probeModels: false },
    });
    assert(!response.isError, 'HTTP tool invocation without a configured token');
    toolResult(response);
  } finally {
    if (client) await client.close();
    await stop(child);
  }
}

async function run() {
  const unprotectedPort = await availablePort();
  await probeUnprotectedServer(unprotectedPort);

  const port = await availablePort();
  const child = launch({
    SIMPLE_MEMORY_HTTP_HOST: '127.0.0.1',
    SIMPLE_MEMORY_HTTP_PORT: String(port),
    SIMPLE_MEMORY_HTTP_TOKEN: token,
  });
  let client;
  try {
    await waitForListening(child);
    const endpoint = new URL(`http://127.0.0.1:${port}/mcp`);
    const unauthorized = await fetch(endpoint, { method: 'POST' });
    assert(unauthorized.status === 401, 'missing bearer token must be rejected');

    client = new Client({ name: 'simple-memory-http-probe', version: '2.0.0' });
    const transport = new StreamableHTTPClientTransport(endpoint, {
      requestInit: { headers: { Authorization: `Bearer ${token}` } },
    });
    await client.connect(transport);
    const tools = await client.listTools();
    assert(
      tools.tools.some((tool) => tool.name === 'memory_search'),
      'HTTP tool discovery',
    );
    const response = await client.callTool({
      name: 'memory_status',
      arguments: { probeModels: false },
    });
    assert(!response.isError, 'HTTP tool invocation');
    const status = toolResult(response);
    assert(status.memories?.active === 0, 'empty status counters must be numeric zero');
    return {
      ok: true,
      tokenOptional: true,
      tokenProtection: true,
      originValidation: true,
      toolCount: tools.tools.length,
      unauthorizedStatus: unauthorized.status,
    };
  } finally {
    if (client) await client.close();
    await stop(child);
  }
}

let outcome;
try {
  outcome = await run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
