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
  const environment = { ...process.env };
  for (const name of [
    'SIMPLE_MEMORY_ACCESS_MODE',
    'SIMPLE_MEMORY_FIXED_PRINCIPAL',
    'SIMPLE_MEMORY_FIXED_ACCESS',
    'SIMPLE_MEMORY_HTTP_PUBLIC_URL',
    'SIMPLE_MEMORY_OAUTH_ISSUER',
    'SIMPLE_MEMORY_OAUTH_AUDIENCE',
    'SIMPLE_MEMORY_HTTP_TOKEN',
  ]) {
    delete environment[name];
  }
  return spawn(process.execPath, [path.join(root, 'dist', 'index.js')], {
    cwd: root,
    env: {
      ...environment,
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

async function expectStartupFailure(child, expectedMessage) {
  const output = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('HTTP server did not reject unsafe startup')), 15_000);
    let stderr = '';
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
    });
    child.once('exit', () => {
      clearTimeout(timer);
      resolve(stderr);
    });
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
  assert(output.includes(expectedMessage), `startup failure must explain: ${expectedMessage}`);
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

async function probeOpenLoopbackServer(port) {
  const allowedOrigin = `http://127.0.0.1:${port}`;
  const child = launch({
    SIMPLE_MEMORY_ACCESS_MODE: 'open',
    SIMPLE_MEMORY_HTTP_HOST: '127.0.0.1',
    SIMPLE_MEMORY_HTTP_PORT: String(port),
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
    client = new Client({ name: 'simple-memory-http-open-probe', version: '2.3.0' });
    await client.connect(
      new StreamableHTTPClientTransport(endpoint, {
        requestInit: { headers: { Origin: allowedOrigin } },
      }),
    );
    const response = await client.callTool({
      name: 'memory_status',
      arguments: { probeModels: false },
    });
    assert(!response.isError, 'loopback open-mode HTTP tool invocation');
    toolResult(response);
  } finally {
    if (client) await client.close();
    await stop(child);
  }
}

async function run() {
  const openPort = await availablePort();
  await probeOpenLoopbackServer(openPort);

  const unsafePort = await availablePort();
  await expectStartupFailure(
    launch({
      SIMPLE_MEMORY_ACCESS_MODE: 'open',
      SIMPLE_MEMORY_HTTP_HOST: '0.0.0.0',
      SIMPLE_MEMORY_HTTP_PORT: String(unsafePort),
      SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS: `http://127.0.0.1:${unsafePort}`,
    }),
    'Open HTTP access may only bind to loopback',
  );

  const legacyPort = await availablePort();
  await expectStartupFailure(
    launch({
      SIMPLE_MEMORY_HTTP_HOST: '127.0.0.1',
      SIMPLE_MEMORY_HTTP_PORT: String(legacyPort),
      SIMPLE_MEMORY_HTTP_TOKEN: 'obsolete-token',
    }),
    'SIMPLE_MEMORY_HTTP_TOKEN is no longer supported',
  );

  return {
    ok: true,
    openLoopback: true,
    originValidation: true,
    unauthenticatedNonLoopbackRefused: true,
    legacySharedTokenRefused: true,
  };
}

let outcome;
try {
  outcome = await run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
