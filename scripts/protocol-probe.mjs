#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { StdioClientTransport } from '@modelcontextprotocol/client/stdio';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const { mcpToolAccessLevels } = await import(
  pathToFileURL(path.join(root, 'dist', 'mcp', 'server.js'))
);
const dataRoot = mkdtempSync(path.join(tmpdir(), 'simple-memory-protocol-'));
const protocolVersion = '2026-07-28';

function assert(condition, message) {
  if (!condition) throw new Error(`Protocol probe assertion failed: ${message}`);
}

function toolResult(response) {
  const text = response.content.find((item) => item.type === 'text');
  assert(text, 'tool response must contain JSON text');
  const parsed = JSON.parse(text.text);
  assert(text.text === JSON.stringify(parsed), 'tool JSON must remain minified');
  assert(response.structuredContent !== undefined, 'tool result must include structuredContent');
  assert(
    JSON.stringify(response.structuredContent) === text.text,
    'structuredContent must match JSON text',
  );
  return parsed;
}

function environment(dataDir, overrides = {}) {
  return {
    ...process.env,
    SIMPLE_MEMORY_DATA_DIR: dataDir,
    SIMPLE_MEMORY_LOG_LEVEL: 'error',
    SIMPLE_MEMORY_MODELS: 'disabled',
    ...overrides,
  };
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
  if (!port) throw new Error('Could not reserve a protocol probe port');
  return port;
}

function launchHttp(port, dataDir, origin) {
  return spawn(process.execPath, [path.join(root, 'dist', 'index.js')], {
    cwd: root,
    env: environment(dataDir, {
      SIMPLE_MEMORY_TRANSPORT: 'http',
      SIMPLE_MEMORY_ACCESS_MODE: 'open',
      SIMPLE_MEMORY_HTTP_HOST: '127.0.0.1',
      SIMPLE_MEMORY_HTTP_PORT: String(port),
      SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS: origin,
      SIMPLE_MEMORY_LOG_LEVEL: 'info',
    }),
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  });
}

async function waitForListening(child) {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('Protocol HTTP server startup timed out')),
      15_000,
    );
    let stderr = '';
    const finish = (callback) => {
      clearTimeout(timer);
      callback();
    };
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString('utf8');
      if (stderr.includes('listening with stateless Streamable HTTP')) finish(resolve);
    });
    child.once('exit', (code) =>
      finish(() =>
        reject(new Error(`Protocol HTTP server exited with ${String(code)}: ${stderr}`)),
      ),
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

function client(name, mode) {
  return new Client(
    { name, version: '3.5.0' },
    { versionNegotiation: { mode, probe: { timeoutMs: 5_000 } } },
  );
}

async function probeStdio(mode, expectedEra, dataDir) {
  const instance = client(`simple-memory-stdio-${expectedEra}-probe`, mode);
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(root, 'dist', 'index.js')],
    cwd: root,
    env: environment(dataDir),
    stderr: 'pipe',
  });
  try {
    await instance.connect(transport);
    assert(instance.getProtocolEra() === expectedEra, `stdio must negotiate ${expectedEra}`);
    if (expectedEra === 'modern') {
      assert(
        instance.getNegotiatedProtocolVersion() === protocolVersion,
        'modern stdio protocol version',
      );
      assert(instance.getDiscoverResult(), 'modern stdio must retain server/discover');
    }
    const response = await instance.callTool({
      name: 'memory_status',
      arguments: { probeModels: false },
    });
    assert(!response.isError, `${expectedEra} stdio memory_status`);
    toolResult(response);
  } finally {
    await instance.close();
  }
}

async function probeConcurrency(dataDir) {
  const instance = client('simple-memory-concurrency-probe', 'auto');
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [path.join(root, 'dist', 'index.js')],
    cwd: root,
    env: environment(dataDir),
    stderr: 'pipe',
  });
  try {
    await instance.connect(transport);
    const space = toolResult(
      await instance.callTool({
        name: 'space_create',
        arguments: { name: 'Protocol concurrency probe' },
      }),
    );
    const createMemory = async (logicalKey, marker) =>
      toolResult(
        await instance.callTool({
          name: 'memory_create',
          arguments: {
            spaceId: space.id,
            logicalKey,
            title: marker,
            kind: 'protocol-probe',
            content: { marker, revision: 1 },
          },
        }),
      );
    const contested = await createMemory('contested', 'contested');
    const revisionArguments = (marker) => ({
      memoryId: contested.id,
      expectedRevisionId: contested.revisionId,
      spaceId: space.id,
      title: marker,
      kind: 'protocol-probe',
      content: { marker, revision: 2 },
    });
    const contestedResults = await Promise.all([
      instance.callTool({
        name: 'memory_revise',
        arguments: revisionArguments('contested-a'),
      }),
      instance.callTool({
        name: 'memory_revise',
        arguments: revisionArguments('contested-b'),
      }),
    ]);
    assert(
      contestedResults.filter((response) => !response.isError).length === 1,
      'one concurrent revision must succeed',
    );
    assert(
      contestedResults.filter((response) => response.isError).length === 1,
      'one concurrent revision must conflict',
    );

    const [independentA, independentB] = await Promise.all([
      createMemory('independent-a', 'independent-a'),
      createMemory('independent-b', 'independent-b'),
    ]);
    const independentResults = await Promise.all([
      instance.callTool({
        name: 'memory_revise',
        arguments: {
          memoryId: independentA.id,
          expectedRevisionId: independentA.revisionId,
          spaceId: space.id,
          title: 'independent-a-updated',
          kind: 'protocol-probe',
          content: { marker: 'independent-a', revision: 2 },
        },
      }),
      instance.callTool({
        name: 'memory_revise',
        arguments: {
          memoryId: independentB.id,
          expectedRevisionId: independentB.revisionId,
          spaceId: space.id,
          title: 'independent-b-updated',
          kind: 'protocol-probe',
          content: { marker: 'independent-b', revision: 2 },
        },
      }),
    ]);
    assert(
      independentResults.every((response) => !response.isError),
      'separate memories must revise independently',
    );
  } finally {
    await instance.close();
  }
}

async function connectHttp(endpoint, origin, mode, name) {
  const instance = client(name, mode);
  const transport = new StreamableHTTPClientTransport(endpoint, {
    requestInit: { headers: { Origin: origin } },
  });
  await instance.connect(transport);
  return { instance, transport };
}

async function statusThroughHttp(endpoint, origin, mode, expectedEra, name) {
  const connection = await connectHttp(endpoint, origin, mode, name);
  try {
    assert(
      connection.instance.getProtocolEra() === expectedEra,
      `HTTP must negotiate ${expectedEra}`,
    );
    assert(
      connection.transport.sessionId === undefined,
      'stateless HTTP must not create a session',
    );
    const response = await connection.instance.callTool({
      name: 'memory_status',
      arguments: { probeModels: false },
    });
    assert(!response.isError, `${expectedEra} HTTP memory_status`);
    toolResult(response);
    return connection.instance;
  } finally {
    await connection.instance.close();
  }
}

function modernBody(id, method, params = {}, version = protocolVersion) {
  return {
    jsonrpc: '2.0',
    id,
    method,
    params: {
      ...params,
      _meta: {
        'io.modelcontextprotocol/protocolVersion': version,
        'io.modelcontextprotocol/clientInfo': { name: 'raw-protocol-probe', version: '3.5.0' },
        'io.modelcontextprotocol/clientCapabilities': {},
      },
    },
  };
}

async function rawModern(endpoint, origin, body, headers = {}) {
  return fetch(endpoint, {
    method: 'POST',
    headers: {
      Accept: 'application/json, text/event-stream',
      'Content-Type': 'application/json',
      Origin: origin,
      'MCP-Protocol-Version': body.params._meta['io.modelcontextprotocol/protocolVersion'],
      'Mcp-Method': body.method,
      ...headers,
    },
    body: JSON.stringify(body),
  });
}

async function probeHttp() {
  const dataDir = path.join(dataRoot, 'http');
  const port = await availablePort();
  const origin = `http://127.0.0.1:${port}`;
  const endpoint = new URL(`${origin}/mcp`);
  const child = launchHttp(port, dataDir, origin);
  try {
    await waitForListening(child);

    const first = await connectHttp(endpoint, origin, 'auto', 'simple-memory-http-modern-one');
    try {
      assert(first.instance.getProtocolEra() === 'modern', 'first HTTP client must be modern');
      assert(first.transport.sessionId === undefined, 'first HTTP client must be stateless');
      const firstList = await first.instance.listTools(undefined, { cacheMode: 'refresh' });
      const secondList = await first.instance.listTools(undefined, { cacheMode: 'refresh' });
      assert(
        JSON.stringify(firstList.tools.map((tool) => tool.name)) ===
          JSON.stringify(secondList.tools.map((tool) => tool.name)),
        'tool order must be deterministic',
      );
      assert(
        JSON.stringify(firstList.tools.map((tool) => tool.name).sort()) ===
          JSON.stringify(Object.keys(mcpToolAccessLevels).sort()),
        'every advertised tool must have one OAuth access level',
      );
      assert(firstList.ttlMs === 300_000, 'tools/list must advertise a five-minute TTL');
      assert(firstList.cacheScope === 'public', 'tools/list must be publicly cacheable');
      const templates = await first.instance.listResourceTemplates(undefined, {
        cacheMode: 'refresh',
      });
      assert(templates.ttlMs === 300_000, 'resource templates must advertise a five-minute TTL');
      assert(templates.cacheScope === 'public', 'resource templates must be publicly cacheable');
      toolResult(
        await first.instance.callTool({
          name: 'memory_status',
          arguments: { probeModels: false },
        }),
      );
    } finally {
      await first.instance.close();
    }

    await statusThroughHttp(endpoint, origin, 'auto', 'modern', 'simple-memory-http-modern-two');
    await statusThroughHttp(endpoint, origin, 'legacy', 'legacy', 'simple-memory-http-legacy');

    const discoverResponse = await rawModern(endpoint, origin, modernBody(101, 'server/discover'));
    assert(discoverResponse.status === 200, 'raw server/discover must succeed');
    assert(
      discoverResponse.headers.get('mcp-session-id') === null,
      'server/discover must not mint a session',
    );
    const discover = await discoverResponse.json();
    assert(discover.result?.resultType === 'complete', 'discover resultType');
    assert(discover.result?.ttlMs === 300_000, 'discover TTL');
    assert(discover.result?.cacheScope === 'public', 'discover cache scope');

    const missingMethod = await rawModern(endpoint, origin, modernBody(102, 'tools/list'), {
      'Mcp-Method': '',
    });
    assert(missingMethod.status === 400, 'missing Mcp-Method must be rejected');

    const unsupported = modernBody(103, 'tools/list', {}, '2099-01-01');
    const unsupportedResponse = await rawModern(endpoint, origin, unsupported);
    assert(unsupportedResponse.status === 400, 'unsupported protocol must be rejected');

    const getResponse = await fetch(endpoint, { method: 'GET', headers: { Origin: origin } });
    const deleteResponse = await fetch(endpoint, { method: 'DELETE', headers: { Origin: origin } });
    assert(getResponse.status === 405, 'modern endpoint GET must return 405');
    assert(deleteResponse.status === 405, 'modern endpoint DELETE must return 405');
  } finally {
    await stop(child);
  }
}

async function run() {
  await probeStdio('auto', 'modern', path.join(dataRoot, 'stdio-modern'));
  await probeStdio('legacy', 'legacy', path.join(dataRoot, 'stdio-legacy'));
  await probeConcurrency(path.join(dataRoot, 'concurrency'));
  await probeHttp();
  return {
    ok: true,
    modernStdio: true,
    legacyStdio: true,
    modernHttp: true,
    legacyHttp: true,
    statelessHttp: true,
    protocolHeaders: true,
    cacheHints: true,
    concurrentRevisionProtection: true,
  };
}

let outcome;
try {
  outcome = await run();
} finally {
  rmSync(dataRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome, null, 2)}\n`);
