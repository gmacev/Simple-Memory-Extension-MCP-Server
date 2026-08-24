#!/usr/bin/env node
import { spawn, spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-load-'));
const modelsEnabled = !process.argv.includes('--models-disabled');
const clientsCount = 4;
const rounds = 2;

function assert(condition, message) {
  if (!condition) throw new Error(`Load probe failed: ${message}`);
}

function result(response) {
  if (response.isError) throw new Error(JSON.stringify(response.content));
  assert(response.structuredContent !== undefined, 'tool response must be structured');
  return response.structuredContent;
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
  if (!port) throw new Error('Could not reserve a load-probe port');
  return port;
}

function percentile(values, fraction) {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
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

async function run() {
  const port = await availablePort();
  const origin = `http://127.0.0.1:${port}`;
  if (modelsEnabled) {
    const prepared = spawnSync(
      process.execPath,
      [path.join(root, 'dist', 'cli.js'), 'embedding', 'upgrade'],
      {
        cwd: root,
        env: {
          ...process.env,
          SIMPLE_MEMORY_DATA_DIR: dataDir,
          SIMPLE_MEMORY_MODELS: 'enabled',
          SIMPLE_MEMORY_LOG_LEVEL: 'error',
        },
        encoding: 'utf8',
        timeout: 900_000,
      },
    );
    if (prepared.error) throw prepared.error;
    assert(
      prepared.status === 0,
      `could not prepare semantic index generation: ${prepared.stderr}`,
    );
  }
  const child = spawn(process.execPath, [path.join(root, 'dist', 'index.js')], {
    cwd: root,
    env: {
      ...process.env,
      SIMPLE_MEMORY_DATA_DIR: dataDir,
      SIMPLE_MEMORY_MODELS: modelsEnabled ? 'enabled' : 'disabled',
      SIMPLE_MEMORY_LOG_LEVEL: 'error',
      SIMPLE_MEMORY_TRANSPORT: 'http',
      SIMPLE_MEMORY_HTTP_HOST: '127.0.0.1',
      SIMPLE_MEMORY_HTTP_PORT: String(port),
      SIMPLE_MEMORY_HTTP_ALLOWED_ORIGINS: origin,
    },
    stdio: ['ignore', 'ignore', 'pipe'],
    windowsHide: true,
  });
  const clients = [];
  let stderr = '';
  child.stderr.on('data', (chunk) => {
    stderr += chunk.toString('utf8');
  });
  try {
    const readyDeadline = Date.now() + 15_000;
    while (Date.now() < readyDeadline) {
      if (child.exitCode !== null) throw new Error(`Server exited: ${stderr}`);
      try {
        const response = await fetch(`${origin}/readyz`);
        if (response.ok) break;
      } catch {
        // Server startup is still in progress.
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const ready = await fetch(`${origin}/readyz`).catch(() => null);
    assert(ready?.ok, `server did not become ready: ${stderr}`);

    for (let index = 0; index < clientsCount; index += 1) {
      const client = new Client(
        { name: `simple-memory-load-agent-${index}`, version: '3.9.2' },
        { versionNegotiation: { mode: 'auto' } },
      );
      await client.connect(
        new StreamableHTTPClientTransport(new URL(`${origin}/mcp`), {
          requestInit: { headers: { Origin: origin } },
        }),
      );
      clients.push(client);
    }

    const call = async (client, name, arguments_) =>
      result(
        await client.callTool({ name, arguments: arguments_ }, undefined, {
          timeout: 900_000,
          maxTotalTimeout: 900_000,
        }),
      );
    const space = await call(clients[0], 'space_create', {
      id: `load-probe-${Date.now()}`,
      name: 'Bounded production load probe',
    });
    const seeds = [];
    for (let index = 0; index < clientsCount; index += 1) {
      seeds.push(
        await call(clients[index], 'memory_create', {
          spaceId: space.id,
          logicalKey: `agent-${index}-state`,
          title: `Agent ${index} canonical workload state`,
          kind: 'load-probe-state',
          tags: ['load-probe', `agent-${index}`],
          content: {
            agent: index,
            state: 'seeded',
            marker: `LOAD-PROBE-AGENT-${index}`,
          },
        }),
      );
    }

    const measurements = [];
    let degradedSearches = 0;
    const startedAt = performance.now();
    await Promise.all(
      clients.map(async (client, clientIndex) => {
        let expectedRevisionId = seeds[clientIndex].revisionId;
        for (let round = 0; round < rounds; round += 1) {
          for (const operation of ['search', 'create', 'revise']) {
            const operationStarted = performance.now();
            if (operation === 'search') {
              const search = await call(client, 'memory_search', {
                spaces: [space.id],
                query: `LOAD-PROBE-AGENT-${clientIndex} canonical workload state`,
                mode: 'auto',
                limit: 3,
              });
              assert(
                search.results[0]?.id === seeds[clientIndex].id,
                `agent ${clientIndex} search must retrieve its exact state first`,
              );
              if (search.degraded === true) degradedSearches += 1;
            } else if (operation === 'create') {
              await call(client, 'memory_create', {
                spaceId: space.id,
                idempotencyKey: `agent-${clientIndex}-round-${round}`,
                title: `Agent ${clientIndex} progress round ${round}`,
                kind: 'load-probe-progress',
                content: { agent: clientIndex, round, status: 'complete' },
              });
            } else {
              const revised = await call(client, 'memory_revise', {
                memoryId: seeds[clientIndex].id,
                expectedRevisionId,
                title: `Agent ${clientIndex} canonical workload state`,
                kind: 'load-probe-state',
                tags: ['load-probe', `agent-${clientIndex}`],
                content: {
                  agent: clientIndex,
                  state: `round-${round}`,
                  marker: `LOAD-PROBE-AGENT-${clientIndex}`,
                },
              });
              expectedRevisionId = revised.revisionId;
            }
            measurements.push({ operation, milliseconds: performance.now() - operationStarted });
          }
        }
      }),
    );
    const elapsedSeconds = (performance.now() - startedAt) / 1_000;
    const byOperation = Object.fromEntries(
      ['search', 'create', 'revise'].map((operation) => {
        const values = measurements
          .filter((measurement) => measurement.operation === operation)
          .map((measurement) => measurement.milliseconds);
        return [
          operation,
          {
            count: values.length,
            meanMs: Math.round(values.reduce((sum, value) => sum + value, 0) / values.length),
            p95Ms: Math.round(percentile(values, 0.95)),
          },
        ];
      }),
    );
    return {
      ok: true,
      modelsEnabled,
      clients: clientsCount,
      operations: measurements.length,
      throughputOpsPerSecond: Number((measurements.length / elapsedSeconds).toFixed(2)),
      degradedSearches,
      byOperation,
    };
  } finally {
    await Promise.all(clients.map((client) => client.close().catch(() => undefined)));
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
