#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-vector-metadata-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Vector metadata probe assertion failed: ${message}`);
}

function segmentFor(memory, text) {
  return {
    id: randomUUID(),
    memoryId: memory.id,
    revisionId: memory.currentRevisionId,
    spaceId: memory.spaceId,
    ordinal: 0,
    path: '$',
    text,
    tokenCount: text.split(/\s+/u).length,
    contentHash: createHash('sha256').update(text).digest('hex'),
  };
}

function run() {
  const config = loadConfig();
  const store = new MemoryStore(config, new Logger('error'));
  try {
    assert(store.vectorAvailable, 'sqlite-vec should be available');
    const profileId = store.ensureModelProfile({
      provider: 'probe',
      model: 'probe',
      modelRevision: '1',
      dimensions: config.embeddingDimension,
      instructionHash: 'probe',
    });
    const fixtures = [
      store.createMemory({ content: 'No confidence or salience values.' }),
      store.createMemory({
        content: 'Integer-valued confidence and salience.',
        confidence: 1,
        salience: 1,
      }),
    ];

    for (const memory of fixtures) {
      const segment = segmentFor(memory, String(memory.revision.content));
      store.indexSegments(memory.currentRevisionId, [segment], null, []);
      store.indexVectors([segment], [new Array(config.embeddingDimension).fill(0)], profileId);
      assert(store.setState(memory.id, 'archived').state === 'archived', 'archive should succeed');
      assert(store.setState(memory.id, 'active').state === 'active', 'restore should succeed');
    }

    return { status: 'ok', fixtures: fixtures.length, vectorAvailable: store.vectorAvailable };
  } finally {
    store.close();
  }
}

let outcome;
try {
  outcome = run();
} finally {
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
process.stdout.write(`${JSON.stringify(outcome)}\n`);
