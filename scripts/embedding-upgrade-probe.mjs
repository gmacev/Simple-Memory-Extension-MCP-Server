#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import * as sqliteVec from 'sqlite-vec';
import { MemoryService } from '../dist/application/memory-service.js';
import { loadConfig } from '../dist/config.js';
import { Indexer } from '../dist/indexing/indexer.js';
import { Logger } from '../dist/logger.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-embedding-upgrade-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Embedding upgrade probe assertion failed: ${message}`);
}

async function run() {
  const baseConfig = loadConfig();
  const config = { ...baseConfig, modelsEnabled: true };
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const expectedInstructionHash = createHash('sha256')
    .update(config.queryInstruction)
    .digest('hex');
  let embeddingCalls = 0;
  const modelProfile = {
    embedding_dimension: config.embeddingDimension,
    embedding_model: config.embeddingModel,
    embedding_revision: config.embeddingRevision,
    query_instruction_hash: expectedInstructionHash,
  };
  const fakeModels = {
    async countTokens(texts) {
      return texts.map((text) => Math.max(1, text.split(/\s+/u).length));
    },
    async embedQuery() {
      embeddingCalls += 1;
      return Array.from({ length: config.embeddingDimension }, () => 0.01);
    },
    async embedDocuments(texts) {
      embeddingCalls += 1;
      return texts.map((_, index) =>
        Array.from({ length: config.embeddingDimension }, (__, dimension) =>
          dimension === index % config.embeddingDimension ? 1 : 0,
        ),
      );
    },
    async embeddingProfile() {
      return modelProfile;
    },
    async stop() {},
  };
  const indexer = new Indexer(config, store, fakeModels, logger);
  const lexicalIndexer = new Indexer(baseConfig, store, fakeModels, logger);
  const service = new MemoryService(config, store, indexer, {}, fakeModels, logger);
  try {
    const evolving = store.createMemory({
      title: 'Evolving memory',
      content: { state: 'first' },
    });
    await lexicalIndexer.indexRevision(evolving.revision.id, false, evolving);
    const revised = store.reviseMemory(
      evolving.id,
      { title: 'Evolving memory', content: { state: 'second' } },
      evolving.currentRevisionId,
    );
    await lexicalIndexer.indexRevision(revised.revision.id, false, revised);
    const archived = store.createMemory({
      title: 'Archived memory',
      content: { retained: true },
    });
    await lexicalIndexer.indexRevision(archived.revision.id, false, archived);
    store.setState(archived.id, 'archived');
    store.createSpace({ id: 'soft-deleted-space', name: 'Soft deleted space' });
    const hidden = store.createMemory({
      spaceId: 'soft-deleted-space',
      title: 'Hidden memory',
      content: { restorable: true },
    });
    await lexicalIndexer.indexRevision(hidden.revision.id, false, hidden);
    store.deleteSpace('soft-deleted-space');
    const revisionIds = [
      evolving.revision.id,
      revised.revision.id,
      archived.revision.id,
      hidden.revision.id,
    ];
    const segmentSnapshot = JSON.stringify(
      revisionIds.flatMap((revisionId) => store.segmentsForRevision(revisionId)),
    );

    const disabledService = new MemoryService(baseConfig, store, indexer, {}, fakeModels, logger);
    let skipModelsRejected = false;
    try {
      await disabledService.upgradeEmbeddingIndex();
    } catch (error) {
      skipModelsRejected = String(error).includes('without --skip-models');
    }
    assert(
      skipModelsRejected,
      'an existing database requiring re-embedding should reject --skip-models',
    );

    const messages = [];
    const first = await service.upgradeEmbeddingIndex((message) => messages.push(message));
    assert(first.required, 'the first profile-aware upgrade should rebuild the index');
    assert(first.generation.status === 'complete', 'the first generation should complete');
    assert(first.generation.total === 4, 'all four retained revisions should be rebuilt');
    assert(first.indexed === 4 && first.failed === 0, 'all revisions should index successfully');
    assert(
      JSON.stringify(revisionIds.flatMap((revisionId) => store.segmentsForRevision(revisionId))) ===
        segmentSnapshot,
      're-embedding should preserve existing lexical segments byte-for-byte',
    );
    assert(
      messages.some((message) => message.includes('One-time semantic index upgrade')),
      'the upgrade should explain the one-time rebuild',
    );

    const callsAfterFirstUpgrade = embeddingCalls;
    const second = await service.upgradeEmbeddingIndex();
    assert(!second.required, 'an unchanged embedding profile should be reused');
    assert(
      embeddingCalls === callsAfterFirstUpgrade,
      'the no-op update must not invoke embeddings',
    );

    const inspection = new Database(config.databasePath, { readonly: true });
    sqliteVec.load(inspection);
    const vectorCount = inspection
      .prepare('SELECT COUNT(*) AS count FROM memory_vectors')
      .get().count;
    const currentVectorCount = inspection
      .prepare('SELECT COUNT(*) AS count FROM memory_current_vectors')
      .get().count;
    const profileCount = inspection
      .prepare('SELECT COUNT(*) AS count FROM model_profiles WHERE dimensions = 896')
      .get().count;
    inspection.close();
    assert(vectorCount > currentVectorCount, 'historical vectors should be preserved separately');
    assert(currentVectorCount === 3, 'all three current revisions should have current vectors');
    assert(profileCount === 1, 'the rebuilt vectors should share one F2LLM profile');

    const alternateProfile = {
      ...modelProfile,
      embedding_model: `${config.embeddingModel}-alternate`,
    };
    const alternateGeneration = store.prepareEmbeddingGeneration({
      provider: 'huggingface',
      model: alternateProfile.embedding_model,
      modelRevision: alternateProfile.embedding_revision,
      dimensions: alternateProfile.embedding_dimension,
      instructionHash: alternateProfile.query_instruction_hash,
    });
    const startupRecovery = await service.reindexPending();
    assert(
      startupRecovery.indexed === 0 && startupRecovery.failed === 0,
      'startup recovery must not claim a current generation for a different configured profile',
    );
    const untouchedAlternate = store.embeddingGenerationProgress({
      provider: 'huggingface',
      model: alternateProfile.embedding_model,
      modelRevision: alternateProfile.embedding_revision,
      dimensions: alternateProfile.embedding_dimension,
      instructionHash: alternateProfile.query_instruction_hash,
    });
    assert(
      untouchedAlternate?.pending === 4 && untouchedAlternate.failed === 0,
      'wrong-profile startup recovery must leave generation jobs untouched',
    );

    const mismatchedDrain = await indexer.indexPending(undefined, {
      embeddingGenerationId: alternateGeneration.id,
    });
    assert(
      mismatchedDrain.indexed === 0 && mismatchedDrain.failed === 4,
      'the indexer must reject generation work when the loaded model profile differs',
    );
    return {
      ok: true,
      generation: first.generation,
      vectorCount,
      currentVectorCount,
      embeddingCalls,
      noOpReused: !second.required,
      wrongProfileStartupSkipped: startupRecovery.indexed === 0,
      mismatchedGenerationRejected: mismatchedDrain.failed === 4,
    };
  } finally {
    await service.close();
  }
}

let result;
let failure;
try {
  result = await run();
} catch (error) {
  failure = error;
}
try {
  const resolvedTemp = path.resolve(tmpdir());
  const resolvedData = path.resolve(dataDir);
  if (resolvedData.startsWith(`${resolvedTemp}${path.sep}`)) {
    rmSync(resolvedData, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
} catch (cleanupError) {
  process.stderr.write(`Embedding upgrade probe cleanup warning: ${String(cleanupError)}\n`);
}
if (failure) throw failure;
process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
