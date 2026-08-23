#!/usr/bin/env node
// Exercises scripts/retrieval-eval.mjs end-to-end on a deterministic corpus:
// collect -> human-style labeling -> eval scoring, including skip semantics.
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig } from '../dist/config.js';
import { Indexer } from '../dist/indexing/indexer.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-eval-probe-'));
process.env.SIMPLE_MEMORY_DATA_DIR = dataDir;
process.env.SIMPLE_MEMORY_MODELS = 'disabled';
process.env.SIMPLE_MEMORY_LOG_LEVEL = 'error';

function assert(condition, message) {
  if (!condition) throw new Error(`Retrieval eval probe assertion failed: ${message}`);
}

function runHarness(args) {
  const result = spawnSync(
    process.execPath,
    [path.join(root, 'scripts', 'retrieval-eval.mjs'), ...args],
    {
      cwd: root,
      encoding: 'utf8',
      timeout: 120_000,
    },
  );
  assert(result.status === 0, `harness exited ${result.status}: ${result.stderr}`);
  return { stdout: result.stdout, status: result.status };
}

async function main() {
  const config = loadConfig();
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const indexer = new Indexer(config, store, models, logger);

  try {
    store.createSpace({ id: 'eval-probe', name: 'Eval Probe' });
    for (const [title, note] of [
      [
        'Deployment rollback procedure',
        'Roll back a bad release by reverting the traffic switch and redeploying the previous image tag.',
      ],
      [
        'Focaccia weekend plan',
        'Bake focaccia on Sunday after slow roasting tomatoes on Saturday.',
      ],
      [
        'Incident review checklist',
        'File incident follow-up actions within five working days with named owners.',
      ],
      [
        'Cache eviction policy',
        'The retrieval cache evicts least recently used entries beyond a fixed bound.',
      ],
    ]) {
      await indexer.indexRevision(
        store.createMemory(
          {
            spaceId: 'eval-probe',
            title,
            kind: 'note',
            tags: title.includes('Deployment') ? ['deploy'] : ['ops'],
            content: { topic: title, notes: [note], flags: {}, history: [] },
            metadata: {},
            salience: 0.8,
            confidence: 0.8,
            sources: [],
          },
          'probe',
        ).revision.id,
        false,
      );
    }

    const queriesPath = path.join(dataDir, 'queries.json');
    writeFileSync(
      queriesPath,
      JSON.stringify([
        { query: 'rollback a bad deployment', spaceIds: ['eval-probe'] },
        { query: 'incident follow-up actions', spaceIds: ['eval-probe'] },
        { query: 'unanswerable zebra quantum query' },
      ]),
    );

    const collected = runHarness([
      '--data-dir',
      dataDir,
      '--collect-queries',
      queriesPath,
      '--pool-size',
      '3',
      '--mode',
      'fast',
    ]);
    assert(
      collected.stdout.trim().startsWith('{'),
      'collect output must start with the identity envelope',
    );
    const envelope = JSON.parse(collected.stdout);
    assert(envelope.corpus === 4, `collect should see the seeded corpus, got ${envelope.corpus}`);
    assert(envelope.entries.length === 3, 'collect should emit one entry per query');

    // Human labeling pass (programmatic here): grade topically correct memories.
    // The envelope itself is fed to evaluation verbatim after filling grades in place —
    // exactly what a human workflow would do, without reshaping the structure.
    for (const entry of envelope.entries) {
      for (const candidate of entry.relevant) {
        candidate.grade =
          (entry.query.startsWith('rollback') && candidate.title.startsWith('Deployment')) ||
          (entry.query.startsWith('incident') && candidate.title.startsWith('Incident'))
            ? 2
            : 0;
      }
    }
    // Ensure each scorable query has exactly one relevant hit in its pool.
    for (const entry of envelope.entries.slice(0, 2)) {
      assert(
        entry.relevant.some((candidate) => candidate.grade >= 1),
        `expected a relevant candidate in pool for "${entry.query}"`,
      );
    }
    // The third entry stays all-zero -> must be skipped by scoring, not counted as perfect.

    const judgmentsPath = path.join(dataDir, 'judgments.json');
    writeFileSync(
      judgmentsPath,
      `${JSON.stringify({ corpus: envelope.corpus, entries: envelope.entries }, null, 2)}\n`,
    );
    const scored = runHarness(['--data-dir', dataDir, '--judgments', judgmentsPath]);
    const summaryStart = scored.stdout.lastIndexOf('\n{');
    assert(
      summaryStart !== -1,
      `eval output must end with a JSON summary; exit=${scored.status} tail=${scored.stdout.slice(-300)}`,
    );
    const summary = JSON.parse(scored.stdout.slice(summaryStart));
    assert(summary.ok === true, 'eval summary must be ok');
    assert(
      summary.identity.collectedCorpus === 4,
      'eval identity must preserve collection-envelope corpus',
    );
    assert(
      summary.queriesScored === 2,
      `two entries should be scored, got ${summary.queriesScored}`,
    );
    assert(summary.queriesSkipped === 1, 'all-zero judgment entry must be skipped');
    assert(summary.recallAt10 === 1, 'seeded relevant docs must be recalled');
    assert(
      summary.ndcgAt5 > 0.9,
      `nDCG@5 should be near-perfect on this corpus, got ${summary.ndcgAt5}`,
    );
    assert(summary.identity.corpus.memories === 4, 'identity must fingerprint the corpus');
    assert(
      typeof summary.meanStageTimingsMs.lexicalMs === 'number',
      'stage timing averages must be reported',
    );

    process.stdout.write(
      `${JSON.stringify({
        ok: true,
        queriesScored: summary.queriesScored,
        queriesSkipped: summary.queriesSkipped,
        recallAt10: summary.recallAt10,
        ndcgAt10: summary.ndcgAt10,
      })}\n`,
    );
  } finally {
    await models.stop().catch(() => {});
    store.close();
    rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
  }
}

await main();
