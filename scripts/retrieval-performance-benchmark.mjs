#!/usr/bin/env node
// Compare builds with identical passages and a copied corpus, using real CPU/CUDA workers.
// Requires --passages captured.json and --snapshot memory.db; model caches are warmed,
// then score caches are cleared between trials. No writes reach the source database.
import { createHash } from 'node:crypto';
import { copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { loadConfig } from '../dist/config.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = { dist: path.join(root, 'dist'), device: 'cpu', trials: 3 };
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (
    !value ||
    ![
      '--dist',
      '--device',
      '--trials',
      '--passages',
      '--snapshot',
      '--output',
      '--scenario',
    ].includes(key)
  ) {
    throw new Error(
      'Usage: --passages captured.json --snapshot memory.db --output result.json [--dist dir] [--device cpu|cuda] [--trials 3] [--scenario name]',
    );
  }
  args[key.slice(2)] = key === '--trials' ? Number(value) : value;
}
if (
  !args.passages ||
  !args.snapshot ||
  !args.output ||
  !['cpu', 'cuda'].includes(args.device) ||
  !Number.isInteger(args.trials) ||
  args.trials < 1
) {
  throw new Error('Required benchmark arguments are missing or invalid');
}
if (
  args.scenario &&
  ![
    'fresh scoring',
    'overlapping recall',
    'concurrent scoring',
    'small concurrent scoring',
    'full quality search',
    'overlapping quality search',
  ].includes(args.scenario)
) {
  throw new Error('Unknown benchmark scenario');
}
const cases = JSON.parse(readFileSync(args.passages, 'utf8'));
if (
  cases.length < 4 ||
  cases.some((entry) => typeof entry.query !== 'string' || entry.candidates.length < 11)
) {
  throw new Error('Provide at least four captured queries with eleven passages each');
}
const load = (relative) => import(pathToFileURL(path.resolve(args.dist, relative)).href);
const [{ Logger }, { ModelClient }, { MemoryStore }, { SearchEngine }] = await Promise.all([
  load('logger.js'),
  load('models/model-client.js'),
  load('storage/memory-store.js'),
  load('retrieval/search-engine.js'),
]);
const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-performance-'));
if (!dataDir.startsWith(`${path.resolve(tmpdir())}${path.sep}`))
  throw new Error('Unexpected benchmark directory');
copyFileSync(args.snapshot, path.join(dataDir, 'memory.db'));
const config = {
  ...loadConfig(),
  dataDir,
  databasePath: path.join(dataDir, 'memory.db'),
  modelsEnabled: true,
  device: args.device,
  localFilesOnly: true,
};
const logger = new Logger('error');
const store = new MemoryStore(config, logger);
const models = new ModelClient(config, logger);
const engine = new SearchEngine(config, store, models, logger);
const counts = { pairs: 0, batches: 0 };
const dispatch = models.transport.dispatch.bind(models.transport);
models.transport.dispatch = (operation, payload) => {
  if (operation === 'rerank_pairs') {
    counts.pairs += payload.pairs.length;
    counts.batches += 1;
  }
  return dispatch(operation, payload);
};
const hash = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const round = (value) => Math.round(value * 100) / 100;
function summarize(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    meanMs: round(values.reduce((a, b) => a + b, 0) / values.length),
    p50Ms: round(sorted[Math.floor(sorted.length / 2)]),
    p95Ms: round(sorted[Math.ceil(sorted.length * 0.95) - 1]),
  };
}
function reset() {
  models.rerankCache.clear();
  models.queryEmbeddingCache.clear();
}
async function trial(operation) {
  reset();
  const before = { ...counts };
  const started = performance.now();
  const outputs = await operation();
  return {
    ms: round(performance.now() - started),
    pairs: counts.pairs - before.pairs,
    batches: counts.batches - before.batches,
    outputs,
  };
}
async function scenario(name, operation) {
  if (args.scenario && args.scenario !== name) return undefined;
  const runs = [];
  for (let index = 0; index < args.trials; index += 1) runs.push(await trial(operation));
  const result = { ...summarize(runs.map((run) => run.ms)), runs };
  process.stderr.write(`${args.device} ${name}: ${result.p50Ms} ms\n`);
  return result;
}
const report = {
  device: args.device,
  trials: args.trials,
  passageFixtureHash: hash(cases),
  sections: {},
};
try {
  const started = performance.now();
  await models.rerank(
    'warm-up',
    cases[0].candidates.slice(0, 4).map((candidate) => candidate.document),
  );
  await models.embedQuery('Warm the existing embedding model for the benchmark.');
  report.excludedStartupMs = round(performance.now() - started);
  report.modelHealth = await models.health();
  report.corpus = store.database
    .prepare("SELECT count(*) AS active FROM memories WHERE state = 'active'")
    .get();
  report.sections.freshScoring = await scenario('fresh scoring', async () => {
    const outputs = [];
    for (const entry of cases)
      outputs.push(
        await models.rerank(
          entry.query,
          entry.candidates.slice(0, 10).map((candidate) => candidate.document),
        ),
      );
    return outputs;
  });
  report.sections.overlappingRecall = await scenario('overlapping recall', async () => {
    const outputs = [];
    for (const entry of cases.slice(0, 2)) {
      const documents = entry.candidates.slice(0, 11).map((candidate) => candidate.document);
      for (const subset of [
        documents.slice(0, 6),
        documents.slice(0, 10),
        documents.slice(0, 10).reverse(),
        documents,
      ]) {
        outputs.push(await models.rerank(entry.query, subset));
      }
    }
    return outputs;
  });
  report.sections.concurrentScoring = await scenario('concurrent scoring', async () => {
    const latencies = [];
    const scores = await Promise.all(
      Array.from({ length: 8 }, async (_, index) => {
        const entry = cases[index % cases.length];
        const query = index < cases.length ? entry.query : `Please recall: ${entry.query}`;
        const started = performance.now();
        const result = await models.rerank(
          query,
          entry.candidates.slice(0, 3).map((candidate) => candidate.document),
        );
        latencies.push(performance.now() - started);
        return result;
      }),
    );
    return { scores, requestLatency: summarize(latencies) };
  });
  report.sections.smallConcurrentScoring = await scenario('small concurrent scoring', async () => {
    const latencies = [];
    const entry = cases[0];
    const scores = await Promise.all(
      Array.from({ length: 8 }, async (_, index) => {
        const started = performance.now();
        const result = await models.rerank(`${entry.query} Case ${index}`, [
          entry.candidates[0].document,
        ]);
        latencies.push(performance.now() - started);
        return result;
      }),
    );
    return { scores, requestLatency: summarize(latencies) };
  });
  const queries = [
    { query: 'Paraboloid orthographic point size projection correction', spaceIds: ['lidar-lt'] },
    {
      query: 'SectorNavigation light mode center button amber dot recenter',
      spaceIds: ['lidar-lt'],
    },
    { query: cases[0].query, spaceIds: ['lidar-lt-scripts'] },
    { query: cases[1].query, spaceIds: ['lidar-lt'] },
    { query: cases[2].query, spaceIds: ['lidar-lt'] },
    { query: 'src/index.css light-mode center dot', spaceIds: ['lidar-lt'] },
    { query: 'project/current-handoff', spaceIds: ['lidar-lt-scripts'] },
  ];
  report.sections.qualitySearch = await scenario('full quality search', async () => {
    const outputs = [];
    for (const options of queries) {
      const response = await engine.search({ ...options, mode: 'quality', topK: 5 });
      if (response.degraded) throw new Error(response.degradationReason);
      outputs.push({
        query: options.query,
        ms: response.timingMs,
        stages: response.stageTimings,
        results: response.results.map((result) => ({
          id: result.memory.id,
          revisionId: result.memory.revision.id,
          logicalKey: result.memory.logicalKey,
          title: result.memory.revision.title,
          excerpt: result.excerpt,
          path: result.segmentPath,
          score: result.score,
        })),
      });
    }
    return outputs;
  });
  report.sections.overlappingQualitySearch = await scenario(
    'overlapping quality search',
    async () => {
      const outputs = [];
      for (const [index, spaceId] of ['lidar-lt-scripts', 'lidar-lt'].entries()) {
        for (const topK of [2, 5, 3, 6]) {
          const response = await engine.search({
            query: cases[index].query,
            spaceIds: [spaceId],
            mode: 'quality',
            topK,
          });
          if (response.degraded) throw new Error(response.degradationReason);
          outputs.push({
            query: cases[index].query,
            topK,
            results: response.results.map((result) => ({
              id: result.memory.id,
              revisionId: result.memory.revision.id,
              logicalKey: result.memory.logicalKey,
              title: result.memory.revision.title,
              excerpt: result.excerpt,
              path: result.segmentPath,
              score: result.score,
            })),
          });
        }
      }
      return outputs;
    },
  );
  let sqlCalls = 0;
  for (const method of ['allRows', 'getRow']) {
    const original = store[method].bind(store);
    store[method] = (...parameters) => {
      sqlCalls += 1;
      return original(...parameters);
    };
  }
  const lexicalRuns = [];
  const lexicalQuery = 'Potree viewer loading performance';
  const filters = { spaceIds: ['lidar-lt'] };
  store.lexicalCandidates(lexicalQuery, filters, 100);
  for (let index = 0; index < 100; index += 1) {
    sqlCalls = 0;
    const started = performance.now();
    const results = store.lexicalCandidates(lexicalQuery, filters, 100);
    lexicalRuns.push({
      ms: performance.now() - started,
      sqlCalls,
      count: results.length,
      hash: hash(results),
    });
  }
  report.sections.lexicalReads = {
    ...summarize(lexicalRuns.map((run) => run.ms)),
    sqlCalls: lexicalRuns[0].sqlCalls,
    count: lexicalRuns[0].count,
    hash: lexicalRuns[0].hash,
    stable: lexicalRuns.every((run) => run.hash === lexicalRuns[0].hash),
  };
  writeFileSync(args.output, `${JSON.stringify(report, null, 2)}\n`);
  process.stdout.write(
    `${JSON.stringify({
      output: args.output,
      device: report.device,
      sections: Object.fromEntries(
        Object.entries(report.sections)
          .filter(([, section]) => section)
          .map(([name, section]) => [
            name,
            {
              p50Ms: section.p50Ms,
              pairs: section.runs?.[0]?.pairs,
              batches: section.runs?.[0]?.batches,
              sqlCalls: section.sqlCalls,
            },
          ]),
      ),
    })}\n`,
  );
} finally {
  await models.stop();
  store.close();
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
