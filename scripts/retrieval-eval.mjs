#!/usr/bin/env node
// Retrieval quality evaluation harness.
//
// Collect mode (build a judgment skeleton for human labeling; output feeds back as --judgments):
//   node scripts/retrieval-eval.mjs --data-dir <dir> --collect-queries queries.json [--pool-size 10] [--mode auto]
//   queries.json: [{ "query": "...", "spaceIds": ["..."], "kinds": ["..."], "tags": ["..."],
//                    "states": ["active"], "minConfidence": 0.5, "minSalience": 0, "mode": "auto" }]
//   A plain text file (one query per line) is also accepted.
//
// Eval mode (score searches against judged relevance):
//   node scripts/retrieval-eval.mjs --data-dir <dir> --judgments judgments.json [--top-k 10]
//
// Judgments format (identical to collected output, grades filled in by a human):
//   [{ "query": "...", "relevant": [{ "memoryId": "...", "grade": 0-3 }], ...retrievalOptions }]
// Grades: 0 = nonrelevant, 1 = marginally relevant, 2 = relevant, 3 = highly relevant.
// Entries without at least one positively graded memory are skipped, not scored.

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { loadConfig } from '../dist/config.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

function parseArgs(argv) {
  const args = { mode: 'auto', topK: 10, poolSize: 10 };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--data-dir') args.dataDir = argv[++index];
    else if (arg === '--judgments') args.judgmentsPath = argv[++index];
    else if (arg === '--collect-queries') args.collectQueriesPath = argv[++index];
    else if (arg === '--pool-size') args.poolSize = Number(argv[++index]);
    else if (arg === '--top-k') args.topK = Number(argv[++index]);
    else if (arg === '--mode') args.mode = argv[++index];
    else throw new Error(`Unknown argument: ${arg}`);
  }
  if (!args.dataDir) throw new Error('--data-dir is required');
  if (!args.judgmentsPath && !args.collectQueriesPath) {
    throw new Error('Provide either --judgments or --collect-queries');
  }
  return args;
}

const RETRIEVAL_OPTION_KEYS = [
  'spaceIds',
  'kinds',
  'tags',
  'states',
  'minConfidence',
  'minSalience',
];

function retrievalOptions(entry, defaultMode, defaultTopK) {
  const options = { mode: entry.mode ?? defaultMode, topK: entry.topK ?? defaultTopK };
  for (const key of RETRIEVAL_OPTION_KEYS) {
    if (entry[key] !== undefined) options[key] = entry[key];
  }
  return options;
}

function dcgAtK(grades, k) {
  let dcg = 0;
  grades.slice(0, k).forEach((grade, index) => {
    dcg += (2 ** grade - 1) / Math.log2(index + 2);
  });
  return dcg;
}

function ndcgAtK(resultGrades, idealGrades, k) {
  const dcg = dcgAtK(resultGrades, k);
  const idcg = dcgAtK(
    [...idealGrades].sort((left, right) => right - left),
    k,
  );
  return idcg > 0 ? dcg / idcg : null;
}

function mrrAtK(resultGrades, k) {
  const first = resultGrades.slice(0, k).findIndex((grade) => grade >= 1);
  return first === -1 ? 0 : 1 / (first + 1);
}

function recallAtK(resultIds, relevantIds, k) {
  const relevant = new Set(relevantIds);
  const hits = resultIds.slice(0, k).filter((id) => relevant.has(id)).length;
  return relevant.size === 0 ? null : hits / relevant.size;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  process.env.SIMPLE_MEMORY_DATA_DIR = args.dataDir;
  process.env.SIMPLE_MEMORY_LOG_LEVEL = process.env.SIMPLE_MEMORY_LOG_LEVEL ?? 'error';

  const config = loadConfig();
  const logger = new Logger('error');
  const store = new MemoryStore(config, logger);
  const models = new ModelClient(config, logger);
  const engine = new SearchEngine(config, store, models, logger);

  try {
    const rawStatus = store.status();
    const corpus = {
      memories: rawStatus.memories?.total ?? 0,
      active: rawStatus.memories?.active ?? 0,
      segments: rawStatus.segments ?? 0,
      revisions: rawStatus.revisions ?? 0,
      spaces: rawStatus.spaces ?? 0,
    };
    let modelIdentity = { modelsEnabled: false };
    if (config.modelsEnabled && store.vectorAvailable) {
      try {
        const health = await models.health();
        modelIdentity = {
          modelsEnabled: true,
          embeddingModel: `${health.embedding_model}@${health.embedding_revision}`,
          rerankerModel: `${health.reranker_model}@${health.reranker_revision}`,
        };
      } catch {
        modelIdentity = { modelsEnabled: true, error: 'model worker unavailable' };
      }
    }

    if (args.collectQueriesPath) {
      const raw = readFileSync(args.collectQueriesPath, 'utf8');
      let entries;
      try {
        entries = JSON.parse(raw);
        if (!Array.isArray(entries)) throw new Error('not an array');
      } catch {
        entries = raw
          .split('\n')
          .map((line) => line.trim())
          .filter((line) => line.length > 0 && !line.startsWith('#'))
          .map((query) => ({ query }));
      }
      const skeleton = [];
      for (const entry of entries) {
        const options = retrievalOptions(entry, args.mode, args.poolSize);
        const response = await engine.search({ query: entry.query, ...options });
        skeleton.push({
          query: entry.query,
          ...Object.fromEntries(
            RETRIEVAL_OPTION_KEYS.filter((key) => entry[key] !== undefined).map((key) => [
              key,
              entry[key],
            ]),
          ),
          mode: response.mode,
          relevant: response.results.map((result) => ({
            memoryId: result.memory.id,
            title: result.memory.revision.title ?? '',
            excerpt: result.excerpt.slice(0, 160),
            grade: null,
          })),
        });
      }
      process.stdout.write(
        `${JSON.stringify({ corpus: corpus.memories, entries: skeleton }, null, 2)}\n`,
      );
      return;
    }

    const judgmentsContent = readFileSync(args.judgmentsPath, 'utf8');
    const parsedJudgments = JSON.parse(judgmentsContent);
    // Accept either a bare array of judged entries or the collect-mode envelope verbatim.
    const isEnvelope = !Array.isArray(parsedJudgments) && Array.isArray(parsedJudgments?.entries);
    if (!isEnvelope && !Array.isArray(parsedJudgments)) {
      throw new Error(
        'Judgments file must be an array of entries or a collect-mode envelope ({ corpus, entries })',
      );
    }
    const judgments = isEnvelope ? parsedJudgments.entries : parsedJudgments;
    const identity = {
      recordedAt: new Date().toISOString(),
      collectedCorpus: isEnvelope ? parsedJudgments.corpus : undefined,
      corpus: {
        memories: corpus.memories,
        active: corpus.active,
        segments: corpus.segments,
        revisions: corpus.revisions,
        spaces: corpus.spaces,
      },
      model: modelIdentity,
      mode: args.mode,
      topK: args.topK,
      judgmentsSha256: createHash('sha256').update(judgmentsContent, 'utf8').digest('hex'),
    };

    const perQuery = [];
    let skipped = 0;
    for (const entry of judgments) {
      const positiveGrades = entry.relevant.filter((item) => item.grade >= 1);
      if (positiveGrades.length === 0 || !entry.query) {
        skipped += 1;
        continue;
      }
      const options = retrievalOptions(entry, args.mode, Math.max(args.topK, 10));
      const response = await engine.search({ query: entry.query, ...options });
      const gradeById = new Map(entry.relevant.map((item) => [item.memoryId, item.grade]));
      const resultIds = response.results.map((result) => result.memory.id);
      const resultGrades = resultIds.map((id) => gradeById.get(id) ?? 0);
      const idealGrades = [...gradeById.values()];
      const record = {
        query: entry.query,
        degraded: response.degraded,
        timingMs: response.timingMs,
        stageTimings: response.stageTimings ?? {},
        ndcgAt5: ndcgAtK(resultGrades, idealGrades, 5),
        ndcgAt10: ndcgAtK(resultGrades, idealGrades, 10),
        mrrAt10: mrrAtK(resultGrades, 10),
        recallAt10: recallAtK(
          resultIds,
          positiveGrades.map((item) => item.memoryId),
          10,
        ),
      };
      perQuery.push(record);
      const format = (value) => (value === null ? 'SKIP' : value.toFixed(3));
      process.stdout.write(
        `${record.query} -> nDCG@5=${format(record.ndcgAt5)} nDCG@10=${format(record.ndcgAt10)} ` +
          `MRR@10=${record.mrrAt10.toFixed(3)} R@10=${format(record.recallAt10)} ${record.timingMs}ms` +
          `${record.degraded ? ' DEGRADED' : ''}\n`,
      );
    }

    if (perQuery.length === 0) {
      process.stdout.write(
        `${JSON.stringify({ ok: false, error: 'no scoreable judgments', skipped, identity }, null, 2)}\n`,
      );
      process.exitCode = 1;
      return;
    }

    const mean = (key) =>
      perQuery.reduce((sum, row) => sum + (row[key] === null ? 0 : row[key]), 0) / perQuery.length;
    const stageKeys = ['embedMs', 'exactMs', 'lexicalMs', 'semanticMs', 'rerankMs'];
    const stageMeans = {};
    for (const key of stageKeys) {
      const values = perQuery
        .map((row) => row.stageTimings[key])
        .filter((value) => typeof value === 'number');
      stageMeans[key] =
        values.length > 0
          ? Math.round((values.reduce((sum, value) => sum + value, 0) / values.length) * 100) / 100
          : null;
    }
    const summary = {
      ok: true,
      identity,
      queriesScored: perQuery.length,
      queriesSkipped: skipped,
      ndcgAt5: mean('ndcgAt5'),
      ndcgAt10: mean('ndcgAt10'),
      mrrAt10: mean('mrrAt10'),
      recallAt10: mean('recallAt10'),
      meanTimingMs: mean('timingMs'),
      meanStageTimingsMs: stageMeans,
      perQuery,
    };
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } finally {
    await models.stop().catch(() => {});
    store.close();
  }
}

await main();
