import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { createSegments } from '../dist/indexing/projector.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { calendarDates, datedExcerpt, queryTimeWindows } from '../dist/retrieval/temporal.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const window = { start: '2026-02-05T00:00:00.000Z', end: '2026-02-06T00:00:00.000Z' };
const examples = [
  [
    'en',
    'What configuration did Mira use on 5 February 2026?',
    'On 5 February 2026 Mira changed the configuration to BLUE.',
  ],
  [
    'lt',
    'Kokią konfigūraciją Mira naudojo 2026 m. vasario 5 d.?',
    '2026 m. vasario 5 d. Mira pakeitė konfigūraciją į MĖLYNA.',
  ],
  [
    'es',
    '¿Qué configuración usó Mira el 5 de febrero de 2026?',
    'El 5 de febrero de 2026 Mira cambió la configuración a AZUL.',
  ],
  [
    'fr',
    'Quelle configuration Mira utilisait-elle le 5 février 2026 ?',
    'Le 5 février 2026 Mira a changé la configuration en BLEU.',
  ],
  [
    'de',
    'Welche Konfiguration verwendete Mira am 5. Februar 2026?',
    'Am 5. Februar 2026 änderte Mira die Konfiguration auf BLAU.',
  ],
  [
    'ru',
    'Какую конфигурацию Мира использовала 5 февраля 2026 года?',
    '5 февраля 2026 года Мира изменила конфигурацию на СИНИЙ.',
  ],
  [
    'ja',
    'ミラは2026年2月5日にどの設定を使いましたか？',
    'ミラは2026年2月5日に設定を青に変更した。',
  ],
  ['zh', '米拉在2026年2月5日使用了什么配置？', '米拉在2026年2月5日把配置改为蓝色。'],
  [
    'ar',
    'ما الإعداد الذي استخدمته ميرا في ٥ فبراير ٢٠٢٦؟',
    'في ٥ فبراير ٢٠٢٦ غيّرت ميرا الإعداد إلى أزرق.',
  ],
];
for (const [language, query, content] of examples) {
  for (const text of [query, content]) {
    const dates = calendarDates(text);
    assert.equal(dates.length, 1, `${language}: ${text}`);
    assert.equal(dates[0].start, window.start, language);
    assert.equal(
      text.slice(dates[0].offset, dates[0].offset + dates[0].text.length),
      dates[0].text,
    );
  }
}
assert.equal(calendarDates('١٢٣٤ ٥/٦/٢٠٢٦ 05/06/2026 3.11.0').length, 0);
assert.equal(calendarDates('https://host/2026年2月5日/report').length, 0);
assert.equal(calendarDates('2026 m. vasario 31 d.').length, 0);
assert.equal(calendarDates('31 février 2026').length, 0);
assert.equal(
  calendarDates('5 listopad 2026').length,
  0,
  'conflicting month names must not choose a language',
);
const arabicText = `😀 ${'سياق '.repeat(500)}في ٥ فبراير ٢٠٢٦ الإعداد أزرق`;
assert(datedExcerpt(arabicText, window, 2000).includes('الإعداد أزرق'));
// Recognizing a date must not silently infer the meaning of an unfamiliar temporal relation.
assert.equal(queryTimeWindows('Kokia konfigūracija naudota prieš 2026 m. vasario 5 d.?').length, 0);
assert.equal(queryTimeWindows('Quelle configuration avant le 5 février 2026 ?').length, 0);

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-multilingual-'));
const config = {
  ...loadConfig(),
  dataDir,
  databasePath: path.join(dataDir, 'memory.db'),
  modelsEnabled: false,
};
const logger = new Logger('error');
const store = new MemoryStore(config, logger);
const models = new ModelClient(config, logger);
const engine = new SearchEngine(config, store, models, logger);
try {
  for (const [language, query, content] of examples) {
    store.createSpace({ id: language, name: language });
    const memory = store.createMemory({ spaceId: language, content }, 'probe');
    store.indexSegments(
      memory.revision.id,
      createSegments({
        memoryId: memory.id,
        revisionId: memory.revision.id,
        spaceId: language,
        content,
        tags: [],
        metadata: {},
        sources: [],
      }),
      undefined,
      [],
    );
    const results = await engine.search({
      query,
      spaceIds: [language],
      mode: 'lexical',
      timeWindows: [window],
      topK: 1,
    });
    assert.equal(results.results[0]?.memory.id, memory.id, language);
    assert.equal(results.results[0]?.score.timeMatch?.basis, 'content', language);
    assert.equal(results.temporal.source, 'explicit');
    const dateOnly = store.temporalCandidates('完全別の話', { spaceIds: [language] }, [window], 50);
    assert(dateOnly.length > 0, language);
    assert(
      dateOnly.every((row) => row.memoryId === memory.id),
      'date-only fallback must preserve scope',
    );
  }
  console.log(
    JSON.stringify({
      status: 'ok',
      languages: examples.map(([language]) => language),
      coverage:
        'same-language dates, source offsets, Unicode digits, invalid dates, ambiguity rejection, URL exclusion, explicit-window retrieval, unsupported-relation fallback',
    }),
  );
} finally {
  await models.stop();
  store.close();
  assert.equal(path.dirname(dataDir), path.resolve(tmpdir()));
  assert(path.basename(dataDir).startsWith('simple-memory-multilingual-'));
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
