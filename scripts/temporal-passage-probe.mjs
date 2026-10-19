import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { createSegments } from '../dist/indexing/projector.js';
import { Logger } from '../dist/logger.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import { calendarDates, datePassages } from '../dist/retrieval/temporal.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const dir = mkdtempSync(path.join(tmpdir(), 'memory-passages-'));
const config = {
  ...loadConfig(),
  dataDir: dir,
  databasePath: path.join(dir, 'test.db'),
  modelsEnabled: false,
};
const logger = new Logger('error');
const store = new MemoryStore(config, logger);
const models = new ModelClient(config, logger);
const engine = new SearchEngine(config, store, models, logger);
const windows = [8, 9].map((day) => ({
  start: `2026-01-0${day}T00:00:00.000Z`,
  end: `2026-01-${day + 1 < 10 ? '0' : ''}${day + 1}T00:00:00.000Z`,
}));
const cases = [
  [
    'sentences',
    'On 2026-01-08 Mira used ALPHA. It required a restart. On 2026-01-09 Mira used BETA.',
    'ALPHA',
    'BETA',
    'restart',
  ],
  ['suffix', 'Mira used ALPHA on 2026-01-08. Mira used BETA on 2026-01-09.', 'ALPHA', 'BETA'],
  ['clauses', '2026-01-08: ALPHA; 2026-01-09: BETA', 'ALPHA', 'BETA'],
  ['lines', '2026-01-08: ALPHA\n2026-01-09: BETA', 'ALPHA', 'BETA'],
  [
    'headings',
    '# 2026-01-08\n\nMira used ALPHA.\n\nRestart required.\n\n# 2026-01-09\n\nMira used BETA.',
    'ALPHA',
    'BETA',
    'Restart',
  ],
  [
    'lt',
    '2026 m. sausio 8 d. Mira naudojo ALPHA. Reikėjo paleisti iš naujo. 2026 m. sausio 9 d. Mira naudojo BETA.',
    'ALPHA',
    'BETA',
    'iš naujo',
  ],
  [
    'ja',
    '2026年1月8日にミラはALPHAを使用した。再起動した。2026年1月9日にミラはBETAを使用した。',
    'ALPHA',
    'BETA',
    '再起動',
  ],
  [
    'ar',
    'في ٨ يناير ٢٠٢٦ استخدمت ميرا ALPHA. ثم أعادت التشغيل. في ٩ يناير ٢٠٢٦ استخدمت ميرا BETA.',
    'ALPHA',
    'BETA',
    'التشغيل',
  ],
  ['repeat', '2026-01-08: ALPHA; 2026-01-09: BETA; 2026-01-08: GAMMA', 'ALPHA', 'BETA', 'GAMMA'],
  [
    'unicode-offset',
    '🧪 On 2026-01-08 Mira used ALPHA. On 2026-01-09 Mira used BETA.',
    'ALPHA',
    'BETA',
  ],
  [
    'shared-context',
    'This is the staging cluster. On 2026-01-08 Mira used ALPHA. On 2026-01-09 Mira used BETA.',
    'ALPHA',
    'BETA',
    'staging cluster',
  ],
  [
    'sections',
    '# Staging\nOn 2026-01-08 Mira used ALPHA.\n# Production\nOn 2026-01-09 Mira used BETA.',
    'ALPHA',
    'BETA',
    'Staging',
  ],
];
try {
  for (const text of [
    'Mira used ALPHA from 2026-01-08 through 2026-01-09.',
    'Mira used ALPHA on 2026-01-08, not 2026-01-09.',
    'On 2026-01-08 Mira used ALPHA. Important undated context follows.',
  ]) {
    const passages = datePassages(text, calendarDates(text));
    assert.equal(passages.length, 1, 'Do not invent a split inside a range or comparison');
    assert.equal(text.slice(passages[0].start, passages[0].end), text);
  }
  for (const [name, content, wanted, excluded, continuation] of cases) {
    store.createSpace({ id: name, name });
    const m = store.createMemory({ spaceId: name, content, kind: 'event' }, 'probe');
    const segments = createSegments({
      memoryId: m.id,
      revisionId: m.revision.id,
      spaceId: name,
      content,
      kind: 'event',
      tags: [],
      metadata: {},
      sources: [],
    });
    store.indexSegments(m.revision.id, segments, null, []);
    const opts = { query: 'Mira ALPHA BETA', spaceIds: [name], mode: 'lexical', topK: 5 };
    const single = await engine.search({ ...opts, timeWindows: [windows[0]] });
    const excerpt = single.results[0].excerpt;
    assert(excerpt.includes(wanted), `${name}: missing answer: ${excerpt}`);
    assert(!excerpt.includes(excluded), `${name}: wrong day leaked: ${excerpt}`);
    if (continuation)
      assert(excerpt.includes(continuation), `${name}: lost continuation: ${excerpt}`);
    if (name === 'sections') {
      assert(!excerpt.includes('Production'));
      const second = await engine.search({ ...opts, timeWindows: [windows[1]] });
      assert(second.results[0].excerpt.includes('Production'));
      assert(!second.results[0].excerpt.includes('Staging'));
    }
    const both = await engine.search({ ...opts, timeWindows: windows });
    assert(
      both.results[0].excerpt.includes(wanted) && both.results[0].excerpt.includes(excluded),
      `${name}: lost multi-date evidence`,
    );
    const off = await engine.search({ ...opts, timeWindows: [] });
    if (off.results.length > 0)
      assert(off.results[0].excerpt.includes(wanted) && off.results[0].excerpt.includes(excluded));
    assert.equal(store.getMemory(m.id).revision.content, content);
  }
  console.log(
    JSON.stringify({
      status: 'ok',
      cases: cases.length,
      coverage:
        'passage isolation, continuation, headings, same-day repeats, multilingual boundaries, multiple windows, opt-out, original preservation',
    }),
  );
} finally {
  await models.stop();
  store.close();
  rmSync(dir, { recursive: true, force: true });
}
