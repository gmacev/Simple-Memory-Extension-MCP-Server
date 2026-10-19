import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { loadConfig } from '../dist/config.js';
import { createSegments } from '../dist/indexing/projector.js';
import { Logger } from '../dist/logger.js';
import { toolInputSchemas } from '../dist/mcp/input-schemas.js';
import { ModelClient } from '../dist/models/model-client.js';
import { SearchEngine } from '../dist/retrieval/search-engine.js';
import {
  calendarDates,
  datedExcerpt,
  queryTimeWindows,
  recordedSnapshotTime,
  segmentTimes,
} from '../dist/retrieval/temporal.js';
import { MemoryStore } from '../dist/storage/memory-store.js';

const start = '2026-01-08T00:00:00.000Z',
  end = '2026-01-09T00:00:00.000Z';
const window = { start, end };
assert.equal(calendarDates('on 31 February 2026').length, 0);
assert.equal(calendarDates('on 29 February 2024')[0].start, '2024-02-29T00:00:00.000Z');
assert.equal(calendarDates('https://host/2026-01-08/report').length, 0);
assert.equal(queryTimeWindows('What happened on 08/01/2026?').length, 0);
assert.equal(queryTimeWindows('What happened after 8 January 2026?').length, 0);
assert.equal(queryTimeWindows('When is the next event?').length, 0);
assert.equal(queryTimeWindows('What happened after moving on 8 January 2026?').length, 0);
assert.deepEqual(queryTimeWindows('What happened on January 8, 2026?'), [window]);
assert.deepEqual(queryTimeWindows('What happened on 8 January 2026?'), [window]);
assert.deepEqual(queryTimeWindows('What happened the day after 7 January 2026?'), [window]);
assert.equal(queryTimeWindows('What happened on 8 January 2026 and 10 January 2026?').length, 2);
assert.deepEqual(
  queryTimeWindows('What happened on 8 January 2026 and the day after 9 January 2026?').map(
    (w) => w.start,
  ),
  ['2026-01-08T00:00:00.000Z', '2026-01-10T00:00:00.000Z'],
);
assert.equal(
  queryTimeWindows('What happened on 8 January 2026 and after 10 January 2026?').length,
  0,
);
assert.equal(recordedSnapshotTime('What was recorded as of 2026-01-08T25:00:00Z?'), undefined);
assert.equal(
  segmentTimes(
    [{ id: 'relative', path: '$', text: '$: Mira changed the configuration yesterday.' }],
    '2026-01-09T12:00:00.000Z',
  )[0].start,
  start,
);
assert.equal(
  segmentTimes(
    [{ id: 'relative', path: '$', text: '$: Mira changed the configuration yesterday.' }],
    null,
  ).length,
  0,
);
assert(
  datedExcerpt(
    `${'Context '.repeat(400)}On 2026-01-08 the answer was TARGET-AT-END.`,
    window,
    2000,
  ).includes('TARGET-AT-END'),
);
assert.equal(
  queryTimeWindows('What happened between 8 January 2026 and 10 January 2026?')[0].end,
  '2026-01-11T00:00:00.000Z',
);
assert.equal(
  recordedSnapshotTime('What was the latest recorded status as of 2026-01-08T10:00:00Z?'),
  '2026-01-08T10:00:00.000Z',
);
assert.equal(recordedSnapshotTime('What happened as of 8 January 2026?'), undefined);
assert.equal(
  recordedSnapshotTime('What was known as of 8 January 2026?'),
  '2026-01-08T23:59:59.999Z',
);
assert.equal(
  toolInputSchemas.memory_search.safeParse({
    query: 'x',
    timeWindows: [{ start: end, end: start }],
  }).success,
  false,
);
assert.equal(
  toolInputSchemas.memory_search.safeParse({ query: 'x', timeWindows: [] }).success,
  true,
);

const dataDir = mkdtempSync(path.join(tmpdir(), 'simple-memory-temporal-'));
const config = {
  ...loadConfig(),
  dataDir,
  databasePath: path.join(dataDir, 'memory.db'),
  modelsEnabled: false,
};
const logger = new Logger('error'),
  store = new MemoryStore(config, logger),
  models = new ModelClient(config, logger),
  engine = new SearchEngine(config, store, models, logger);
const index = (m) =>
  store.indexSegments(
    m.revision.id,
    createSegments({
      memoryId: m.id,
      revisionId: m.revision.id,
      spaceId: m.spaceId,
      title: m.revision.title,
      kind: m.revision.kind,
      content: m.revision.content,
      tags: m.revision.tags,
      metadata: m.revision.metadata,
      sources: m.revision.sources,
    }),
    m.revision.title,
    m.revision.tags,
  );
const add = (input) => {
  const m = store.createMemory(input, 'probe');
  index(m);
  return m;
};
try {
  store.createSpace({ id: 'events', name: 'Events' });
  store.createSpace({ id: 'hidden', name: 'Hidden' });
  let target;
  for (let i = 1; i <= 120; i++) {
    const date = new Date(Date.UTC(2026, 0, i)).toISOString();
    const m = add({
      spaceId: 'events',
      content: `Mira changed deployment configuration to token${i}.`,
      observedAt: date,
      kind: 'event',
      tags: ['allowed'],
      confidence: 0.9,
      salience: 0.8,
    });
    if (i === 8) target = m;
  }
  const query = 'What deployment configuration did Mira change on 8 January 2026?';
  const result = await engine.search({ query, spaceIds: ['events'], mode: 'lexical', topK: 5 });
  assert.equal(result.results[0].memory.id, target.id);
  assert.equal(result.results[0].score.timeMatch.basis, 'observed');
  assert.deepEqual(result.temporal.windows, [window]);
  const explicit = await engine.search({
    query: 'Mira deployment configuration',
    spaceIds: ['events'],
    mode: 'lexical',
    timeWindows: [window],
  });
  assert.equal(explicit.results[0].memory.id, target.id);
  assert.equal(explicit.temporal.source, 'explicit');
  const off = await engine.search({
    query,
    spaceIds: ['events'],
    mode: 'lexical',
    timeWindows: [],
  });
  assert.equal(off.temporal, undefined);
  const nondated = {
    query: 'Mira deployment configuration',
    spaceIds: ['events'],
    mode: 'lexical',
  };
  const normal = await engine.search(nondated),
    disabled = await engine.search({ ...nondated, timeWindows: [] });
  assert.deepEqual(normal.results, disabled.results);
  const hidden = add({
    spaceId: 'hidden',
    content: 'Mira deployment configuration secret.',
    observedAt: start,
  });
  assert(
    !store
      .temporalCandidates(query, { spaceIds: ['events'] }, [window], 100)
      .some((r) => r.memoryId === hidden.id),
  );
  assert.equal(store.temporalCandidates(query, { spaceIds: [] }, [window], 100).length, 0);
  for (const filters of [
    { kinds: ['other'] },
    { tags: ['missing'] },
    { minConfidence: 1 },
    { minSalience: 1 },
  ])
    assert.equal(
      store.temporalCandidates(query, { spaceIds: ['events'], ...filters }, [window], 100).length,
      0,
    );
  store.setState(target.id, 'archived');
  assert.equal(store.temporalCandidates(query, { spaceIds: ['events'] }, [window], 100).length, 0);
  assert.equal(
    store.temporalCandidates(
      query,
      { spaceIds: ['events'], states: ['archived'] },
      [window],
      100,
    )[0].memoryId,
    target.id,
  );
  store.setState(target.id, 'active');
  const bound = add({
    spaceId: 'events',
    content: 'Mira deployment configuration future-boundary.',
    observedAt: end,
  });
  assert(
    !store
      .temporalCandidates(query, { spaceIds: ['events'] }, [window], 100)
      .some((r) => r.memoryId === bound.id),
  );
  const expired = add({
    spaceId: 'events',
    content: 'Mira deployment configuration expired.',
    observedAt: start,
    expiresAt: '2020-01-01T00:00:00.000Z',
  });
  const invalid = add({
    spaceId: 'events',
    content: 'Mira deployment configuration invalid.',
    observedAt: start,
    validFrom: '2030-01-01T00:00:00.000Z',
  });
  assert(
    !store
      .temporalCandidates(query, { spaceIds: ['events'] }, [window], 100)
      .some((r) => [expired.id, invalid.id].includes(r.memoryId)),
  );
  const both = add({
    spaceId: 'events',
    content: 'On 2026-01-08 Mira deployment configuration alpha; on 2026-01-09 deployment beta.',
    observedAt: '2026-02-01T00:00:00.000Z',
  });
  assert.equal(
    store
      .temporalCandidates(
        query,
        { spaceIds: ['events'] },
        [window, { start: '2026-01-09T00:00:00.000Z', end: '2026-01-10T00:00:00.000Z' }],
        100,
      )
      .filter((r) => r.memoryId === both.id).length,
    1,
  );
  assert.equal(
    store
      .temporalCandidates(
        query,
        { spaceIds: ['events'] },
        [{ start: '2026-02-01T00:00:00.000Z', end: '2026-02-02T00:00:00.000Z' }],
        100,
      )
      .filter((r) => r.memoryId === both.id).length,
    0,
    'content dates override observation fallback',
  );
  const grouped = add({
    spaceId: 'events',
    content: { stage: { date: '2026-01-08', detail: 'Mira deployment configuration inherited.' } },
    sources: [{ uri: 'https://host/2026-01-08', label: 'Mira deployment configuration source' }],
  });
  const groupedHits = store
    .temporalCandidates(query, { spaceIds: ['events'] }, [window], 100)
    .filter((r) => r.memoryId === grouped.id);
  assert(groupedHits.some((r) => r.path === '$/stage/detail'));
  assert(groupedHits.every((r) => !r.path.startsWith('sources/')));
  store.deleteSpace('hidden');
  assert.equal(
    store.temporalCandidates(query, {}, [window], 100).some((r) => r.memoryId === hidden.id),
    false,
  );
  const history = add({
    spaceId: 'events',
    logicalKey: 'status/current',
    title: 'Deployment status',
    content: 'Deployment was first-release.',
  });
  const next = store.reviseMemory(
    history.id,
    { title: 'Deployment status', content: 'Deployment is second-release.' },
    history.revision.id,
    'probe',
  );
  index(next);
  store.database
    .prepare('UPDATE memory_revisions SET recorded_at=? WHERE id=?')
    .run('2026-01-01T00:00:00.000Z', history.revision.id);
  store.database
    .prepare('UPDATE memory_revisions SET recorded_at=? WHERE id=?')
    .run('2026-02-01T00:00:00.000Z', next.revision.id);
  store.database
    .prepare('UPDATE memory_state_events SET recorded_at=? WHERE memory_id=?')
    .run('2026-01-01T00:00:00.000Z', history.id);
  const snapshot = await engine.search({
    query: 'What was the latest recorded deployment status as of 8 January 2026?',
    spaceIds: ['events'],
    mode: 'lexical',
  });
  assert(snapshot.results.some((r) => r.memory.revision.id === history.revision.id));
  assert.equal(snapshot.temporal.atTime, '2026-01-08T23:59:59.999Z');
  const overridden = await engine.search({
    query: 'What was the latest recorded deployment status as of 8 January 2026?',
    atTime: '2026-03-01T00:00:00.000Z',
    spaceIds: ['events'],
    mode: 'lexical',
  });
  assert(overridden.results.some((r) => r.memory.revision.id === next.revision.id));
  console.log(
    JSON.stringify({
      status: 'ok',
      dataDir,
      coverage:
        'date parsing, pre-limit recovery, explicit windows, opt-out, ordinary parity, scope/lifecycle/validity/tag/confidence filters, endpoint bounds, anchor inheritance, duplicates, source exclusion and historical routing',
    }),
  );
} finally {
  await models.stop();
  store.close();
  assert.equal(
    path.dirname(dataDir),
    path.resolve(tmpdir()),
    'cleanup must stay inside the system temporary directory',
  );
  assert(path.basename(dataDir).startsWith('simple-memory-temporal-'));
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 250 });
}
