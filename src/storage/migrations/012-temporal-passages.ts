export const temporalPassagesSql = `
DROP TABLE memory_segment_times;
CREATE TABLE memory_segment_times (
  segment_id TEXT NOT NULL REFERENCES memory_segments(id) ON DELETE CASCADE,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  basis TEXT NOT NULL CHECK(basis IN ('content', 'observed')),
  passage_start INTEGER NOT NULL CHECK(passage_start >= 0),
  passage_end INTEGER NOT NULL CHECK(passage_end >= passage_start),
  PRIMARY KEY(segment_id, start_at, end_at, basis, passage_start, passage_end)
);
CREATE INDEX idx_segment_times_window ON memory_segment_times(start_at, end_at, segment_id);
DELETE FROM memory_temporal_revisions;
`;
