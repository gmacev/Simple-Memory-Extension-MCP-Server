export const temporalEvidenceSql = `
CREATE TABLE memory_segment_times (
  segment_id TEXT NOT NULL REFERENCES memory_segments(id) ON DELETE CASCADE,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  basis TEXT NOT NULL CHECK(basis IN ('content', 'observed')),
  PRIMARY KEY(segment_id, start_at, end_at, basis)
);
CREATE INDEX idx_segment_times_window ON memory_segment_times(start_at, end_at, segment_id);
CREATE TABLE memory_temporal_revisions (
  revision_id TEXT PRIMARY KEY REFERENCES memory_revisions(id) ON DELETE CASCADE
);
`;
