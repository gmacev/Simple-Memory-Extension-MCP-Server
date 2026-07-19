export const schemaSql = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS spaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS memories (
  id TEXT PRIMARY KEY,
  space_id TEXT NOT NULL REFERENCES spaces(id),
  state TEXT NOT NULL CHECK(state IN ('active', 'archived', 'deleted')),
  current_revision_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  index_status TEXT NOT NULL CHECK(index_status IN ('pending', 'ready', 'lexical-only', 'failed')),
  idempotency_key TEXT,
  UNIQUE(space_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS memory_revisions (
  id TEXT PRIMARY KEY,
  memory_id TEXT NOT NULL REFERENCES memories(id),
  revision_number INTEGER NOT NULL,
  parent_revision_id TEXT REFERENCES memory_revisions(id),
  title TEXT,
  kind TEXT,
  content_json TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  salience REAL,
  confidence REAL,
  observed_at TEXT,
  valid_from TEXT,
  valid_to TEXT,
  expires_at TEXT,
  review_after TEXT,
  recorded_at TEXT NOT NULL,
  actor TEXT,
  content_hash TEXT NOT NULL,
  searchable_text TEXT NOT NULL,
  UNIQUE(memory_id, revision_number)
);

CREATE INDEX IF NOT EXISTS idx_revisions_memory ON memory_revisions(memory_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS idx_revisions_recorded ON memory_revisions(recorded_at);

CREATE TABLE IF NOT EXISTS memory_state_events (
  id TEXT PRIMARY KEY,
  memory_id TEXT NOT NULL REFERENCES memories(id),
  event_number INTEGER NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('active', 'archived', 'deleted')),
  recorded_at TEXT NOT NULL,
  UNIQUE(memory_id, event_number)
);
CREATE INDEX IF NOT EXISTS idx_state_events_memory
  ON memory_state_events(memory_id, recorded_at, event_number);

CREATE TABLE IF NOT EXISTS revision_tags (
  revision_id TEXT NOT NULL REFERENCES memory_revisions(id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY(revision_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_revision_tags_tag ON revision_tags(tag);

CREATE TABLE IF NOT EXISTS revision_sources (
  id TEXT PRIMARY KEY,
  revision_id TEXT NOT NULL REFERENCES memory_revisions(id) ON DELETE CASCADE,
  uri TEXT,
  label TEXT,
  type TEXT,
  observed_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS memory_links (
  id TEXT PRIMARY KEY,
  space_id TEXT NOT NULL REFERENCES spaces(id),
  from_memory_id TEXT NOT NULL REFERENCES memories(id),
  to_memory_id TEXT NOT NULL REFERENCES memories(id),
  relation TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  valid_from TEXT,
  valid_to TEXT,
  created_at TEXT NOT NULL,
  deleted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_links_from ON memory_links(from_memory_id, deleted_at);
CREATE INDEX IF NOT EXISTS idx_links_to ON memory_links(to_memory_id, deleted_at);

CREATE TABLE IF NOT EXISTS memory_feedback (
  id TEXT PRIMARY KEY,
  memory_id TEXT NOT NULL REFERENCES memories(id),
  signal TEXT NOT NULL,
  value REAL,
  note TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_profiles (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  model_revision TEXT NOT NULL,
  dimensions INTEGER NOT NULL,
  instruction_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(provider, model, model_revision, dimensions, instruction_hash)
);

CREATE TABLE IF NOT EXISTS memory_segments (
  id TEXT PRIMARY KEY,
  memory_id TEXT NOT NULL REFERENCES memories(id),
  revision_id TEXT NOT NULL REFERENCES memory_revisions(id),
  space_id TEXT NOT NULL REFERENCES spaces(id),
  ordinal INTEGER NOT NULL,
  path TEXT NOT NULL,
  text TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  model_profile_id TEXT REFERENCES model_profiles(id),
  UNIQUE(revision_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_segments_memory ON memory_segments(memory_id, revision_id);

CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
  segment_id UNINDEXED,
  memory_id UNINDEXED,
  revision_id UNINDEXED,
  space_id UNINDEXED,
  title,
  text,
  tags,
  tokenize='unicode61 remove_diacritics 2'
);

CREATE TABLE IF NOT EXISTS index_jobs (
  id TEXT PRIMARY KEY,
  revision_id TEXT NOT NULL REFERENCES memory_revisions(id),
  status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'complete', 'failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_index_jobs_status ON index_jobs(status, created_at);
`;
