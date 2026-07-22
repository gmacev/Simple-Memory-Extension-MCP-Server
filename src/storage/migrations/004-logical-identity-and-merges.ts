export const logicalIdentityAndMergesSql = `
ALTER TABLE memories ADD COLUMN logical_key TEXT;

CREATE UNIQUE INDEX idx_memories_space_logical_key
  ON memories(space_id, logical_key)
  WHERE logical_key IS NOT NULL;

CREATE TABLE memory_merge_operations (
  id TEXT PRIMARY KEY,
  space_id TEXT NOT NULL REFERENCES spaces(id),
  canonical_memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  canonical_revision_id TEXT NOT NULL REFERENCES memory_revisions(id) ON DELETE CASCADE,
  actor_id TEXT,
  reason TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  idempotency_key TEXT,
  request_hash TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_merge_operation_idempotency
  ON memory_merge_operations(canonical_memory_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_merge_operations_space_created
  ON memory_merge_operations(space_id, created_at DESC, id DESC);

CREATE TABLE memory_merge_members (
  operation_id TEXT NOT NULL REFERENCES memory_merge_operations(id) ON DELETE CASCADE,
  duplicate_memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  duplicate_revision_id TEXT NOT NULL REFERENCES memory_revisions(id) ON DELETE CASCADE,
  PRIMARY KEY(operation_id, duplicate_memory_id)
);
CREATE INDEX idx_merge_members_duplicate
  ON memory_merge_members(duplicate_memory_id, operation_id);

CREATE TABLE memory_redirect_events (
  id TEXT PRIMARY KEY,
  source_memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  canonical_memory_id TEXT NOT NULL REFERENCES memories(id) ON DELETE CASCADE,
  operation_id TEXT NOT NULL REFERENCES memory_merge_operations(id) ON DELETE CASCADE,
  direct INTEGER NOT NULL CHECK(direct IN (0, 1)),
  created_at TEXT NOT NULL,
  CHECK(source_memory_id <> canonical_memory_id)
);
CREATE INDEX idx_redirect_events_source_created
  ON memory_redirect_events(source_memory_id, created_at DESC, id DESC);
CREATE INDEX idx_redirect_events_canonical_created
  ON memory_redirect_events(canonical_memory_id, created_at DESC, id DESC);
`;
