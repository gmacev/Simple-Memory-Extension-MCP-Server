export const revisionAwareFeedbackSql = `
ALTER TABLE memory_feedback
  ADD COLUMN revision_id TEXT REFERENCES memory_revisions(id) ON DELETE CASCADE;
ALTER TABLE memory_feedback
  ADD COLUMN scope TEXT NOT NULL DEFAULT 'legacy'
  CHECK(scope IN ('legacy', 'content', 'retrieval'));
ALTER TABLE memory_feedback
  ADD COLUMN actor_type TEXT
  CHECK(actor_type IN ('user', 'agent', 'system', 'external'));
ALTER TABLE memory_feedback ADD COLUMN actor_id TEXT;
ALTER TABLE memory_feedback ADD COLUMN query TEXT;
ALTER TABLE memory_feedback ADD COLUMN idempotency_key TEXT;
ALTER TABLE memory_feedback ADD COLUMN request_hash TEXT;

CREATE UNIQUE INDEX idx_feedback_memory_idempotency
  ON memory_feedback(memory_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_feedback_revision_scope_created
  ON memory_feedback(revision_id, scope, created_at DESC, id DESC);
`;
