export const embeddingIndexGenerationsSql = `
CREATE TABLE embedding_index_generations (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  model_revision TEXT NOT NULL,
  dimensions INTEGER NOT NULL,
  instruction_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'complete', 'failed')),
  revision_cutoff TEXT NOT NULL,
  vectors_reset_at TEXT,
  completed_at TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

ALTER TABLE index_jobs
  ADD COLUMN embedding_generation_id TEXT REFERENCES embedding_index_generations(id);

CREATE UNIQUE INDEX idx_index_jobs_revision_generation
  ON index_jobs(revision_id, embedding_generation_id)
  WHERE embedding_generation_id IS NOT NULL;

CREATE INDEX idx_index_jobs_generation_status
  ON index_jobs(embedding_generation_id, status, created_at, id);
`;
