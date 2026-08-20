export const currentEmbeddingGenerationSql = `
ALTER TABLE embedding_index_generations
  ADD COLUMN is_current INTEGER NOT NULL DEFAULT 0 CHECK(is_current IN (0, 1));

CREATE UNIQUE INDEX idx_embedding_generations_single_current
  ON embedding_index_generations(is_current)
  WHERE is_current = 1;

UPDATE embedding_index_generations
SET is_current = 1
WHERE id = (
  SELECT id
  FROM embedding_index_generations
  WHERE vectors_reset_at IS NOT NULL
  ORDER BY vectors_reset_at DESC, updated_at DESC, rowid DESC
  LIMIT 1
);
`;
