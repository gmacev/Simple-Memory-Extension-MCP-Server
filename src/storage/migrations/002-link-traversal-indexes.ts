export const linkTraversalIndexesSql = `
CREATE INDEX IF NOT EXISTS idx_links_from_relation
  ON memory_links(from_memory_id, relation COLLATE NOCASE, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS idx_links_to_relation
  ON memory_links(to_memory_id, relation COLLATE NOCASE, deleted_at, created_at, id);
`;
