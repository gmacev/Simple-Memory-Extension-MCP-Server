export const spaceLifecycleAndDiscoverySql = `
ALTER TABLE spaces ADD COLUMN deleted_at TEXT;

CREATE INDEX idx_spaces_state_name
  ON spaces(deleted_at, name COLLATE NOCASE, id);
`;
