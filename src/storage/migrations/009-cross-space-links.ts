export const crossSpaceLinksSql = `
ALTER TABLE memory_links
  ADD COLUMN from_space_id TEXT REFERENCES spaces(id);

ALTER TABLE memory_links
  ADD COLUMN to_space_id TEXT REFERENCES spaces(id);

UPDATE memory_links
SET from_space_id = (SELECT space_id FROM memories WHERE id = from_memory_id),
    to_space_id = (SELECT space_id FROM memories WHERE id = to_memory_id);

CREATE INDEX idx_links_from_space
  ON memory_links(from_space_id, from_memory_id, deleted_at, created_at, id);

CREATE INDEX idx_links_to_space
  ON memory_links(to_space_id, to_memory_id, deleted_at, created_at, id);

CREATE TRIGGER memory_links_validate_insert
BEFORE INSERT ON memory_links
BEGIN
  SELECT CASE
    WHEN NEW.from_space_id IS NULL OR NEW.to_space_id IS NULL
      THEN RAISE(ABORT, 'memory link endpoint spaces are required')
    WHEN NEW.space_id <> NEW.from_space_id
      THEN RAISE(ABORT, 'memory link owner must be the source space')
    WHEN NEW.from_space_id <> (SELECT space_id FROM memories WHERE id = NEW.from_memory_id)
      THEN RAISE(ABORT, 'memory link source space does not match its memory')
    WHEN NEW.to_space_id <> (SELECT space_id FROM memories WHERE id = NEW.to_memory_id)
      THEN RAISE(ABORT, 'memory link target space does not match its memory')
  END;
END;

CREATE TRIGGER memory_links_validate_update
BEFORE UPDATE OF space_id, from_space_id, to_space_id, from_memory_id, to_memory_id ON memory_links
BEGIN
  SELECT CASE
    WHEN NEW.from_space_id IS NULL OR NEW.to_space_id IS NULL
      THEN RAISE(ABORT, 'memory link endpoint spaces are required')
    WHEN NEW.space_id <> NEW.from_space_id
      THEN RAISE(ABORT, 'memory link owner must be the source space')
    WHEN NEW.from_space_id <> (SELECT space_id FROM memories WHERE id = NEW.from_memory_id)
      THEN RAISE(ABORT, 'memory link source space does not match its memory')
    WHEN NEW.to_space_id <> (SELECT space_id FROM memories WHERE id = NEW.to_memory_id)
      THEN RAISE(ABORT, 'memory link target space does not match its memory')
  END;
END;
`;
