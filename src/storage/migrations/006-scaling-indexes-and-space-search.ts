export const scalingIndexesAndSpaceSearchSql = `
CREATE INDEX idx_feedback_memory_created
  ON memory_feedback(memory_id, created_at DESC, id DESC);

CREATE INDEX idx_revisions_title_nocase
  ON memory_revisions(title COLLATE NOCASE, recorded_at DESC, id);

CREATE INDEX idx_spaces_state_id_nocase
  ON spaces(deleted_at, id COLLATE NOCASE);

CREATE VIRTUAL TABLE space_fts USING fts5(
  space_id UNINDEXED,
  name,
  description,
  tokenize='unicode61 remove_diacritics 2'
);

INSERT INTO space_fts(space_id, name, description)
SELECT id, name, COALESCE(description, '') FROM spaces;

CREATE TRIGGER spaces_fts_insert AFTER INSERT ON spaces BEGIN
  INSERT INTO space_fts(space_id, name, description)
  VALUES (NEW.id, NEW.name, COALESCE(NEW.description, ''));
END;

CREATE TRIGGER spaces_fts_update AFTER UPDATE OF name, description ON spaces BEGIN
  DELETE FROM space_fts WHERE space_id = OLD.id;
  INSERT INTO space_fts(space_id, name, description)
  VALUES (NEW.id, NEW.name, COALESCE(NEW.description, ''));
END;

CREATE TRIGGER spaces_fts_delete AFTER DELETE ON spaces BEGIN
  DELETE FROM space_fts WHERE space_id = OLD.id;
END;
`;
