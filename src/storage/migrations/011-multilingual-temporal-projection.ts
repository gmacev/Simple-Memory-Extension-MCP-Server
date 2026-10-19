// Rebuild derived anchors lazily using localized date formats. Preserve all source and vector data.
export const multilingualTemporalProjectionSql = `
DELETE FROM memory_segment_times;
DELETE FROM memory_temporal_revisions;
`;
