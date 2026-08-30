import * as z from 'zod/v4';
import { compileSchema } from '../validation.js';

const uuidSchema = z.string().uuid();
const isoDateTimeSchema = z.iso.datetime({ offset: true });
const cursorSchema = z.string();
const jsonObjectSchema = z.record(z.string(), z.json());
const memoryStateSchema = z.enum(['active', 'archived']);
const indexStatusSchema = z.enum(['pending', 'ready', 'lexical-only', 'failed']);
const feedbackStatusSchema = z.enum(['unreviewed', 'supported', 'verified', 'needs-review']);
const feedbackActorTypeSchema = z.enum(['user', 'agent', 'system', 'external']);
const contentFeedbackSignalSchema = z.enum([
  'verified',
  'correct',
  'incorrect',
  'stale',
  'contradicted',
]);

const sourceOutputSchema = z
  .object({
    uri: z.string().optional(),
    label: z.string().optional(),
    type: z.string().optional(),
    observedAt: isoDateTimeSchema.optional(),
    metadata: jsonObjectSchema.optional(),
  })
  .strict();

const revisionBaseShape = {
  id: uuidSchema,
  revisionNumber: z.number().int().positive(),
  recordedAt: isoDateTimeSchema,
  parentRevisionId: uuidSchema.optional(),
  title: z.string().optional(),
  kind: z.string().optional(),
  tags: z.array(z.string()).optional(),
  salience: z.number().min(0).max(1).optional(),
  confidence: z.number().min(0).max(1).optional(),
  observedAt: isoDateTimeSchema.optional(),
  validFrom: isoDateTimeSchema.optional(),
  validTo: isoDateTimeSchema.optional(),
  expiresAt: isoDateTimeSchema.optional(),
  reviewAfter: isoDateTimeSchema.optional(),
  reviewDue: z.literal(true).optional(),
  actor: z.string().optional(),
  sources: z.array(sourceOutputSchema).optional(),
};

const historyRevisionOutputSchema = z
  .object({
    ...revisionBaseShape,
    content: z.json().optional(),
    metadata: jsonObjectSchema.optional(),
  })
  .strict();

const completeRevisionOutputSchema = z
  .object({
    ...revisionBaseShape,
    content: z.json(),
    metadata: jsonObjectSchema.optional(),
  })
  .strict();

const compactFeedbackSummaryOutputSchema = z
  .object({
    feedbackStatus: feedbackStatusSchema,
    contentEventCount: z.number().int().nonnegative().optional(),
    retrievalEventCount: z.number().int().nonnegative().optional(),
    latestSignal: contentFeedbackSignalSchema.optional(),
    latestActorType: feedbackActorTypeSchema.optional(),
    latestAt: isoDateTimeSchema.optional(),
  })
  .strict();

const completeMemoryOutputSchema = z
  .object({
    id: uuidSchema,
    spaceId: z.string(),
    state: memoryStateSchema,
    createdAt: isoDateTimeSchema,
    updatedAt: isoDateTimeSchema,
    currentRevisionId: uuidSchema.optional(),
    indexStatus: indexStatusSchema.optional(),
    logicalKey: z.string().optional(),
    canonicalMemoryId: uuidSchema.optional(),
    mergedMemoryCount: z.number().int().positive().optional(),
    revision: completeRevisionOutputSchema,
    feedbackSummary: compactFeedbackSummaryOutputSchema,
  })
  .strict();

const memorySummaryBaseShape = {
  id: uuidSchema,
  spaceId: z.string(),
  revisionId: uuidSchema,
  updatedAt: isoDateTimeSchema,
  indexStatus: indexStatusSchema.optional(),
  title: z.string().optional(),
  logicalKey: z.string().optional(),
  canonicalMemoryId: uuidSchema.optional(),
  mergedMemoryCount: z.number().int().positive().optional(),
  kind: z.string().optional(),
  tags: z.array(z.string()).optional(),
  salience: z.number().min(0).max(1).optional(),
  confidence: z.number().min(0).max(1).optional(),
  validFrom: isoDateTimeSchema.optional(),
  validTo: isoDateTimeSchema.optional(),
  expiresAt: isoDateTimeSchema.optional(),
  reviewAfter: isoDateTimeSchema.optional(),
  reviewDue: z.literal(true).optional(),
  feedbackStatus: feedbackStatusSchema.optional(),
};

const memorySummaryOutputSchema = z.object(memorySummaryBaseShape).strict();
const traversalMemorySummaryOutputSchema = z
  .object({ ...memorySummaryBaseShape, state: memoryStateSchema })
  .strict();

const mutationAcknowledgementOutputSchema = compileSchema(
  z
    .object({
      id: uuidSchema,
      spaceId: z.string(),
      state: memoryStateSchema,
      revisionId: uuidSchema,
      indexStatus: indexStatusSchema,
      recordedAt: isoDateTimeSchema,
      logicalKey: z.string().optional(),
    })
    .strict(),
);

const lifecycleAcknowledgementOutputSchema = compileSchema(
  z
    .object({
      id: uuidSchema,
      state: memoryStateSchema,
      updatedAt: isoDateTimeSchema,
    })
    .strict(),
);

const deletionAcknowledgementOutputSchema = compileSchema(
  z.object({ id: uuidSchema, deleted: z.literal(true) }).strict(),
);

const feedbackAcknowledgementOutputSchema = compileSchema(
  z
    .object({
      id: uuidSchema,
      memoryId: uuidSchema,
      revisionId: uuidSchema.optional(),
      createdAt: isoDateTimeSchema,
    })
    .strict(),
);

const searchScoreOutputSchema = z
  .object({
    lexicalRank: z.number().int().positive().optional(),
    semanticRank: z.number().int().positive().optional(),
    relationBoost: z.number().optional(),
    exactBoost: z.number().optional(),
    rerankerScore: z.number().optional(),
    fusedScore: z.number(),
  })
  .strict();

const searchResultOutputSchema = z
  .object({
    id: uuidSchema,
    revisionId: uuidSchema,
    currentRevisionId: uuidSchema.optional(),
    spaceId: z.string(),
    state: memoryStateSchema,
    excerpt: z.string(),
    recordedAt: isoDateTimeSchema,
    logicalKey: z.string().optional(),
    canonicalMemoryId: uuidSchema.optional(),
    mergedMemoryCount: z.number().int().positive().optional(),
    title: z.string().optional(),
    kind: z.string().optional(),
    tags: z.array(z.string()).optional(),
    segmentPath: z.string().optional(),
    score: searchScoreOutputSchema.optional(),
    salience: z.number().min(0).max(1).optional(),
    confidence: z.number().min(0).max(1).optional(),
    observedAt: isoDateTimeSchema.optional(),
    validFrom: isoDateTimeSchema.optional(),
    validTo: isoDateTimeSchema.optional(),
    reviewAfter: isoDateTimeSchema.optional(),
    reviewDue: z.literal(true).optional(),
    feedbackStatus: feedbackStatusSchema.optional(),
    sources: z.array(sourceOutputSchema).optional(),
  })
  .strict();

const traversalPathStepOutputSchema = z
  .object({
    linkId: uuidSchema,
    relation: z.string(),
    direction: z.enum(['outgoing', 'incoming']),
    fromMemoryId: uuidSchema,
    toMemoryId: uuidSchema,
    validFrom: isoDateTimeSchema.optional(),
    validTo: isoDateTimeSchema.optional(),
    metadata: jsonObjectSchema.optional(),
  })
  .strict();

const feedbackListItemOutputSchema = z
  .object({
    id: uuidSchema,
    revisionId: uuidSchema.optional(),
    scope: z.enum(['legacy', 'content', 'retrieval']),
    signal: z.string(),
    actorType: feedbackActorTypeSchema.optional(),
    actorId: z.string().optional(),
    createdAt: isoDateTimeSchema,
    query: z.string().optional(),
    value: z.number().optional(),
    note: z.string().optional(),
    metadata: jsonObjectSchema.optional(),
  })
  .strict();

const memoryCountsOutputSchema = z
  .object({
    total: z.number().int().nonnegative(),
    active: z.number().int().nonnegative(),
    archived: z.number().int().nonnegative(),
    deleted: z.number().int().nonnegative().optional(),
    indexed: z.number().int().nonnegative(),
    lexical_only: z.number().int().nonnegative(),
  })
  .strict();

const mergeStatusOutputSchema = z
  .object({
    operations: z.number().int().nonnegative(),
    currentRedirects: z.number().int().nonnegative(),
    redirectEvents: z.number().int().nonnegative(),
  })
  .strict();

const modelHealthOutputSchema = z
  .object({
    status: z.string(),
    pid: z.number().int(),
    embedding_model: z.string(),
    embedding_revision: z.string(),
    reranker_model: z.string(),
    reranker_revision: z.string(),
    query_instruction_hash: z.string(),
    rerank_instruction_hash: z.string(),
    device: z.string(),
    device_name: z.string(),
    torch_version: z.string(),
    torch_cuda_version: z.string().nullable(),
    embedding_dimension: z.number().int().nullable(),
    embedding_loaded: z.boolean(),
    reranker_loaded: z.boolean(),
  })
  .strict();

const inferenceLaneStatusOutputSchema = z
  .object({
    queued: z.number().int().nonnegative(),
    completed: z.number().int().nonnegative(),
    meanQueueMs: z.number().nonnegative(),
    meanExecutionMs: z.number().nonnegative(),
  })
  .strict();

const inferenceSchedulerOutputSchema = z
  .object({
    queueDepth: z.number().int().nonnegative(),
    queueLimit: z.number().int().positive(),
    inFlight: z.boolean(),
    counters: z
      .object({
        completed: z.number().int().nonnegative(),
        rejected: z.number().int().nonnegative(),
        queueTimeouts: z.number().int().nonnegative(),
        executionTimeouts: z.number().int().nonnegative(),
        dispatchedBatches: z.number().int().nonnegative(),
      })
      .strict(),
    lanes: z
      .object({
        query: inferenceLaneStatusOutputSchema,
        ingestion: inferenceLaneStatusOutputSchema,
        rerank: inferenceLaneStatusOutputSchema,
      })
      .strict(),
  })
  .strict();

export const toolOutputSchemas = {
  space_create: compileSchema(z.object({ id: z.string(), createdAt: isoDateTimeSchema }).strict()),
  space_list: z
    .object({
      items: z.array(
        z
          .object({
            id: z.string(),
            name: z.string(),
            description: z.string().optional(),
            metadata: jsonObjectSchema.optional(),
            deletedAt: isoDateTimeSchema.optional(),
          })
          .strict(),
      ),
      nextCursor: cursorSchema.optional(),
    })
    .strict(),
  space_delete: compileSchema(
    z
      .object({
        id: z.string(),
        deleted: z.literal(true),
        deletedAt: isoDateTimeSchema.nullable(),
      })
      .strict(),
  ),
  space_restore: compileSchema(z.object({ id: z.string(), restored: z.literal(true) }).strict()),
  memory_create: mutationAcknowledgementOutputSchema,
  memory_revise: mutationAcknowledgementOutputSchema,
  memory_merge: compileSchema(
    z
      .object({
        operationId: uuidSchema,
        canonicalMemoryId: uuidSchema,
        canonicalRevisionId: uuidSchema,
        mergedMemoryIds: z.array(uuidSchema),
        createdAt: isoDateTimeSchema,
        redirectedMemoryCount: z.number().int().nonnegative().optional(),
      })
      .strict(),
  ),
  memory_get: completeMemoryOutputSchema,
  memory_get_by_key: z
    .object({
      redirectedFromMemoryId: uuidSchema.optional(),
      memory: completeMemoryOutputSchema,
    })
    .strict(),
  memory_history: z
    .object({
      revisions: z.array(historyRevisionOutputSchema),
      nextCursor: cursorSchema.optional(),
    })
    .strict(),
  memory_list: compileSchema(
    z
      .object({ items: z.array(memorySummaryOutputSchema), nextCursor: cursorSchema.optional() })
      .strict(),
  ),
  memory_search: z
    .object({
      results: z.array(searchResultOutputSchema),
      degraded: z.literal(true).optional(),
      degradationReason: z.string().optional(),
      mode: z.enum(['auto', 'fast', 'quality', 'lexical', 'semantic']).optional(),
      timingMs: z.number().nonnegative().optional(),
      stageTimings: z
        .object({
          embedMs: z.number().nonnegative().optional(),
          exactMs: z.number().nonnegative().optional(),
          lexicalMs: z.number().nonnegative().optional(),
          semanticMs: z.number().nonnegative().optional(),
          rerankMs: z.number().nonnegative().optional(),
        })
        .strict()
        .optional(),
    })
    .strict(),
  memory_archive: lifecycleAcknowledgementOutputSchema,
  memory_restore: lifecycleAcknowledgementOutputSchema,
  memory_delete: deletionAcknowledgementOutputSchema,
  memory_link: compileSchema(z.object({ id: uuidSchema, createdAt: isoDateTimeSchema }).strict()),
  memory_unlink: compileSchema(
    z
      .object({ id: uuidSchema, deleted: z.literal(true), deletedAt: isoDateTimeSchema.optional() })
      .strict(),
  ),
  memory_traverse: z
    .object({
      items: z.array(
        z
          .object({
            memory: traversalMemorySummaryOutputSchema,
            depth: z.number().int().nonnegative(),
            path: z.array(traversalPathStepOutputSchema),
            relevanceScore: z.number().optional(),
            rerankerScore: z.number().optional(),
          })
          .strict(),
      ),
      nextCursor: cursorSchema.optional(),
      truncated: z.literal(true).optional(),
      atTime: isoDateTimeSchema,
      degraded: z.literal(true).optional(),
      degradationReason: z.string().optional(),
    })
    .strict(),
  memory_feedback: feedbackAcknowledgementOutputSchema,
  memory_feedback_list: z
    .object({ items: z.array(feedbackListItemOutputSchema), nextCursor: cursorSchema.optional() })
    .strict(),
  memory_status: compileSchema(
    z
      .object({
        database: z.string().optional(),
        schemaVersion: z.number().int().nonnegative(),
        vectorAvailable: z.boolean(),
        memories: memoryCountsOutputSchema,
        spaces: z.number().int().nonnegative().optional(),
        revisions: z.number().int().nonnegative().optional(),
        segments: z.number().int().nonnegative().optional(),
        stateEvents: z.number().int().nonnegative().optional(),
        modelProfiles: z.number().int().nonnegative().optional(),
        pendingJobs: z.number().int().nonnegative(),
        logicalKeys: z.number().int().nonnegative().optional(),
        merges: mergeStatusOutputSchema.optional(),
        modelsEnabled: z.boolean(),
        modelLauncherPid: z.number().int().nullable().optional(),
        modelWorkerPid: z.number().int().nullable().optional(),
        modelWorkerStarts: z.number().int().nonnegative().optional(),
        inferenceScheduler: inferenceSchedulerOutputSchema.optional(),
        modelHealth: modelHealthOutputSchema.optional(),
        modelError: z.string().optional(),
      })
      .strict(),
  ),
} as const;
