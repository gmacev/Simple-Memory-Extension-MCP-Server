import * as z from 'zod/v4';
import { compileSchema } from '../validation.js';
import { jsonObjectInputSchema, jsonValueInputSchema } from './json-value-schemas.js';

const dateSchema = z.iso
  .datetime({ offset: true })
  .transform((value) => new Date(value).toISOString());
const contentSchema = jsonValueInputSchema.refine(
  (value) => JSON.stringify(value).length <= 1_000_000,
  {
    message: 'Memory content must be at most 1 MB of JSON',
  },
);
const sourceSchema = z.object({
  uri: z.string().max(4_000).optional(),
  label: z.string().max(500).optional(),
  type: z.string().max(100).optional(),
  observedAt: dateSchema.optional(),
  metadata: jsonObjectInputSchema.optional(),
});
const feedbackScopeSchema = z.enum(['content', 'retrieval']);
const storedFeedbackScopeSchema = z.enum(['legacy', 'content', 'retrieval']);
const feedbackSignalSchema = z.enum([
  'verified',
  'correct',
  'incorrect',
  'stale',
  'contradicted',
  'relevant',
  'irrelevant',
  'helpful',
  'not_helpful',
]);
const feedbackActorTypeSchema = z.enum(['user', 'agent', 'system', 'external']);
const feedbackStatusSchema = z.enum(['unreviewed', 'supported', 'verified', 'needs-review']);
const actorIdSchema = z.string().min(1).max(200);
const logicalKeySchema = z.string().min(1).max(500);

const memoryInputSchema = z.object({
  spaceId: z.string().min(1).max(200).optional(),
  title: z.string().max(500).optional(),
  kind: z.string().max(100).optional(),
  content: contentSchema,
  tags: z.array(z.string().min(1).max(100)).max(100).optional(),
  metadata: jsonObjectInputSchema.optional(),
  sources: z.array(sourceSchema).max(100).optional(),
  salience: z.number().min(0).max(1).optional(),
  confidence: z.number().min(0).max(1).optional(),
  observedAt: dateSchema.optional(),
  validFrom: dateSchema.optional(),
  validTo: dateSchema.optional(),
  expiresAt: dateSchema.optional(),
  reviewAfter: dateSchema.optional(),
  idempotencyKey: z.string().min(1).max(500).optional(),
});

export type MemoryInputArguments = z.output<typeof memoryInputSchema>;

export const toolInputSchemas = {
  space_create: z.object({
    id: z.string().min(1).max(200).optional(),
    name: z.string().min(1).max(200),
    description: z.string().max(2_000).optional(),
    metadata: jsonObjectInputSchema.optional(),
  }),
  space_list: compileSchema(
    z.object({
      id: z.string().min(1).max(200).optional(),
      query: z.string().min(1).max(2_000).optional(),
      state: z.enum(['active', 'deleted']).optional(),
      includeMetadata: z.boolean().optional(),
      limit: z.number().int().min(1).max(100).optional(),
      cursor: z.string().max(2_000).optional(),
    }),
  ),
  space_delete: compileSchema(z.object({ spaceId: z.string().min(1).max(200) })),
  space_restore: compileSchema(z.object({ spaceId: z.string().min(1).max(200) })),
  memory_create: memoryInputSchema.extend({
    logicalKey: logicalKeySchema.optional(),
    actorId: actorIdSchema.optional(),
  }),
  memory_revise: memoryInputSchema.extend({
    memoryId: z.string().uuid(),
    expectedRevisionId: z.string().uuid(),
    actorId: actorIdSchema.optional(),
  }),
  memory_merge: z.object({
    canonicalMemoryId: z.string().uuid(),
    expectedCanonicalRevisionId: z.string().uuid(),
    duplicates: z
      .array(
        z.object({
          memoryId: z.string().uuid(),
          expectedRevisionId: z.string().uuid(),
        }),
      )
      .min(1)
      .max(50),
    actorId: actorIdSchema.optional(),
    reason: z.string().max(4_000).optional(),
    metadata: jsonObjectInputSchema.optional(),
    idempotencyKey: z.string().min(1).max(500).optional(),
  }),
  memory_get: compileSchema(
    z.object({
      memoryId: z.string().uuid(),
      revisionId: z.string().uuid().optional(),
      atTime: dateSchema.optional(),
    }),
  ),
  memory_get_by_key: compileSchema(
    z.object({
      spaceId: z.string().min(1).max(200).optional(),
      logicalKey: logicalKeySchema,
      atTime: dateSchema.optional(),
    }),
  ),
  memory_history: compileSchema(
    z.object({
      memoryId: z.string().uuid(),
      includeContent: z.boolean().optional(),
      limit: z.number().int().min(1).max(100).optional(),
      cursor: z.string().max(2_000).optional(),
    }),
  ),
  memory_list: compileSchema(
    z.object({
      spaceId: z.string().max(200).optional(),
      state: z.enum(['active', 'archived']).optional(),
      kind: z.string().max(100).optional(),
      tags: z.array(z.string()).max(100).optional(),
      feedbackStatus: feedbackStatusSchema.optional(),
      limit: z.number().int().min(1).max(200).optional(),
      cursor: z.string().max(2_000).optional(),
    }),
  ),
  memory_search: compileSchema(
    z.object({
      query: z.string().min(1).max(10_000),
      spaceIds: z.array(z.string()).max(100).optional(),
      states: z
        .array(z.enum(['active', 'archived']))
        .min(1)
        .max(3)
        .optional(),
      kinds: z.array(z.string()).max(100).optional(),
      tags: z.array(z.string()).max(100).optional(),
      minConfidence: z.number().min(0).max(1).optional(),
      minSalience: z.number().min(0).max(1).optional(),
      topK: z.number().int().min(1).max(50).optional(),
      mode: z.enum(['auto', 'fast', 'quality', 'lexical', 'semantic']).optional(),
      atTime: dateSchema.optional(),
      validAt: dateSchema.optional(),
      expandRelations: z.boolean().optional(),
      explain: z.boolean().optional(),
      includeSourceMetadata: z.boolean().optional(),
    }),
  ),
  memory_archive: compileSchema(z.object({ memoryId: z.string().uuid() })),
  memory_restore: compileSchema(z.object({ memoryId: z.string().uuid() })),
  memory_delete: compileSchema(z.object({ memoryId: z.string().uuid() })),
  memory_link: z.object({
    fromMemoryId: z.string().uuid(),
    toMemoryId: z.string().uuid(),
    relation: z.string().min(1).max(200),
    metadata: jsonObjectInputSchema.optional(),
    validFrom: dateSchema.optional(),
    validTo: dateSchema.optional(),
  }),
  memory_unlink: compileSchema(z.object({ linkId: z.string().uuid() })),
  memory_traverse: compileSchema(
    z.object({
      memoryId: z.string().uuid(),
      maxDepth: z.number().int().min(0).max(5).optional(),
      atTime: dateSchema.optional(),
      relations: z.array(z.string().min(1).max(200)).max(50).optional(),
      direction: z.enum(['outgoing', 'incoming', 'both']).optional(),
      query: z.string().min(1).max(10_000).optional(),
      explain: z.boolean().optional(),
      limit: z.number().int().min(1).max(200).optional(),
      cursor: z.string().max(2_000).optional(),
    }),
  ),
  memory_feedback: z.object({
    memoryId: z.string().uuid(),
    revisionId: z.string().uuid().optional(),
    scope: feedbackScopeSchema,
    signal: feedbackSignalSchema,
    actorType: feedbackActorTypeSchema,
    actorId: z.string().min(1).max(200).optional(),
    query: z.string().min(1).max(10_000).optional(),
    note: z.string().max(4_000).optional(),
    metadata: jsonObjectInputSchema.optional(),
    idempotencyKey: z.string().min(1).max(500).optional(),
  }),
  memory_feedback_list: compileSchema(
    z.object({
      memoryId: z.string().uuid(),
      revisionId: z.string().uuid().optional(),
      scope: storedFeedbackScopeSchema.optional(),
      atTime: dateSchema.optional(),
      limit: z.number().int().min(1).max(100).optional(),
      cursor: z.string().max(2_000).optional(),
      includeDetails: z.boolean().optional(),
    }),
  ),
  memory_status: compileSchema(
    z.object({
      probeModels: z.boolean().optional(),
      includeDetails: z.boolean().optional(),
    }),
  ),
} as const;
