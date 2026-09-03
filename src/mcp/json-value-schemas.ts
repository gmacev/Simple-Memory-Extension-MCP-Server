import * as z from 'zod/v4';
import type { JsonValue } from '../domain/types.js';

const runtimeJsonValueSchema = z.json();

export const jsonValueInputSchema = z.unknown().transform((value, context): JsonValue => {
  const result = runtimeJsonValueSchema.safeParse(value);
  if (result.success) return result.data;
  for (const issue of result.error.issues) {
    context.addIssue({ code: 'custom', message: issue.message, path: issue.path });
  }
  return z.NEVER;
});

export const jsonValueOutputSchema = z.unknown().superRefine((value, context) => {
  const result = runtimeJsonValueSchema.safeParse(value);
  if (result.success) return;
  for (const issue of result.error.issues) {
    context.addIssue({ code: 'custom', message: issue.message, path: issue.path });
  }
});

export const jsonObjectInputSchema = z.record(z.string(), jsonValueInputSchema);
export const jsonObjectOutputSchema = z.record(z.string(), jsonValueOutputSchema);
