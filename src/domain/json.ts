import { createHash } from 'node:crypto';
import * as z from 'zod/v4';
import type { JsonValue } from './types.js';

export function stableStringify(value: JsonValue): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${stableStringify(item)}`).join(',')}}`;
}

export function contentHash(value: JsonValue): string {
  return createHash('sha256').update(stableStringify(value)).digest('hex');
}

export function parseJsonValue(value: string): JsonValue {
  return z.json().parse(JSON.parse(value));
}
