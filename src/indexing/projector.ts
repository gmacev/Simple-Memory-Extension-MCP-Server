import { createHash } from 'node:crypto';
import type { JsonObject, JsonValue, SegmentRecord, SourceInput } from '../domain/types.js';

export interface ProjectionInput {
  memoryId: string;
  revisionId: string;
  spaceId: string;
  title: string | null;
  kind: string | null;
  content: JsonValue;
  tags: string[];
  metadata: JsonObject;
  sources: SourceInput[];
}

interface FieldText {
  path: string;
  text: string;
}

function flatten(value: JsonValue, path = '$'): FieldText[] {
  if (value === null) return [{ path, text: 'null' }];
  if (typeof value !== 'object') return [{ path, text: String(value) }];
  if (Array.isArray(value)) {
    return value.flatMap((item, index) => flatten(item, `${path}/${index}`));
  }
  return Object.entries(value).flatMap(([key, item]) =>
    flatten(item, `${path}/${key.replaceAll('~', '~0').replaceAll('/', '~1')}`),
  );
}

function sourceFields(sources: SourceInput[]): FieldText[] {
  return sources.flatMap((source, index) => {
    const base = `sources/${index}`;
    const fields: FieldText[] = [];
    if (source.uri) fields.push({ path: `${base}/uri`, text: source.uri });
    if (source.label) fields.push({ path: `${base}/label`, text: source.label });
    if (source.type) fields.push({ path: `${base}/type`, text: source.type });
    if (source.observedAt) {
      fields.push({ path: `${base}/observedAt`, text: source.observedAt });
    }
    if (source.metadata) {
      fields.push(...flatten(source.metadata, `${base}/metadata`));
    }
    return fields;
  });
}

function sentencePieces(text: string): string[] {
  const paragraphs = text
    .split(/\n{2,}/u)
    .map((part) => part.trim())
    .filter(Boolean);
  const pieces: string[] = [];
  for (const paragraph of paragraphs.length > 0 ? paragraphs : [text]) {
    if (paragraph.length <= 4_000) {
      pieces.push(paragraph);
      continue;
    }
    const sentences = paragraph.match(/[^.!?\n]+(?:[.!?]+|$)/gu) ?? [paragraph];
    pieces.push(...sentences.map((sentence) => sentence.trim()).filter(Boolean));
  }
  return pieces;
}

function chunkField(
  field: FieldText,
  targetCharacters = 3_200,
  maxCharacters = 4_800,
): FieldText[] {
  if (field.text.length <= maxCharacters) return [field];
  const output: FieldText[] = [];
  let current = '';
  for (const piece of sentencePieces(field.text)) {
    if (piece.length > maxCharacters) {
      if (current) {
        output.push({ path: field.path, text: current.trim() });
        current = '';
      }
      for (let offset = 0; offset < piece.length; offset += maxCharacters) {
        output.push({ path: field.path, text: piece.slice(offset, offset + maxCharacters).trim() });
      }
      continue;
    }
    if (current && current.length + piece.length + 1 > targetCharacters) {
      output.push({ path: field.path, text: current.trim() });
      current = piece;
    } else {
      current = current ? `${current} ${piece}` : piece;
    }
  }
  if (current) output.push({ path: field.path, text: current.trim() });
  return output;
}

export function searchableProjection(
  input: Omit<ProjectionInput, 'memoryId' | 'revisionId' | 'spaceId'>,
): string {
  const lines: string[] = [];
  if (input.title) lines.push(`Title: ${input.title}`);
  if (input.kind) lines.push(`Kind: ${input.kind}`);
  if (input.tags.length > 0) lines.push(`Tags: ${input.tags.join(', ')}`);
  for (const field of flatten(input.content)) lines.push(`${field.path}: ${field.text}`);
  for (const field of flatten(input.metadata))
    lines.push(`metadata${field.path.slice(1)}: ${field.text}`);
  for (const field of sourceFields(input.sources)) lines.push(`${field.path}: ${field.text}`);
  return lines.join('\n');
}

export function createSegments(input: ProjectionInput): SegmentRecord[] {
  const header = [
    input.title ? `Title: ${input.title}` : '',
    input.kind ? `Kind: ${input.kind}` : '',
    input.tags.length > 0 ? `Tags: ${input.tags.join(', ')}` : '',
  ]
    .filter(Boolean)
    .join('\n');
  const fields = flatten(input.content).flatMap((field) => chunkField(field));
  const metadata = flatten(input.metadata).flatMap((field) =>
    chunkField({ path: `metadata${field.path.slice(1)}`, text: field.text }),
  );
  const sources = sourceFields(input.sources).flatMap((field) => chunkField(field));
  const all = [...fields, ...metadata, ...sources];
  if (all.length === 0) all.push({ path: '$', text: '' });
  return all.map((field, ordinal) => {
    const text = header
      ? `${header}\n${field.path}: ${field.text}`
      : `${field.path}: ${field.text}`;
    const hash = createHash('sha256').update(text).digest('hex');
    return {
      id: `${input.revisionId}:${ordinal}`,
      memoryId: input.memoryId,
      revisionId: input.revisionId,
      spaceId: input.spaceId,
      ordinal,
      path: field.path,
      text,
      tokenCount: Math.max(1, Math.ceil(text.length / 4)),
      contentHash: hash,
    };
  });
}
