export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export type MemoryState = 'active' | 'archived' | 'deleted';
export type IndexStatus = 'pending' | 'ready' | 'lexical-only' | 'failed';

export interface SourceInput {
  uri?: string;
  label?: string;
  type?: string;
  observedAt?: string;
  metadata?: JsonObject;
}

export interface MemoryInput {
  spaceId?: string;
  title?: string;
  kind?: string;
  content: JsonValue;
  tags?: string[];
  metadata?: JsonObject;
  sources?: SourceInput[];
  salience?: number;
  confidence?: number;
  observedAt?: string;
  validFrom?: string;
  validTo?: string;
  expiresAt?: string;
  reviewAfter?: string;
  idempotencyKey?: string;
}

export interface MemoryRevision {
  id: string;
  memoryId: string;
  revisionNumber: number;
  parentRevisionId: string | null;
  title: string | null;
  kind: string | null;
  content: JsonValue;
  tags: string[];
  metadata: JsonObject;
  sources: SourceInput[];
  salience: number | null;
  confidence: number | null;
  observedAt: string | null;
  validFrom: string | null;
  validTo: string | null;
  expiresAt: string | null;
  reviewAfter: string | null;
  recordedAt: string;
  actor: string | null;
  contentHash: string;
  searchableText: string;
}

export interface MemoryRecord {
  id: string;
  spaceId: string;
  state: MemoryState;
  createdAt: string;
  updatedAt: string;
  currentRevisionId: string;
  indexStatus: IndexStatus;
  revision: MemoryRevision;
}

export interface MemoryListFilters {
  spaceId?: string;
  state?: MemoryState;
  kind?: string;
  tags?: string[];
  limit?: number;
  cursor?: string;
}

export interface MemoryListPage {
  items: MemoryRecord[];
  nextCursor: string | null;
}

export interface SearchOptions {
  query: string;
  spaceIds?: string[];
  states?: MemoryState[];
  kinds?: string[];
  tags?: string[];
  minConfidence?: number;
  minSalience?: number;
  topK?: number;
  mode?: 'auto' | 'fast' | 'quality' | 'lexical' | 'semantic';
  atTime?: string;
  validAt?: string;
  expandRelations?: boolean;
}

export interface SearchScoreExplanation {
  lexicalRank?: number;
  semanticRank?: number;
  relationBoost?: number;
  exactBoost?: number;
  rerankerScore?: number;
  fusedScore: number;
}

export interface SearchResult {
  memory: MemoryRecord;
  excerpt: string;
  segmentPath: string;
  score: SearchScoreExplanation;
}

export interface SearchResponse {
  query: string;
  mode: string;
  degraded: boolean;
  degradationReason?: string;
  results: SearchResult[];
  timingMs: number;
}

export interface MemoryLink {
  id: string;
  spaceId: string;
  fromMemoryId: string;
  toMemoryId: string;
  relation: string;
  metadata: JsonObject;
  validFrom: string | null;
  validTo: string | null;
  createdAt: string;
  deletedAt: string | null;
}

export type MemoryLinkDirection = 'outgoing' | 'incoming' | 'both';

export interface MemoryTraversalOptions {
  memoryId: string;
  maxDepth?: number;
  atTime?: string;
  relations?: string[];
  direction?: MemoryLinkDirection;
  query?: string;
  limit?: number;
  cursor?: string;
}

export interface MemoryTraversalPathStep {
  link: MemoryLink;
  direction: Exclude<MemoryLinkDirection, 'both'>;
}

export interface MemoryTraversalEntry {
  memory: MemoryRecord;
  depth: number;
  via: MemoryLink | null;
  path: MemoryTraversalPathStep[];
  relevanceScore?: number;
  rerankerScore?: number;
}

export interface MemoryTraversalPage {
  items: MemoryTraversalEntry[];
  nextCursor: string | null;
  truncated: boolean;
  atTime: string;
  query?: string;
  degraded: boolean;
  degradationReason?: string;
}

export interface SegmentRecord {
  id: string;
  memoryId: string;
  revisionId: string;
  spaceId: string;
  ordinal: number;
  path: string;
  text: string;
  tokenCount: number;
  contentHash: string;
}
