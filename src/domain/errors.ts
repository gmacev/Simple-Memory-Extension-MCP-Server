import type { MemoryState } from './types.js';

export interface MemoryIdentityConflictDetails {
  spaceId: string;
  logicalKey: string;
  matchedMemoryId: string;
  canonicalMemoryId: string;
  currentRevisionId: string;
  state: MemoryState;
}

export class MemoryIdentityConflictError extends Error {
  public constructor(public readonly details: MemoryIdentityConflictDetails) {
    super(`Logical memory already exists: ${details.logicalKey}`);
    this.name = 'MemoryIdentityConflictError';
  }
}
