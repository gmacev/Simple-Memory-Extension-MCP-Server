import type { AuthInfo } from '@modelcontextprotocol/sdk/server/auth/types.js';
import * as z from 'zod/v4';

export const accessModes = ['open', 'fixed', 'oauth'] as const;
export const spaceAccessLevels = ['read', 'write', 'manage'] as const;
export const memoryScopes = ['memory:read', 'memory:write', 'memory:manage'] as const;

export type AccessMode = (typeof accessModes)[number];
export type SpaceAccessLevel = (typeof spaceAccessLevels)[number];
export type MemoryScope = (typeof memoryScopes)[number];

export interface AccessConfiguration {
  mode: AccessMode;
  fixedPrincipal?: string;
  fixedGrants?: Record<string, SpaceAccessLevel>;
  oauthIssuer?: string;
  oauthAudience?: string;
  oauthAccessClaim: string;
  httpPublicUrl?: string;
  allowUnauthenticatedNonLoopback: boolean;
}

export interface AccessContext {
  mode: AccessMode;
  principal: string | null;
  globalLevel: SpaceAccessLevel;
  grants: Readonly<Record<string, SpaceAccessLevel>>;
}

export interface OAuthAccessContext extends AccessContext {
  mode: 'oauth';
  principal: string;
}

const accessClaimSchema = z.object({
  spaces: z.record(z.string().min(1).max(200), z.enum(spaceAccessLevels)),
});

const authenticatedContextSchema = z.object({
  mode: z.literal('oauth'),
  principal: z.string().min(1),
  globalLevel: z.enum(spaceAccessLevels),
  grants: z.record(z.string(), z.enum(spaceAccessLevels)),
});

const levelRank: Record<SpaceAccessLevel, number> = {
  read: 1,
  write: 2,
  manage: 3,
};

const authContextKey = 'simpleMemoryAccessContext';

export class MemoryAccessError extends Error {
  public constructor(
    public readonly code: 'access-denied' | 'not-found-or-inaccessible',
    message?: string,
  ) {
    super(message ? `${code}: ${message}` : code);
    this.name = 'MemoryAccessError';
  }
}

export function parseAccessClaim(value: unknown): Record<string, SpaceAccessLevel> {
  return accessClaimSchema.parse(value).spaces;
}

export function parseFixedAccess(value: string): Record<string, SpaceAccessLevel> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error('SIMPLE_MEMORY_FIXED_ACCESS must be valid JSON');
  }
  return parseAccessClaim(parsed);
}

export function globalLevelFromScopes(scopes: readonly string[]): SpaceAccessLevel | null {
  if (scopes.includes('memory:manage')) return 'manage';
  if (scopes.includes('memory:write')) return 'write';
  if (scopes.includes('memory:read')) return 'read';
  return null;
}

export function oauthAuthExtra(context: OAuthAccessContext): Record<string, unknown> {
  return {
    [authContextKey]: {
      mode: 'oauth',
      principal: context.principal,
      globalLevel: context.globalLevel,
      grants: context.grants,
    },
  };
}

export class AuthorizationService {
  private readonly fixedContext: AccessContext | null;

  public constructor(private readonly configuration: AccessConfiguration) {
    this.fixedContext =
      configuration.mode === 'fixed'
        ? {
            mode: 'fixed',
            principal: configuration.fixedPrincipal ?? null,
            globalLevel: 'manage',
            grants: configuration.fixedGrants ?? {},
          }
        : null;
  }

  public get mode(): AccessMode {
    return this.configuration.mode;
  }

  public get protected(): boolean {
    return this.configuration.mode !== 'open';
  }

  public context(authInfo?: AuthInfo): AccessContext {
    if (this.configuration.mode === 'open') {
      return { mode: 'open', principal: null, globalLevel: 'manage', grants: { '*': 'manage' } };
    }
    if (this.configuration.mode === 'fixed') {
      if (!this.fixedContext?.principal) {
        throw new Error('Fixed access mode is missing its configured principal');
      }
      return this.fixedContext;
    }
    const stored = authenticatedContextSchema.safeParse(authInfo?.extra?.[authContextKey]);
    if (!stored.success) {
      throw new MemoryAccessError('access-denied', 'authenticated access context is missing');
    }
    return stored.data;
  }

  public actor(context: AccessContext, requestedActorId?: string): string | undefined {
    if (context.mode === 'open') return requestedActorId;
    return context.principal ?? undefined;
  }

  public effectiveLevel(context: AccessContext, spaceId: string): SpaceAccessLevel | null {
    if (context.mode === 'open') return 'manage';
    const grant = context.grants[spaceId] ?? context.grants['*'];
    if (!grant) return null;
    return levelRank[grant] <= levelRank[context.globalLevel] ? grant : context.globalLevel;
  }

  public can(context: AccessContext, spaceId: string, required: SpaceAccessLevel): boolean {
    const effective = this.effectiveLevel(context, spaceId);
    return effective !== null && levelRank[effective] >= levelRank[required];
  }

  public requireSpace(
    context: AccessContext,
    spaceId: string,
    required: SpaceAccessLevel,
    hideExistence = false,
  ): void {
    if (this.can(context, spaceId, required)) return;
    if (hideExistence && !this.can(context, spaceId, 'read')) {
      throw new MemoryAccessError('not-found-or-inaccessible');
    }
    throw new MemoryAccessError('access-denied', `${required} access is required for space ${spaceId}`);
  }

  public spaceIds(context: AccessContext, required: SpaceAccessLevel): string[] | undefined {
    if (context.mode === 'open' || this.can(context, '*', required)) return undefined;
    return Object.entries(context.grants)
      .filter(([spaceId]) => spaceId !== '*')
      .filter(([spaceId]) => this.can(context, spaceId, required))
      .map(([spaceId]) => spaceId)
      .sort();
  }

  public hasWildcardManage(context: AccessContext): boolean {
    return this.can(context, '*', 'manage');
  }
}
