import { discoverAuthorizationServerMetadata } from '@modelcontextprotocol/sdk/client/auth.js';
import { InvalidTokenError } from '@modelcontextprotocol/sdk/server/auth/errors.js';
import type { OAuthTokenVerifier } from '@modelcontextprotocol/sdk/server/auth/provider.js';
import type { AuthInfo } from '@modelcontextprotocol/sdk/server/auth/types.js';
import { OAuthMetadataSchema, type OAuthMetadata } from '@modelcontextprotocol/sdk/shared/auth.js';
import { createRemoteJWKSet, jwtVerify, type JWTPayload } from 'jose';
import * as z from 'zod/v4';
import type { AccessConfiguration, OAuthAccessContext } from './authorization.js';
import {
  globalLevelFromScopes,
  oauthAuthExtra,
  parseAccessClaim,
} from './authorization.js';

export interface OAuthRuntime {
  metadata: OAuthMetadata;
  verifier: OAuthTokenVerifier;
}

const metadataJwksSchema = z.object({
  issuer: z.string().url(),
  jwks_uri: z.string().url(),
});

function stringClaim(payload: JWTPayload, name: string): string | null {
  const value = payload[name];
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

function tokenScopes(payload: JWTPayload): string[] {
  const scope = payload.scope;
  const scp = payload.scp;
  const values = [
    ...(typeof scope === 'string' ? scope.split(/\s+/u) : []),
    ...(Array.isArray(scp) ? scp.filter((value): value is string => typeof value === 'string') : []),
  ];
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function requireOAuthConfiguration(configuration: AccessConfiguration): {
  issuer: URL;
  issuerIdentifier: string;
  audience: string;
  resource: URL;
  accessClaim: string;
} {
  if (
    configuration.mode !== 'oauth' ||
    !configuration.oauthIssuer ||
    !configuration.oauthAudience ||
    !configuration.httpPublicUrl
  ) {
    throw new Error('OAuth access configuration is incomplete');
  }
  return {
    issuer: new URL(configuration.oauthIssuer),
    issuerIdentifier: configuration.oauthIssuer,
    audience: configuration.oauthAudience,
    resource: new URL(configuration.httpPublicUrl),
    accessClaim: configuration.oauthAccessClaim,
  };
}

export async function createOAuthRuntime(
  configuration: AccessConfiguration,
): Promise<OAuthRuntime> {
  const { issuer, issuerIdentifier, audience, resource, accessClaim } =
    requireOAuthConfiguration(configuration);
  const discovered = await discoverAuthorizationServerMetadata(issuer);
  if (!discovered) {
    throw new Error(`OAuth/OIDC metadata was not found for issuer ${issuerIdentifier}`);
  }
  const metadataIdentity = metadataJwksSchema.parse(discovered);
  if (metadataIdentity.issuer !== issuerIdentifier) {
    throw new Error(
      `OAuth issuer mismatch: configured ${issuerIdentifier}, discovered ${metadataIdentity.issuer}`,
    );
  }
  const metadata = OAuthMetadataSchema.parse(discovered);
  const jwks = createRemoteJWKSet(new URL(metadataIdentity.jwks_uri));

  const verifier: OAuthTokenVerifier = {
    verifyAccessToken: async (token: string): Promise<AuthInfo> => {
      try {
        const verified = await jwtVerify(token, jwks, {
          issuer: issuerIdentifier,
          audience,
        });
        const subject = stringClaim(verified.payload, 'sub');
        if (!subject) throw new Error('Token subject is missing');
        if (typeof verified.payload.exp !== 'number') throw new Error('Token expiry is missing');
        const scopes = tokenScopes(verified.payload);
        const recognizedLevel = globalLevelFromScopes(scopes);
        const globalLevel = recognizedLevel ?? 'read';
        const grants = recognizedLevel ? parseAccessClaim(verified.payload[accessClaim]) : {};
        const context: OAuthAccessContext = {
          mode: 'oauth',
          principal: subject,
          globalLevel,
          grants,
        };
        return {
          token,
          clientId:
            stringClaim(verified.payload, 'client_id') ??
            stringClaim(verified.payload, 'azp') ??
            subject,
          scopes,
          expiresAt: verified.payload.exp,
          resource,
          extra: oauthAuthExtra(context),
        };
      } catch (error) {
        if (error instanceof InvalidTokenError) throw error;
        throw new InvalidTokenError('Invalid or expired access token');
      }
    },
  };
  return { metadata, verifier };
}
