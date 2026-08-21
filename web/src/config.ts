/**
 * Runtime configuration.
 *
 * The app is deployed as static assets alongside a `/config.json` file that is
 * populated per-environment at deploy time. Configuration is therefore fetched
 * at runtime (not baked into the bundle) so the same build artifact can be
 * promoted across environments. `public/config.json` is a non-secret
 * development placeholder; it is overwritten during deployment.
 */

export interface RuntimeConfig {
  /** OIDC issuer / Cognito user pool authority URL. */
  readonly authority: string;
  /** Public OIDC client id (not a secret). */
  readonly clientId: string;
  /** Hosted-UI redirect URI registered with the identity provider. */
  readonly redirectUri: string;
  /** Post-logout redirect URI registered with the identity provider. */
  readonly postLogoutRedirectUri: string;
  /** Base path for same-origin API calls, e.g. "/api". */
  readonly apiBasePath: string;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function assertValidConfig(value: unknown): asserts value is RuntimeConfig {
  if (typeof value !== 'object' || value === null) {
    throw new Error('config.json must be a JSON object');
  }
  const record = value as Record<string, unknown>;
  const required: readonly (keyof RuntimeConfig)[] = [
    'authority',
    'clientId',
    'redirectUri',
    'postLogoutRedirectUri',
    'apiBasePath',
  ];
  const missing = required.filter((key) => !isNonEmptyString(record[key]));
  if (missing.length > 0) {
    throw new Error(
      `config.json is missing required string field(s): ${missing.join(', ')}`,
    );
  }
}

let cached: RuntimeConfig | undefined;

/**
 * Loads and validates `/config.json` from the serving origin. The result is
 * cached for the lifetime of the page. Non-secret values only.
 */
export async function loadRuntimeConfig(): Promise<RuntimeConfig> {
  if (cached) {
    return cached;
  }
  const response = await fetch('/config.json', {
    cache: 'no-store',
    credentials: 'same-origin',
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load runtime configuration (HTTP ${response.status}).`,
    );
  }
  const parsed: unknown = await response.json();
  assertValidConfig(parsed);
  // Normalize the API base path to have no trailing slash.
  cached = {
    authority: parsed.authority,
    clientId: parsed.clientId,
    redirectUri: parsed.redirectUri,
    postLogoutRedirectUri: parsed.postLogoutRedirectUri,
    apiBasePath: parsed.apiBasePath.replace(/\/+$/, ''),
  };
  return cached;
}
