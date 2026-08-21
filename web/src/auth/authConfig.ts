import {
  InMemoryWebStorage,
  WebStorageStateStore,
  type UserManagerSettings,
} from 'oidc-client-ts';
import type { AuthProviderProps } from 'react-oidc-context';
import type { RuntimeConfig } from '../config';

/** Build Cognito Hosted UI Authorization Code + PKCE settings. */
export function buildAuthProviderProps(config: RuntimeConfig): AuthProviderProps {
  const settings: UserManagerSettings = {
    authority: config.authority,
    client_id: config.clientId,
    redirect_uri: config.redirectUri,
    post_logout_redirect_uri: config.postLogoutRedirectUri,
    response_type: 'code',
    scope: 'openid profile',
    // Tokens remain in memory and disappear on reload or tab close.
    userStore: new WebStorageStateStore({ store: new InMemoryWebStorage() }),
    // The transient state and PKCE verifier must survive the Hosted UI redirect.
    // sessionStorage is tab-scoped and does not persist authenticated tokens.
    stateStore: new WebStorageStateStore({ store: window.sessionStorage }),
    automaticSilentRenew: false,
    monitorSession: false,
    loadUserInfo: false,
  };

  return {
    ...settings,
    onSigninCallback: (): void => {
      window.history.replaceState({}, document.title, window.location.pathname);
    },
  };
}
