import {
  createContext,
  useContext,
  useMemo,
  useRef,
  type ReactNode,
} from 'react';
import { useAuth } from 'react-oidc-context';
import { ApiClient } from './client';
import type { RuntimeConfig } from '../config';

const ApiClientContext = createContext<ApiClient | undefined>(undefined);

interface ApiClientProviderProps {
  readonly config: RuntimeConfig;
  readonly children: ReactNode;
}

/**
 * Provides a singleton {@link ApiClient}. The client reads the current access
 * token lazily via a ref so it always attaches the freshest bearer token
 * without recreating the client (or its React Query cache identity).
 */
export function ApiClientProvider({
  config,
  children,
}: ApiClientProviderProps): ReactNode {
  const auth = useAuth();
  const tokenRef = useRef<string | undefined>(undefined);
  tokenRef.current = auth.user?.access_token;

  const client = useMemo(
    () =>
      new ApiClient({
        basePath: config.apiBasePath,
        getAccessToken: () => tokenRef.current,
      }),
    [config.apiBasePath],
  );

  return (
    <ApiClientContext.Provider value={client}>
      {children}
    </ApiClientContext.Provider>
  );
}

export function useApiClient(): ApiClient {
  const client = useContext(ApiClientContext);
  if (!client) {
    throw new Error('useApiClient must be used within an ApiClientProvider');
  }
  return client;
}
