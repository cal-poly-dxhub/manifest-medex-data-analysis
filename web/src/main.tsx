import { StrictMode, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider } from 'react-oidc-context';
import { App } from './App';
import { loadRuntimeConfig, type RuntimeConfig } from './config';
import { buildAuthProviderProps } from './auth/authConfig';
import { ApiClientProvider } from './api/ApiClientContext';
import { ErrorState } from './components/StateViews';
import './styles.css';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

function Root({ config }: { config: RuntimeConfig }): ReactNode {
  return (
    <StrictMode>
      <AuthProvider {...buildAuthProviderProps(config)}>
        <QueryClientProvider client={queryClient}>
          <ApiClientProvider config={config}>
            <App />
          </ApiClientProvider>
        </QueryClientProvider>
      </AuthProvider>
    </StrictMode>
  );
}

function FatalError({ message }: { message: string }): ReactNode {
  return (
    <StrictMode>
      <main className="app app--centered">
        <ErrorState message={message} />
      </main>
    </StrictMode>
  );
}

function bootstrap(): void {
  const container = document.getElementById('root');
  if (!container) {
    throw new Error('Root container #root not found');
  }
  const root = createRoot(container);

  loadRuntimeConfig().then(
    (config) => root.render(<Root config={config} />),
    () => {
      // Do not log the underlying error object (it may echo config internals).
      root.render(
        <FatalError message="Unable to load application configuration. Please contact your administrator." />,
      );
    },
  );
}

bootstrap();
