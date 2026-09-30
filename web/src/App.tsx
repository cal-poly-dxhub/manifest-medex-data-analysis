import { useEffect, useState, type ReactNode } from 'react';
import { useAuth } from 'react-oidc-context';
import { MessageExplorer } from './components/MessageExplorer';
import { SearchExplorer } from './components/SearchExplorer';
import { SqlConsole } from './components/SqlConsole';
import { ReportsExplorer } from './components/ReportsExplorer';
import { ErrorState, LoadingState } from './components/StateViews';

export function App(): ReactNode {
  const auth = useAuth();
  const [activeView, setActiveView] = useState<'messages' | 'search' | 'sql' | 'reports'>(
    'messages',
  );

  // Initiate the hosted-UI Authorization Code + PKCE flow automatically when
  // the user is not authenticated. No self-signup UI is presented.
  useEffect(() => {
    if (
      !auth.isLoading &&
      !auth.isAuthenticated &&
      !auth.activeNavigator &&
      !auth.error
    ) {
      void auth.signinRedirect();
    }
  }, [auth.isLoading, auth.isAuthenticated, auth.activeNavigator, auth.error, auth]);

  if (auth.error) {
    return (
      <main className="app app--centered">
        <ErrorState
          message="Sign-in failed. Please try again."
          onRetry={() => void auth.signinRedirect()}
        />
      </main>
    );
  }

  if (auth.isLoading || auth.activeNavigator) {
    return (
      <main className="app app--centered">
        <LoadingState label="Signing in…" />
      </main>
    );
  }

  if (!auth.isAuthenticated) {
    return (
      <main className="app app--centered">
        <LoadingState label="Redirecting to sign in…" />
      </main>
    );
  }

  return (
    <div className="app">
      <header className="app__header">
        <h1 className="app__title">PHI Explorer</h1>
        <div className="app__header-actions">
          <div className="view-switcher" role="tablist" aria-label="Explorer view">
            <button
              type="button"
              role="tab"
              aria-selected={activeView === 'messages'}
              className={activeView === 'messages' ? 'tab tab--active' : 'tab'}
              onClick={() => setActiveView('messages')}
            >
              Messages
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={activeView === 'search'}
              className={activeView === 'search' ? 'tab tab--active' : 'tab'}
              onClick={() => setActiveView('search')}
            >
              Search
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={activeView === 'sql'}
              className={activeView === 'sql' ? 'tab tab--active' : 'tab'}
              onClick={() => setActiveView('sql')}
            >
              SQL query
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={activeView === 'reports'}
              className={activeView === 'reports' ? 'tab tab--active' : 'tab'}
              onClick={() => setActiveView('reports')}
            >
              Reports
            </button>
          </div>
          <button
            type="button"
            className="button"
            onClick={() =>
              void auth.signoutRedirect().catch(() => {
                // Fall back to a local sign-out if the IdP logout is unavailable.
                void auth.removeUser();
              })
            }
          >
            Sign out
          </button>
        </div>
      </header>
      <main className="app__body">
        {activeView === 'messages' ? (
          <MessageExplorer />
        ) : activeView === 'search' ? (
          <SearchExplorer />
        ) : activeView === 'sql' ? (
          <SqlConsole />
        ) : (
          <ReportsExplorer />
        )}
      </main>
    </div>
  );
}
