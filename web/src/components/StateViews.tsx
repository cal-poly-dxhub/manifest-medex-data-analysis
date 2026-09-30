import type { ReactNode } from 'react';

interface LoadingStateProps {
  readonly label?: string;
}

export function LoadingState({ label = 'Loading…' }: LoadingStateProps): ReactNode {
  return (
    <div className="state state--loading" role="status" aria-live="polite">
      <span className="spinner" aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}

interface ErrorStateProps {
  readonly message: string;
  readonly onRetry?: () => void;
}

export function ErrorState({ message, onRetry }: ErrorStateProps): ReactNode {
  return (
    <div className="state state--error" role="alert" aria-live="assertive">
      <p>{message}</p>
      {onRetry ? (
        <button type="button" className="button" onClick={onRetry}>
          Try again
        </button>
      ) : null}
    </div>
  );
}

interface EmptyStateProps {
  readonly message?: string;
}

export function EmptyState({
  message = 'No messages match the current filters.',
}: EmptyStateProps): ReactNode {
  return (
    <div className="state state--empty" role="status" aria-live="polite">
      <p>{message}</p>
    </div>
  );
}
