import { useId, useState, type ReactNode } from 'react';
import { JsonView, allExpanded, defaultStyles } from 'react-json-view-lite';
import 'react-json-view-lite/dist/index.css';
import { useMessageBody } from '../api/queries';
import type { BodyVariant } from '../api/types';
import { ApiError } from '../api/client';
import { EmptyState, ErrorState, LoadingState } from './StateViews';

interface MessageBodyTabsProps {
  readonly messageId: string;
}

const TABS: readonly { id: BodyVariant; label: string }[] = [
  { id: 'parsed', label: 'Parsed' },
  { id: 'raw', label: 'Raw' },
];

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to view this message body.';
    }
    if (error.status === 404) {
      return 'This message body is no longer available.';
    }
    if (error.status === 413) {
      return 'This message body exceeds the viewer size limit.';
    }
  }
  return 'Unable to load the message body. Please try again.';
}

export function MessageBodyTabs({ messageId }: MessageBodyTabsProps): ReactNode {
  const [active, setActive] = useState<BodyVariant>('parsed');
  const baseId = useId();
  const query = useMessageBody(messageId, active, true);

  const renderPanel = (): ReactNode => {
    if (query.isLoading || query.isFetching) {
      return <LoadingState label="Loading message body…" />;
    }
    if (query.isError) {
      return (
        <ErrorState
          message={errorMessage(query.error)}
          onRetry={() => void query.refetch()}
        />
      );
    }
    const data = query.data;
    if (data === undefined || data === null) {
      return <EmptyState message="No body content available." />;
    }
    if (active === 'parsed') {
      const jsonData =
        typeof data === 'object' ? (data as object) : ({ value: data } as object);
      return (
        <div className="json-view">
          <JsonView
            data={jsonData}
            shouldExpandNode={allExpanded}
            style={defaultStyles}
          />
        </div>
      );
    }
    if (typeof data !== 'string' || data.length === 0) {
      return <EmptyState message="No raw representation available." />;
    }
    return <pre className="raw-body">{data}</pre>;
  };

  return (
    <div className="body-tabs">
      <div className="tablist" role="tablist" aria-label="Message body representation">
        {TABS.map((tab) => {
          const selected = tab.id === active;
          return (
            <button
              key={tab.id}
              id={`${baseId}-tab-${tab.id}`}
              role="tab"
              type="button"
              aria-selected={selected}
              aria-controls={`${baseId}-panel-${tab.id}`}
              tabIndex={selected ? 0 : -1}
              className={selected ? 'tab tab--active' : 'tab'}
              onClick={() => setActive(tab.id)}
            >
              {tab.label}
            </button>
          );
        })}
      </div>
      <div
        id={`${baseId}-panel-${active}`}
        role="tabpanel"
        aria-labelledby={`${baseId}-tab-${active}`}
        className="tabpanel"
      >
        {renderPanel()}
      </div>
    </div>
  );
}
