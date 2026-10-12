import { useId, useState, type ReactNode } from 'react';
import { JsonView, allExpanded, defaultStyles } from 'react-json-view-lite';
import 'react-json-view-lite/dist/index.css';
import { useMessageBody, useMessageDownloadUrl } from '../api/queries';
import type { BodyVariant, RawMessageBody } from '../api/types';
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
      return 'This message body is too large to display. Download it to view the full content.';
    }
  }
  return 'Unable to load the message body. Please try again.';
}

function isTooLarge(error: unknown): boolean {
  return error instanceof ApiError && error.status === 413;
}

function isRawBody(data: unknown): data is RawMessageBody {
  return typeof data === 'object' && data !== null && 'text' in data && 'truncated' in data;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) {
    return `${Math.max(1, Math.round(bytes / 1024))} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/**
 * Start a browser download from a presigned URL without rendering the content. The URL
 * is followed by a hidden link rather than fetched so S3 needs no CORS rule; S3 answers
 * with `Content-Disposition: attachment`, so the page never navigates away.
 */
function startDownload(url: string): void {
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.rel = 'noopener noreferrer';
  anchor.style.display = 'none';
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
}

interface DownloadFullBodyProps {
  readonly messageId: string;
  readonly variant: BodyVariant;
}

function DownloadFullBody({ messageId, variant }: DownloadFullBodyProps): ReactNode {
  const download = useMessageDownloadUrl();
  return (
    <div className="body-download">
      <button
        type="button"
        className="button button--primary"
        disabled={download.isPending}
        onClick={() =>
          download.mutate(
            { id: messageId, variant },
            { onSuccess: (result) => startDownload(result.url) },
          )
        }
      >
        {download.isPending ? 'Preparing download…' : 'Download full file'}
      </button>
      {download.isError ? (
        <p className="body-download__error" role="alert">
          Unable to start the download. Please try again.
        </p>
      ) : null}
    </div>
  );
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
      if (isTooLarge(query.error)) {
        return (
          <div className="body-truncated">
            <p className="body-truncated__notice">{errorMessage(query.error)}</p>
            <DownloadFullBody messageId={messageId} variant={active} />
          </div>
        );
      }
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
    if (!isRawBody(data) || data.text.length === 0) {
      return <EmptyState message="No raw representation available." />;
    }
    if (data.truncated) {
      const shown = formatBytes(new TextEncoder().encode(data.text).length);
      const total = data.totalBytes === undefined ? 'a larger file' : formatBytes(data.totalBytes);
      return (
        <div className="body-truncated">
          <p className="body-truncated__notice" role="status">
            Showing the first {shown} of {total}. Download the full file to view all of it.
          </p>
          <DownloadFullBody messageId={messageId} variant="raw" />
          <pre className="raw-body">{data.text}</pre>
        </div>
      );
    }
    return <pre className="raw-body">{data.text}</pre>;
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
