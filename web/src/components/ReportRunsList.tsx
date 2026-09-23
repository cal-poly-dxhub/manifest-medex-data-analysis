import { useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useDownloadReportOutput, useReportRuns } from '../api/queries';
import type { ReportRun } from '../api/types';
import { formatTimestamp } from '../util/format';
import { EmptyState, ErrorState, LoadingState } from './StateViews';

interface ReportRunsListProps {
  readonly reportId: string;
}

const STATUS_LABELS: Record<ReportRun['status'], string> = {
  running: 'Running',
  complete: 'Complete',
  failed: 'Failed',
};

function runsErrorMessage(error: unknown): string {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
    return 'You are not authorized to view runs for this report.';
  }
  return 'Unable to load report runs. Please try again.';
}

function downloadErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to download this report output.';
    }
    if (error.status === 404) {
      return 'This run output no longer exists.';
    }
    if (error.status === 409) {
      return 'This run is not complete yet.';
    }
    if (error.status === 413) {
      return 'The report output exceeds the download-size limit.';
    }
  }
  return 'Unable to download the report output. Please try again.';
}

function progressLabel(run: ReportRun): string {
  const { completedPartitions, totalPartitions } = run.progress;
  return `${completedPartitions}/${totalPartitions} partitions`;
}

/**
 * Build the optional run-level summary line, e.g. "80 rows executed, 37 placeholders
 * skipped". Returns undefined when the run carries no summary or no numeric parts.
 */
function runSummaryLabel(run: ReportRun): string | undefined {
  const summary = run.summary;
  if (!summary) {
    return undefined;
  }
  const parts: string[] = [];
  if (typeof summary.rowsExecuted === 'number') {
    parts.push(`${summary.rowsExecuted} rows executed`);
  }
  if (typeof summary.placeholdersSkipped === 'number') {
    parts.push(`${summary.placeholdersSkipped} placeholders skipped`);
  }
  return parts.length > 0 ? parts.join(', ') : undefined;
}

export function ReportRunsList({ reportId }: ReportRunsListProps): ReactNode {
  const runs = useReportRuns(reportId);
  const download = useDownloadReportOutput();
  const [downloadingRunId, setDownloadingRunId] = useState<string | undefined>(
    undefined,
  );
  const [downloadError, setDownloadError] = useState<string | undefined>(
    undefined,
  );

  const handleDownload = (run: ReportRun): void => {
    setDownloadError(undefined);
    setDownloadingRunId(run.runId);
    download.mutate(
      { runId: run.runId },
      {
        onSuccess: (blob) => {
          // Hand the authenticated blob straight to the browser; nothing logged.
          const url = URL.createObjectURL(blob);
          const anchor = document.createElement('a');
          anchor.href = url;
          anchor.download = `${reportId}-${run.runId}.zip`;
          document.body.appendChild(anchor);
          anchor.click();
          anchor.remove();
          URL.revokeObjectURL(url);
          setDownloadingRunId(undefined);
        },
        onError: (error) => {
          setDownloadError(downloadErrorMessage(error));
          setDownloadingRunId(undefined);
        },
      },
    );
  };

  if (runs.isLoading) {
    return <LoadingState label="Loading runs…" />;
  }
  if (runs.isError) {
    return (
      <ErrorState
        message={runsErrorMessage(runs.error)}
        onRetry={() => void runs.refetch()}
      />
    );
  }
  const items = runs.data?.items ?? [];
  if (items.length === 0) {
    return <EmptyState message="This report has not been run yet." />;
  }

  return (
    <div className="runs">
      {downloadError ? (
        <p className="runs__error" role="alert">
          {downloadError}
        </p>
      ) : null}
      <div className="table-wrap" role="region" aria-label="Report runs" tabIndex={0}>
        <table className="table runs__table">
          <thead>
            <tr>
              <th scope="col">Status</th>
              <th scope="col">Started</th>
              <th scope="col">Progress</th>
              <th scope="col">Window</th>
              <th scope="col">Facilities</th>
              <th scope="col">Output</th>
            </tr>
          </thead>
          <tbody>
            {items.map((run) => (
              <tr key={run.runId}>
                <td>
                  <span
                    className={`run-status run-status--${run.status}`}
                    role="status"
                  >
                    {run.status === 'running' ? (
                      <span className="spinner spinner--button" aria-hidden="true" />
                    ) : null}
                    {STATUS_LABELS[run.status]}
                  </span>
                  {run.status === 'failed' && run.failingPartition ? (
                    <span className="run-status__detail">
                      Failed partition: {run.failingPartition}
                    </span>
                  ) : null}
                </td>
                <td>{formatTimestamp(run.startedAt)}</td>
                <td>
                  <span className="runs__progress-label">{progressLabel(run)}</span>
                  <progress
                    className="runs__progress"
                    max={run.progress.totalPartitions || 1}
                    value={run.progress.completedPartitions}
                    aria-label={`Run progress: ${progressLabel(run)}`}
                  />
                  {runSummaryLabel(run) ? (
                    <span className="runs__summary">{runSummaryLabel(run)}</span>
                  ) : null}
                </td>
                <td className="cell-mono">
                  {formatTimestamp(run.params.from)} –{' '}
                  {formatTimestamp(run.params.to)}
                </td>
                <td>{run.params.partitionValues.length}</td>
                <td>
                  {run.status === 'complete' && run.downloadReady ? (
                    <button
                      type="button"
                      className="button"
                      disabled={
                        download.isPending && downloadingRunId === run.runId
                      }
                      onClick={() => handleDownload(run)}
                    >
                      {download.isPending && downloadingRunId === run.runId ? (
                        <>
                          <span
                            className="spinner spinner--button"
                            aria-hidden="true"
                          />
                          Downloading…
                        </>
                      ) : (
                        'Download'
                      )}
                    </button>
                  ) : (
                    <span className="runs__no-output">—</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
