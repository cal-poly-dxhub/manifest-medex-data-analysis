import { type ReactNode } from "react";
import { ApiError } from "../api/client";
import { useReingestJobs } from "../api/queries";
import type { ReingestJob, ReingestJobStatus } from "../api/types";
import { formatTimestamp } from "../util/format";
import { EmptyState, ErrorState, LoadingState } from "./StateViews";

const STATUS_LABELS: Record<ReingestJobStatus, string> = {
  queued: "Queued",
  running: "Running",
  complete: "Complete",
  failed: "Failed",
};

const ACTIVE_STATUSES: ReadonlySet<ReingestJobStatus> = new Set([
  "queued",
  "running",
]);

/**
 * Status to display. A finished job with any permanently failed document is shown as
 * Failed even if the stored status says complete, so the badge never contradicts the
 * Failed counter beside it.
 */
function displayStatus(job: ReingestJob): ReingestJobStatus {
  if (!ACTIVE_STATUSES.has(job.status) && job.counters.failed > 0) {
    return "failed";
  }
  return job.status;
}

function jobsErrorMessage(error: unknown): string {
  if (
    error instanceof ApiError &&
    (error.status === 401 || error.status === 403)
  ) {
    return "You are not authorized to view reingestion jobs.";
  }
  return "Unable to load reingestion jobs. Please try again.";
}

/**
 * Render the selection cell for one job. A SQL job expands to reveal exactly what the
 * backend recorded — the guarded SQL when present, otherwise its SHA-256 fingerprint (the
 * backend intentionally returns only the fingerprint). An id job shows its document count.
 * The SQL text lives only inside the collapsed `details`/`pre`; it is never logged.
 */
function JobSelectionCell({ job }: { readonly job: ReingestJob }): ReactNode {
  if (job.mode === "sql") {
    if (typeof job.sql === "string") {
      return (
        <details className="reingest-job-sql">
          <summary>View SQL</summary>
          <pre className="reingest-job-sql__text">{job.sql}</pre>
        </details>
      );
    }
    if (typeof job.sqlSha256 === "string") {
      return (
        <details className="reingest-job-sql">
          <summary>SQL fingerprint</summary>
          <pre className="reingest-job-sql__text">{job.sqlSha256}</pre>
        </details>
      );
    }
    return <span className="reingest-jobs__muted">SQL</span>;
  }
  const idCount = typeof job.idCount === "number" ? job.idCount : job.expected;
  return <span className="reingest-jobs__muted">{idCount} document ids</span>;
}

/**
 * Lists parsed-zone reingestion jobs newest-first and polls every four seconds while any
 * job is still queued or running. Every row shows the lifecycle status, the five outcome
 * counters, the requester, the created/finished times, and the expected document count.
 * SQL jobs carry an expandable selection cell; nothing here is ever written to the console.
 */
export function ReingestJobsPanel(): ReactNode {
  const jobs = useReingestJobs(true);

  return (
    <section className="reingest-jobs" aria-label="Reingestion jobs">
      <h2 className="reingest-jobs__title">Reingestion jobs</h2>
      {jobs.isLoading ? (
        <LoadingState label="Loading reingestion jobs…" />
      ) : jobs.isError ? (
        <ErrorState
          message={jobsErrorMessage(jobs.error)}
          onRetry={() => void jobs.refetch()}
        />
      ) : (jobs.data?.items.length ?? 0) === 0 ? (
        <EmptyState message="No reingestion jobs have been started yet." />
      ) : (
        <div
          className="table-wrap"
          role="region"
          aria-label="Reingestion jobs"
          tabIndex={0}
        >
          <table className="table reingest-jobs__table">
            <thead>
              <tr>
                <th scope="col">Status</th>
                <th scope="col">Expected</th>
                <th scope="col">Enqueued</th>
                <th scope="col">Reindexed</th>
                <th scope="col">Stale parser</th>
                <th scope="col">Missing parsed</th>
                <th scope="col">Failed</th>
                <th scope="col">Requested by</th>
                <th scope="col">Created</th>
                <th scope="col">Finished</th>
                <th scope="col">Selection</th>
              </tr>
            </thead>
            <tbody>
              {jobs.data?.items.map((job) => {
                const status = displayStatus(job);
                return (
                  <tr key={job.jobId}>
                    <td>
                      <span
                        className={`run-status run-status--${status}`}
                        role="status"
                      >
                        {ACTIVE_STATUSES.has(status) ? (
                          <span
                            className="spinner spinner--button"
                            aria-hidden="true"
                          />
                        ) : null}
                        {STATUS_LABELS[status]}
                      </span>
                    </td>
                    <td>{job.expected}</td>
                    <td>{job.counters.enqueued}</td>
                    <td>{job.counters.reindexed}</td>
                    <td>{job.counters.reindexedStaleParser}</td>
                    <td>{job.counters.missingParsed}</td>
                    <td>{job.counters.failed}</td>
                    <td>{job.requestedBy}</td>
                    <td>{formatTimestamp(job.createdAt)}</td>
                    <td>{formatTimestamp(job.finishedAt ?? null)}</td>
                    <td>
                      <JobSelectionCell job={job} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
