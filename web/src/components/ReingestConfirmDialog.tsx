import { useEffect, useRef, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useCreateReingestJob, useReingestPreview } from '../api/queries';
import {
  REINGEST_MAX_SELECTION,
  type CreateReingestJobRequest,
} from '../api/types';

/**
 * The selection a confirmation covers. An `ids` selection already knows its exact count
 * (the number of chosen rows), so it is confirmed directly. A `sql` selection is previewed
 * against the backend first to learn how many documents the guarded SELECT resolves to.
 */
export type ReingestSelection =
  | { readonly kind: 'ids'; readonly documentIds: readonly string[] }
  | { readonly kind: 'sql'; readonly sql: string };

interface ReingestConfirmDialogProps {
  readonly selection: ReingestSelection;
  readonly onClose: () => void;
  /** Called after a job is queued so the caller can refresh and dismiss the dialog. */
  readonly onCreated: () => void;
}

function previewErrorMessage(error: unknown): string {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
    return 'You are not authorized to preview reingestion selections.';
  }
  return 'Unable to preview this selection. Check the query and try again.';
}

function createErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to start reingestion jobs.';
    }
    if (error.status === 400) {
      return 'This reingestion selection was rejected. Adjust it and try again.';
    }
  }
  return 'Unable to start the reingestion job. Please try again.';
}

/**
 * Accessible confirmation modal for queuing a parsed-zone reingestion. For a SQL selection
 * it previews the exact reindexable-document count before confirming; for an id selection
 * the count is the number of selected rows. Confirmation is blocked when the count is 0 or
 * exceeds the backend selection cap, each with a clear hint. All loading and error text is
 * sanitized: neither the SQL, the ids, nor any backend body is ever surfaced or logged.
 */
export function ReingestConfirmDialog({
  selection,
  onClose,
  onCreated,
}: ReingestConfirmDialogProps): ReactNode {
  const preview = useReingestPreview();
  const create = useCreateReingestJob();
  const cancelButtonRef = useRef<HTMLButtonElement | null>(null);

  const isSql = selection.kind === 'sql';
  const sql = isSql ? selection.sql : undefined;

  // Preview a SQL selection exactly once when the dialog opens. An id selection needs no
  // preview because the count is simply the number of chosen rows.
  useEffect(() => {
    if (typeof sql === 'string') {
      preview.mutate(sql);
    }
    // The selection is fixed for the lifetime of an open dialog; run once on open.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sql]);

  useEffect(() => {
    cancelButtonRef.current?.focus();
  }, []);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const count = isSql ? preview.data?.count : selection.documentIds.length;
  const previewing =
    isSql && (preview.isPending || (!preview.isError && count === undefined));
  const tooMany = typeof count === 'number' && count > REINGEST_MAX_SELECTION;
  const empty = count === 0;
  const canConfirm =
    typeof count === 'number' &&
    !empty &&
    !tooMany &&
    !create.isPending &&
    !previewing;

  const handleConfirm = (): void => {
    if (!canConfirm) {
      return;
    }
    const request: CreateReingestJobRequest =
      selection.kind === 'ids'
        ? { documentIds: selection.documentIds }
        : { sql: selection.sql };
    create.mutate(request, {
      onSuccess: () => {
        onCreated();
      },
    });
  };

  return (
    <div
      className="modal-overlay"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="reingest-confirm-title"
        aria-describedby="reingest-confirm-body"
      >
        <header className="modal__header">
          <h2 className="modal__title" id="reingest-confirm-title">
            Confirm reingestion
          </h2>
          <button
            type="button"
            className="button"
            aria-label="Close dialog"
            onClick={onClose}
          >
            Close
          </button>
        </header>

        <div className="reingest-confirm" id="reingest-confirm-body">
          {previewing ? (
            <div className="reingest-confirm__status" role="status" aria-live="polite">
              <span className="spinner" aria-hidden="true" />
              Counting matching documents…
            </div>
          ) : isSql && preview.isError ? (
            <p className="report-editor__error" role="alert">
              {previewErrorMessage(preview.error)}
            </p>
          ) : (
            <>
              <p className="reingest-confirm__message">
                This will reingest {count} documents as originally parsed. Documents from
                older parser versions are restored unchanged.
              </p>
              {empty ? (
                <p className="reingest-confirm__hint" role="alert">
                  This selection resolves to no documents, so there is nothing to
                  reingest.
                </p>
              ) : null}
              {tooMany ? (
                <p className="reingest-confirm__hint" role="alert">
                  This selection has {count} documents, above the{' '}
                  {REINGEST_MAX_SELECTION.toLocaleString()} limit. Narrow the selection
                  and try again.
                </p>
              ) : null}
            </>
          )}

          {create.isError ? (
            <p className="report-editor__error" role="alert">
              {createErrorMessage(create.error)}
            </p>
          ) : null}

          <div className="report-editor__actions">
            <button
              ref={cancelButtonRef}
              type="button"
              className="button"
              onClick={onClose}
            >
              Cancel
            </button>
            <button
              type="button"
              className="button button--primary"
              disabled={!canConfirm}
              onClick={handleConfirm}
            >
              {create.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Starting…
                </>
              ) : (
                'Reingest documents'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
