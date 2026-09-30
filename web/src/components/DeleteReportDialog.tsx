import { useEffect, useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useDeleteReport } from '../api/queries';
import { deleteConfirmationMatches } from '../util/reportEditing';

interface DeleteReportDialogProps {
  readonly reportId: string;
  /** Human-readable report name, shown for context in the warning. */
  readonly reportName: string;
  readonly onClose: () => void;
  /** Called after a successful delete so the caller can leave the now-gone report. */
  readonly onDeleted: () => void;
}

function deleteErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 404) {
      return 'This report no longer exists. It may already have been deleted.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to delete this report.';
    }
  }
  return 'Unable to delete the report. Please try again.';
}

/**
 * Destructive confirmation modal for permanently deleting a report. Deletion is
 * irreversible, so the confirm button stays disabled until the user types the exact
 * report id. On success the caller is notified so it can navigate away.
 */
export function DeleteReportDialog({
  reportId,
  reportName,
  onClose,
  onDeleted,
}: DeleteReportDialogProps): ReactNode {
  const deleteReport = useDeleteReport();
  const [typed, setTyped] = useState('');

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const confirmed = deleteConfirmationMatches(typed, reportId);
  const canDelete = confirmed && !deleteReport.isPending;

  const handleDelete = (): void => {
    if (!confirmed) {
      return;
    }
    deleteReport.mutate(reportId, {
      onSuccess: () => {
        onDeleted();
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
        aria-label="Delete report"
      >
        <header className="modal__header">
          <h2 className="modal__title">Delete report</h2>
          <button
            type="button"
            className="button"
            aria-label="Close dialog"
            onClick={onClose}
          >
            Close
          </button>
        </header>

        <div className="report-editor">
          <p className="delete-report__warning" role="alert">
            This permanently deletes <strong>{reportName}</strong> and every section,
            row, and run history associated with it. This action cannot be undone.
          </p>
          <label className="report-editor__label" htmlFor="delete-report-confirm">
            Type the report id <code>{reportId}</code> to confirm.
          </label>
          <input
            id="delete-report-confirm"
            type="text"
            className="delete-report__input"
            value={typed}
            spellCheck={false}
            autoComplete="off"
            autoCapitalize="off"
            autoCorrect="off"
            onChange={(event) => setTyped(event.target.value)}
          />
          {deleteReport.isError ? (
            <p className="report-editor__error" role="alert">
              {deleteErrorMessage(deleteReport.error)}
            </p>
          ) : null}
          <div className="report-editor__actions">
            <button type="button" className="button" onClick={onClose}>
              Cancel
            </button>
            <button
              type="button"
              className="button button--danger"
              disabled={!canDelete}
              onClick={handleDelete}
            >
              {deleteReport.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Deleting…
                </>
              ) : (
                'Delete report'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
