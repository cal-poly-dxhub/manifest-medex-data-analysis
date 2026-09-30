import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useUpdateReport } from '../api/queries';
import type { ReportDefinition } from '../api/types';
import { parseReportDefinitionDraft } from '../util/reportEditing';

interface EditReportJsonDialogProps {
  readonly reportId: string;
  /** The clean, canonical definition used to seed the editor. */
  readonly definition: ReportDefinition;
  /** The report's optimistic-lock token; null means it cannot be saved safely. */
  readonly updatedAt: string | null;
  readonly onClose: () => void;
  /** Called after a successful save, before the dialog is expected to close. */
  readonly onSaved?: () => void;
}

function saveErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 409) {
      return 'This report was changed by someone else since you opened it. Close the editor and reopen the report to get the latest version before editing.';
    }
    if (error.status === 400) {
      return 'The definition is invalid. Check it against the report schema.';
    }
    if (error.status === 404) {
      return 'This report no longer exists.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to edit this report.';
    }
    if (error.status === 413) {
      return 'The definition exceeds the maximum allowed size.';
    }
  }
  return 'Unable to save the report. Please try again.';
}

/**
 * Modal to edit a whole report definition as JSON. The editor is seeded from the clean
 * definition, validates that the text is a JSON object whose `report_id` still matches
 * this report, and saves through a full-definition PUT guarded by the report's
 * `updatedAt` lock (surfacing a 409 conflict). The definition text is never logged.
 */
export function EditReportJsonDialog({
  reportId,
  definition,
  updatedAt,
  onClose,
  onSaved,
}: EditReportJsonDialogProps): ReactNode {
  const updateReport = useUpdateReport();
  const [draft, setDraft] = useState(() => JSON.stringify(definition, null, 2));

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const parse = useMemo(
    () => parseReportDefinitionDraft(draft, reportId),
    [draft, reportId],
  );
  const lockMissing = updatedAt === null;
  const canSave = parse.kind === 'valid' && !lockMissing && !updateReport.isPending;

  const handleSave = (): void => {
    if (parse.kind !== 'valid' || updatedAt === null) {
      return;
    }
    updateReport.mutate(
      { reportId, definition: parse.value, updatedAt },
      {
        onSuccess: () => {
          onSaved?.();
          onClose();
        },
      },
    );
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
        className="modal modal--wide"
        role="dialog"
        aria-modal="true"
        aria-label="Edit full report JSON"
      >
        <header className="modal__header">
          <h2 className="modal__title">Edit full report JSON</h2>
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
          <label className="report-editor__label" htmlFor="report-edit-json">
            Report definition (JSON)
          </label>
          <textarea
            id="report-edit-json"
            className="report-editor__textarea report-editor__textarea--tall"
            value={draft}
            spellCheck={false}
            onChange={(event) => setDraft(event.target.value)}
          />
          {parse.kind === 'invalid-json' ? (
            <p className="report-editor__hint" role="status">
              The definition is not valid JSON.
            </p>
          ) : null}
          {parse.kind === 'not-object' ? (
            <p className="report-editor__hint" role="status">
              The definition must be a JSON object.
            </p>
          ) : null}
          {parse.kind === 'id-mismatch' ? (
            <p className="report-editor__hint" role="status">
              The report_id must stay "{reportId}". You cannot change a report's id
              through this editor.
            </p>
          ) : null}
          {lockMissing ? (
            <p className="report-editor__hint" role="status">
              This report has no lock token and cannot be saved. Reload the report.
            </p>
          ) : null}
          {updateReport.isError ? (
            <p className="report-editor__error" role="alert">
              {saveErrorMessage(updateReport.error)}
            </p>
          ) : null}
          <div className="report-editor__actions">
            <button type="button" className="button" onClick={onClose}>
              Cancel
            </button>
            <button
              type="button"
              className="button button--primary"
              disabled={!canSave}
              onClick={handleSave}
            >
              {updateReport.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Saving…
                </>
              ) : (
                'Save definition'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
