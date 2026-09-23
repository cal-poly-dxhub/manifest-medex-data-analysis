import { useEffect, useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useImportReport } from '../api/queries';
import type { ImportReportResult } from '../api/types';

interface ImportReportDialogProps {
  readonly onClose: () => void;
  readonly onImported: (result: ImportReportResult) => void;
}

function isValidJson(text: string): boolean {
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
}

function importErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 409) {
      return 'A report with this id already exists. Change the report_id or delete the existing report first.';
    }
    if (error.status === 400) {
      return 'The definition is invalid. Check it against the report schema.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to import reports.';
    }
    if (error.status === 413) {
      return 'The definition exceeds the maximum allowed size.';
    }
  }
  return 'Unable to import the report. Please try again.';
}

/** Modal to import a whole report definition as JSON, creating a new catalog report. */
export function ImportReportDialog({
  onClose,
  onImported,
}: ImportReportDialogProps): ReactNode {
  const importReport = useImportReport();
  const [draft, setDraft] = useState('');

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const draftValid = isValidJson(draft);

  const handleImport = (): void => {
    if (!draftValid) {
      return;
    }
    importReport.mutate(draft, {
      onSuccess: (result) => onImported(result),
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
      <div className="modal" role="dialog" aria-modal="true" aria-label="Import report">
        <header className="modal__header">
          <h2 className="modal__title">Import report</h2>
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
          <label className="report-editor__label" htmlFor="report-import-json">
            Report definition (JSON)
          </label>
          <textarea
            id="report-import-json"
            className="report-editor__textarea"
            value={draft}
            spellCheck={false}
            placeholder='{"report_id": "...", "name": "...", ...}'
            onChange={(event) => setDraft(event.target.value)}
          />
          {draft && !draftValid ? (
            <p className="report-editor__hint" role="status">
              The definition is not valid JSON.
            </p>
          ) : null}
          {importReport.isError ? (
            <p className="report-editor__error" role="alert">
              {importErrorMessage(importReport.error)}
            </p>
          ) : null}
          <div className="report-editor__actions">
            <button type="button" className="button" onClick={onClose}>
              Cancel
            </button>
            <button
              type="button"
              className="button button--primary"
              disabled={!draftValid || importReport.isPending}
              onClick={handleImport}
            >
              {importReport.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Importing…
                </>
              ) : (
                'Import'
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
