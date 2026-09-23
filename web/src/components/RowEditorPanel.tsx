import {
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { ApiError } from '../api/client';
import {
  useAddRow,
  useDeleteRow,
  useFacilities,
  useTestQuery,
  useUpdateRow,
} from '../api/queries';
import {
  REPORT_ROW_INDEXES,
  type ReportDefinitionRow,
  type ReportRowInput,
} from '../api/types';
import { ErrorState, LoadingState } from './StateViews';

/** What the panel is editing: an existing row (edit) or a new row in a section (create). */
export type RowEditorTarget =
  | {
      readonly mode: 'edit';
      readonly sectionStorageSeq: number;
      readonly rowStorageSeq: number;
      readonly definition: ReportDefinitionRow;
      /** The row's optimistic-lock token; null means the row cannot be edited safely. */
      readonly updatedAt: string | null;
    }
  | {
      readonly mode: 'create';
      readonly sectionStorageSeq: number;
      /** Insert-after position, or undefined to append at the section's end. */
      readonly afterStorageSeq?: number;
      /** The logical definition seq to assign to the new row (max existing + 1). */
      readonly nextSeq: number;
    };

interface RowEditorPanelProps {
  readonly reportId: string;
  readonly target: RowEditorTarget;
  readonly onClose: () => void;
}

/** Classification of the query editor text. */
type QueryParse =
  | { readonly kind: 'placeholder' }
  | { readonly kind: 'valid'; readonly value: Record<string, unknown> }
  | { readonly kind: 'invalid' };

/** Converts a `datetime-local` value (local time) to an ISO-8601 UTC string. */
function toIsoWithOffset(localValue: string): string | undefined {
  if (!localValue) {
    return undefined;
  }
  const parsed = new Date(localValue);
  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }
  return parsed.toISOString();
}

/**
 * Classify the query editor text. Empty/whitespace text or a literal JSON `null` is a
 * placeholder, saved as `query: null`. A non-empty JSON object is valid. Anything else
 * (arrays, primitives, an empty object, or unparseable text) is invalid and blocks save.
 */
function parseQuery(text: string): QueryParse {
  if (text.trim().length === 0) {
    return { kind: 'placeholder' };
  }
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return { kind: 'invalid' };
  }
  if (value === null) {
    return { kind: 'placeholder' };
  }
  if (
    typeof value !== 'object' ||
    Array.isArray(value) ||
    Object.keys(value).length === 0
  ) {
    return { kind: 'invalid' };
  }
  return { kind: 'valid', value: value as Record<string, unknown> };
}

function saveErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 409) {
      return 'This row was changed by someone else since you opened it. Close the panel and reopen the row to get the latest version before editing.';
    }
    if (error.status === 400) {
      return 'The row is invalid. Check the label, index, and query JSON.';
    }
    if (error.status === 404) {
      return 'This row or section no longer exists.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to edit this report.';
    }
  }
  return 'Unable to save the row. Please try again.';
}

function deleteErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 409) {
      return 'This row was changed since you opened it and cannot be deleted. Reopen the row first.';
    }
    if (error.status === 404) {
      return 'This row no longer exists.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to edit this report.';
    }
  }
  return 'Unable to delete the row. Please try again.';
}

function testErrorMessage(error: unknown): string {
  if (error instanceof ApiError && error.status === 400) {
    return 'The query was rejected. Check the query JSON, index, facility, and time range.';
  }
  return 'Unable to test the query. Please try again.';
}

/**
 * Right-side drawer to edit or create one report row. It edits label, description, index,
 * and the query JSON, saves through a row-granular PUT/POST guarded by the row's
 * `updatedAt` lock (surfacing a 409 conflict), deletes with the same lock, and offers a
 * dry-run query test scoped by an optional directory facility and an optional paired time
 * window. The query body and any clinical response are never logged.
 */
export function RowEditorPanel({
  reportId,
  target,
  onClose,
}: RowEditorPanelProps): ReactNode {
  const isEdit = target.mode === 'edit';
  const updateRow = useUpdateRow();
  const addRow = useAddRow();
  const deleteRow = useDeleteRow();
  const testQuery = useTestQuery();
  const facilities = useFacilities(true);

  const [label, setLabel] = useState(
    target.mode === 'edit' ? target.definition.label : '',
  );
  const [description, setDescription] = useState(
    target.mode === 'edit' ? target.definition.description : '',
  );
  const [index, setIndex] = useState<string>(
    target.mode === 'edit' ? target.definition.index : REPORT_ROW_INDEXES[0],
  );
  const [queryText, setQueryText] = useState(
    target.mode === 'edit'
      ? target.definition.query === null
        ? ''
        : JSON.stringify(target.definition.query, null, 2)
      : '',
  );
  const [testFacility, setTestFacility] = useState('');
  const [testFrom, setTestFrom] = useState('');
  const [testTo, setTestTo] = useState('');
  const [testValidation, setTestValidation] = useState<string | undefined>(undefined);
  const [confirmingDelete, setConfirmingDelete] = useState(false);

  // Close on Escape for keyboard accessibility.
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const queryParse = useMemo(() => parseQuery(queryText), [queryText]);
  const isPlaceholder = queryParse.kind === 'placeholder';
  const queryInvalid = queryParse.kind === 'invalid';
  const canTest = queryParse.kind === 'valid';
  const labelValid = label.trim().length > 0;
  const lockMissing = target.mode === 'edit' && target.updatedAt === null;
  const isSaving = updateRow.isPending || addRow.isPending;
  const canSave = !queryInvalid && labelValid && !lockMissing && !isSaving;

  const handleSave = (): void => {
    if (queryInvalid || !labelValid) {
      return;
    }
    const row: ReportRowInput = {
      seq: target.mode === 'edit' ? target.definition.seq : target.nextSeq,
      label: label.trim(),
      description,
      index,
      query: queryParse.kind === 'valid' ? queryParse.value : null,
    };
    if (target.mode === 'edit') {
      updateRow.mutate(
        {
          reportId,
          sectionStorageSeq: target.sectionStorageSeq,
          rowStorageSeq: target.rowStorageSeq,
          row,
          updatedAt: target.updatedAt ?? '',
        },
        { onSuccess: () => onClose() },
      );
      return;
    }
    addRow.mutate(
      {
        reportId,
        sectionStorageSeq: target.sectionStorageSeq,
        row,
        ...(target.afterStorageSeq === undefined
          ? {}
          : { afterStorageSeq: target.afterStorageSeq }),
      },
      { onSuccess: () => onClose() },
    );
  };

  const handleDelete = (): void => {
    if (target.mode !== 'edit') {
      return;
    }
    if (!confirmingDelete) {
      setConfirmingDelete(true);
      return;
    }
    deleteRow.mutate(
      {
        reportId,
        sectionStorageSeq: target.sectionStorageSeq,
        rowStorageSeq: target.rowStorageSeq,
        updatedAt: target.updatedAt ?? '',
      },
      { onSuccess: () => onClose() },
    );
  };

  const handleTest = (): void => {
    setTestValidation(undefined);
    testQuery.reset();
    if (isPlaceholder) {
      setTestValidation(
        'This row is a placeholder with no query yet. Add a query before testing.',
      );
      return;
    }
    if (queryParse.kind !== 'valid') {
      setTestValidation('Enter a valid, non-empty query JSON object first.');
      return;
    }
    const from = toIsoWithOffset(testFrom);
    const to = toIsoWithOffset(testTo);
    if ((testFrom && !from) || (testTo && !to)) {
      setTestValidation('Enter a valid start and end time, or leave both blank.');
      return;
    }
    if ((from && !to) || (to && !from)) {
      setTestValidation('Enter both a start and an end time, or leave both blank.');
      return;
    }
    if (from && to && new Date(from).getTime() >= new Date(to).getTime()) {
      setTestValidation('The start time must be before the end time.');
      return;
    }
    testQuery.mutate({
      index,
      query: queryParse.value,
      ...(testFacility ? { facility: testFacility } : {}),
      ...(from && to ? { from, to } : {}),
    });
  };

  const facilityOptions = facilities.data?.facilities ?? [];
  const title = isEdit ? 'Edit row' : 'Add row';

  return (
    <div
      className="drawer-overlay"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <aside className="drawer" role="dialog" aria-modal="true" aria-label={title}>
        <header className="drawer__header">
          <h3 className="drawer__title">{title}</h3>
          <button
            type="button"
            className="button"
            aria-label="Close panel"
            onClick={onClose}
          >
            Close
          </button>
        </header>

        <div className="row-editor">
          <label className="row-editor__field">
            <span>Label</span>
            <input
              type="text"
              value={label}
              onChange={(event) => setLabel(event.target.value)}
              required
            />
          </label>

          <label className="row-editor__field">
            <span>Description</span>
            <input
              type="text"
              value={description}
              onChange={(event) => setDescription(event.target.value)}
            />
          </label>

          <label className="row-editor__field">
            <span>Index</span>
            <select value={index} onChange={(event) => setIndex(event.target.value)}>
              {REPORT_ROW_INDEXES.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>

          <label className="row-editor__field">
            <span>Query (JSON)</span>
            <textarea
              className="row-editor__query"
              value={queryText}
              spellCheck={false}
              onChange={(event) => setQueryText(event.target.value)}
            />
          </label>
          {queryInvalid ? (
            <p className="row-editor__hint" role="status">
              The query must be a non-empty JSON object. Leave it empty (or enter
              null) to save this row as a placeholder.
            </p>
          ) : null}
          {isPlaceholder ? (
            <p className="row-editor__hint" role="status">
              No query yet — this row will be saved as a placeholder and skipped by
              runs until you add a query.
            </p>
          ) : null}
          {lockMissing ? (
            <p className="row-editor__hint" role="status">
              This row has no lock token and cannot be edited. Reload the report.
            </p>
          ) : null}

          {isEdit && updateRow.isError ? (
            <p className="row-editor__error" role="alert">
              {saveErrorMessage(updateRow.error)}
            </p>
          ) : null}
          {!isEdit && addRow.isError ? (
            <p className="row-editor__error" role="alert">
              {saveErrorMessage(addRow.error)}
            </p>
          ) : null}
          {deleteRow.isError ? (
            <p className="row-editor__error" role="alert">
              {deleteErrorMessage(deleteRow.error)}
            </p>
          ) : null}

          <div className="row-editor__actions">
            {isEdit ? (
              <button
                type="button"
                className="button row-editor__delete"
                disabled={deleteRow.isPending || lockMissing}
                onClick={handleDelete}
              >
                {deleteRow.isPending
                  ? 'Deleting…'
                  : confirmingDelete
                    ? 'Confirm delete'
                    : 'Delete row'}
              </button>
            ) : (
              <span />
            )}
            <div className="row-editor__actions-right">
              <button type="button" className="button" onClick={onClose}>
                Cancel
              </button>
              <button
                type="button"
                className="button button--primary"
                disabled={!canSave}
                onClick={handleSave}
              >
                {isSaving ? (
                  <>
                    <span className="spinner spinner--button" aria-hidden="true" />
                    Saving…
                  </>
                ) : (
                  'Save row'
                )}
              </button>
            </div>
          </div>

          <section className="row-editor__test" aria-label="Test query">
            <h4 className="row-editor__test-title">Test query</h4>
            <p className="row-editor__test-hint">
              Runs the current index and query as a dry-run count. Nothing is saved.
            </p>
            {isPlaceholder ? (
              <p className="row-editor__hint" role="status">
                Testing is unavailable for a placeholder row. Add a query first.
              </p>
            ) : null}

            <label className="row-editor__field">
              <span>Facility (optional)</span>
              {facilities.isLoading ? (
                <LoadingState label="Loading facilities…" />
              ) : facilities.isError ? (
                <ErrorState
                  message="Unable to load the facility directory."
                  onRetry={() => void facilities.refetch()}
                />
              ) : (
                <select
                  value={testFacility}
                  onChange={(event) => setTestFacility(event.target.value)}
                >
                  <option value="">Any facility</option>
                  {facilityOptions.map((facility) => (
                    <option key={facility} value={facility}>
                      {facility}
                    </option>
                  ))}
                </select>
              )}
            </label>

            <div className="row-editor__test-times">
              <label className="row-editor__field">
                <span>From (optional)</span>
                <input
                  type="datetime-local"
                  value={testFrom}
                  onChange={(event) => setTestFrom(event.target.value)}
                />
              </label>
              <label className="row-editor__field">
                <span>To (optional)</span>
                <input
                  type="datetime-local"
                  value={testTo}
                  onChange={(event) => setTestTo(event.target.value)}
                />
              </label>
            </div>

            {testValidation ? (
              <p className="row-editor__error" role="alert">
                {testValidation}
              </p>
            ) : null}
            {testQuery.isError ? (
              <p className="row-editor__error" role="alert">
                {testErrorMessage(testQuery.error)}
              </p>
            ) : null}
            {testQuery.isSuccess ? (
              testQuery.data.count === 0 ? (
                <p
                  className="row-editor__test-result row-editor__test-result--zero"
                  role="status"
                >
                  0 matches — check field names
                </p>
              ) : (
                <p className="row-editor__test-result" role="status">
                  {testQuery.data.count} matches
                </p>
              )
            ) : null}

            <button
              type="button"
              className="button"
              disabled={testQuery.isPending || !canTest}
              onClick={handleTest}
            >
              {testQuery.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Testing…
                </>
              ) : (
                'Test query'
              )}
            </button>
          </section>
        </div>
      </aside>
    </div>
  );
}
