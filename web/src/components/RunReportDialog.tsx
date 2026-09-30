import {
  useEffect,
  useMemo,
  useState,
  type FormEvent,
  type ReactNode,
} from 'react';
import { ApiError } from '../api/client';
import { useFacilities, useStartReportRun } from '../api/queries';
import type { StartReportRunRequest } from '../api/types';
import { ErrorState, LoadingState } from './StateViews';

const MIN_PARTITIONS = 1;
const MAX_PARTITIONS = 200;

/**
 * Parse free-text facility UIDs separated by commas or newlines into a clean list:
 * trims whitespace, drops empties, and dedupes while preserving first-seen order.
 */
function parseManualFacilities(text: string): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const token of text.split(/[,\n]/)) {
    const value = token.trim();
    if (value.length === 0 || seen.has(value)) {
      continue;
    }
    seen.add(value);
    result.push(value);
  }
  return result;
}

interface RunReportDialogProps {
  readonly reportId: string;
  readonly reportName: string;
  readonly onClose: () => void;
}

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

function facilitiesErrorMessage(error: unknown): string {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
    return 'You are not authorized to list facilities.';
  }
  return 'Unable to load the facility directory. Please try again.';
}

function startRunErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to run this report.';
    }
    if (error.status === 400) {
      return 'The run parameters were rejected. Check the time window and facilities.';
    }
    if (error.status === 404) {
      return 'This report no longer exists.';
    }
  }
  return 'Unable to start the report run. Please try again.';
}

export function RunReportDialog({
  reportId,
  reportName,
  onClose,
}: RunReportDialogProps): ReactNode {
  const facilities = useFacilities(true);
  const startRun = useStartReportRun();
  const [fromValue, setFromValue] = useState('');
  const [toValue, setToValue] = useState('');
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
  const [filter, setFilter] = useState('');
  const [manualText, setManualText] = useState('');
  const [validationError, setValidationError] = useState<string | undefined>(
    undefined,
  );

  const manualFacilities = useMemo(
    () => parseManualFacilities(manualText),
    [manualText],
  );

  // Merge checked directory facilities with manually-entered UIDs, deduped and order-stable.
  const mergedFacilities = useMemo(() => {
    const seen = new Set<string>();
    const result: string[] = [];
    for (const value of [...selected, ...manualFacilities]) {
      if (seen.has(value)) {
        continue;
      }
      seen.add(value);
      result.push(value);
    }
    return result;
  }, [selected, manualFacilities]);

  const combinedCount = mergedFacilities.length;

  const options = facilities.data?.facilities ?? [];
  const visibleOptions = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) {
      return options;
    }
    return options.filter((facility) =>
      facility.toLowerCase().includes(needle),
    );
  }, [options, filter]);

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

  const toggleFacility = (facility: string): void => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(facility)) {
        next.delete(facility);
      } else {
        next.add(facility);
      }
      return next;
    });
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    setValidationError(undefined);

    const from = toIsoWithOffset(fromValue);
    const to = toIsoWithOffset(toValue);
    if (!from || !to) {
      setValidationError('Enter both a start and an end time.');
      return;
    }
    if (new Date(from).getTime() >= new Date(to).getTime()) {
      setValidationError('The start time must be before the end time.');
      return;
    }
    const partitionValues = mergedFacilities;
    if (partitionValues.length < MIN_PARTITIONS) {
      setValidationError(
        'Select at least one facility from the directory or enter at least one facility UID.',
      );
      return;
    }
    if (partitionValues.length > MAX_PARTITIONS) {
      setValidationError(
        `Too many facilities: ${partitionValues.length} selected, but at most ${MAX_PARTITIONS} are allowed.`,
      );
      return;
    }

    const request: StartReportRunRequest = { from, to, partitions: partitionValues };
    startRun.mutate(
      { reportId, request },
      {
        onSuccess: () => onClose(),
      },
    );
  };

  const selectionCount = selected.size;
  const canSubmit =
    !startRun.isPending &&
    combinedCount >= MIN_PARTITIONS &&
    combinedCount <= MAX_PARTITIONS;

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
        aria-label={`Run report ${reportName}`}
      >
        <header className="modal__header">
          <h2 className="modal__title">Run report: {reportName}</h2>
          <button
            type="button"
            className="button"
            aria-label="Close dialog"
            onClick={onClose}
          >
            Close
          </button>
        </header>

        <form className="run-form" onSubmit={handleSubmit}>
          <div className="run-form__times">
            <label className="run-form__field">
              From
              <input
                type="datetime-local"
                value={fromValue}
                onChange={(event) => setFromValue(event.target.value)}
                required
              />
            </label>
            <label className="run-form__field">
              To
              <input
                type="datetime-local"
                value={toValue}
                onChange={(event) => setToValue(event.target.value)}
                required
              />
            </label>
          </div>

          <fieldset className="run-form__facilities">
            <legend>
              Facilities ({combinedCount} selected; choose {MIN_PARTITIONS}–
              {MAX_PARTITIONS})
            </legend>
            {facilities.isLoading ? (
              <LoadingState label="Loading facilities…" />
            ) : facilities.isError ? (
              <ErrorState
                message={facilitiesErrorMessage(facilities.error)}
                onRetry={() => void facilities.refetch()}
              />
            ) : options.length === 0 ? (
              <p className="run-form__empty">
                No facilities are available to scope this report.
              </p>
            ) : (
              <>
                <input
                  type="search"
                  className="run-form__facility-filter"
                  placeholder="Filter facilities"
                  aria-label="Filter facilities"
                  value={filter}
                  onChange={(event) => setFilter(event.target.value)}
                />
                <div
                  className="run-form__facility-list"
                  role="group"
                  aria-label="Facility selection"
                >
                  {visibleOptions.map((facility) => (
                    <label key={facility} className="run-form__facility-option">
                      <input
                        type="checkbox"
                        checked={selected.has(facility)}
                        onChange={() => toggleFacility(facility)}
                      />
                      <span>{facility}</span>
                    </label>
                  ))}
                  {visibleOptions.length === 0 ? (
                    <p className="run-form__empty">No facilities match the filter.</p>
                  ) : null}
                </div>
              </>
            )}
          </fieldset>

          <div className="run-form__manual">
            <label className="run-form__field">
              <span className="run-form__manual-label">
                Additional facility UIDs (optional)
              </span>
              <textarea
                className="run-form__manual-input"
                value={manualText}
                onChange={(event) => setManualText(event.target.value)}
                placeholder="Enter facility UIDs separated by commas or new lines, e.g. abc, sd, sdf"
                aria-label="Additional facility UIDs, comma or newline separated"
                rows={3}
                spellCheck={false}
              />
            </label>
            <p className="run-form__manual-hint">
              Separate UIDs with commas or new lines. Entries are trimmed and
              de-duplicated, then merged with the facilities checked above.
              {manualFacilities.length > 0
                ? ` ${manualFacilities.length} manual UID${
                    manualFacilities.length === 1 ? '' : 's'
                  } entered.`
                : ''}
            </p>
            <p className="run-form__manual-summary" role="status">
              {combinedCount} facilit{combinedCount === 1 ? 'y' : 'ies'} selected
              in total ({selectionCount} from directory, {manualFacilities.length}{' '}
              manual). Choose {MIN_PARTITIONS}–{MAX_PARTITIONS}.
            </p>
          </div>

          {validationError ? (
            <p className="run-form__error" role="alert">
              {validationError}
            </p>
          ) : null}
          {startRun.isError ? (
            <p className="run-form__error" role="alert">
              {startRunErrorMessage(startRun.error)}
            </p>
          ) : null}

          <div className="run-form__actions">
            <button type="button" className="button" onClick={onClose}>
              Cancel
            </button>
            <button
              type="submit"
              className="button button--primary"
              disabled={!canSubmit}
            >
              {startRun.isPending ? (
                <>
                  <span className="spinner spinner--button" aria-hidden="true" />
                  Starting…
                </>
              ) : (
                'Start run'
              )}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
