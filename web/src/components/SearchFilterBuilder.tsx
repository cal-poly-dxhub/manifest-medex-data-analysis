import { useId, useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { useFacilities, useSearchFields } from '../api/queries';
import {
  SEARCH_CONTAINS_MIN_LENGTH,
  SEARCH_INDEXES,
  SEARCH_OPERATORS,
  SEARCH_PAGE_SIZE,
  type SearchFilter,
  type SearchIndex,
  type SearchOperator,
  type SearchRequest,
} from '../api/types';

interface SearchFilterBuilderProps {
  /** Called with a fully-formed request body (without cursor) when Apply succeeds. */
  readonly onApply: (request: SearchRequest) => void;
  /** Whether a search is currently running, used to disable the controls. */
  readonly isSearching: boolean;
}

/** A single in-progress filter row in the builder. */
interface DraftFilter {
  readonly key: number;
  readonly field: string;
  readonly operator: SearchOperator;
  readonly value: string;
}

/** Whether an operator requires a value (everything except `exists`). */
function operatorNeedsValue(operator: SearchOperator): boolean {
  const match = SEARCH_OPERATORS.find((entry) => entry.id === operator);
  return match ? match.needsValue : true;
}

/** Converts a `datetime-local` value to an ISO-8601 instant, or undefined when blank. */
function toIso(local: string): string | undefined {
  if (!local) {
    return undefined;
  }
  const date = new Date(local);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
}

let nextFilterKey = 1;

function newDraftFilter(): DraftFilter {
  nextFilterKey += 1;
  return { key: nextFilterKey, field: '', operator: 'equals', value: '' };
}

/**
 * Guided attribute-filter builder for non-technical users. It never accepts raw query DSL:
 * the user picks an index, then composes typed field/operator/value filters (with the
 * field name autocompleted from the index's real fields), an optional exact facility, and
 * an optional paired time window. All filters combine with AND. Apply validates the shape
 * locally and hands a bounded request to the parent.
 */
export function SearchFilterBuilder({
  onApply,
  isSearching,
}: SearchFilterBuilderProps): ReactNode {
  const indexId = useId();
  const facilityId = useId();
  const fromId = useId();
  const toId = useId();
  const fieldListId = useId();

  const [index, setIndex] = useState<SearchIndex>(SEARCH_INDEXES[0].id);
  const [filters, setFilters] = useState<DraftFilter[]>([newDraftFilter()]);
  const [facility, setFacility] = useState('');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [error, setError] = useState<string | undefined>(undefined);

  const fieldsQuery = useSearchFields(index);
  const facilitiesQuery = useFacilities(true);

  const fieldOptions = useMemo(
    () => fieldsQuery.data?.fields ?? [],
    [fieldsQuery.data],
  );
  const facilityOptions = useMemo(
    () => facilitiesQuery.data?.facilities ?? [],
    [facilitiesQuery.data],
  );

  const updateFilter = (key: number, patch: Partial<DraftFilter>): void => {
    setFilters((current) =>
      current.map((filter) =>
        filter.key === key ? { ...filter, ...patch } : filter,
      ),
    );
  };

  const addFilter = (): void => {
    setFilters((current) => [...current, newDraftFilter()]);
  };

  const removeFilter = (key: number): void => {
    setFilters((current) => {
      const next = current.filter((filter) => filter.key !== key);
      return next.length > 0 ? next : [newDraftFilter()];
    });
  };

  const handleSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();

    const builtFilters: SearchFilter[] = [];
    for (const draft of filters) {
      const field = draft.field.trim();
      if (!field) {
        // A row with no field name is treated as blank and skipped.
        continue;
      }
      if (!operatorNeedsValue(draft.operator)) {
        builtFilters.push({ field, operator: draft.operator });
        continue;
      }
      const value = draft.value.trim();
      if (!value) {
        setError('Enter a value for every filter, or choose "Has any value".');
        return;
      }
      if (
        draft.operator === 'contains' &&
        value.length < SEARCH_CONTAINS_MIN_LENGTH
      ) {
        setError(
          `"Contains" needs at least ${SEARCH_CONTAINS_MIN_LENGTH} characters.`,
        );
        return;
      }
      builtFilters.push({ field, operator: draft.operator, value });
    }

    const fromTime = toIso(from);
    const toTime = toIso(to);
    if (Boolean(fromTime) !== Boolean(toTime)) {
      setError('Enter both a start and an end time, or leave both empty.');
      return;
    }
    if (fromTime && toTime && new Date(fromTime) >= new Date(toTime)) {
      setError('The start time must be before the end time.');
      return;
    }

    setError(undefined);
    const request: SearchRequest = {
      index,
      filters: builtFilters,
      limit: SEARCH_PAGE_SIZE,
      ...(facility ? { facility } : {}),
      ...(fromTime && toTime ? { from: fromTime, to: toTime } : {}),
    };
    onApply(request);
  };

  const handleReset = (): void => {
    setFilters([newDraftFilter()]);
    setFacility('');
    setFrom('');
    setTo('');
    setError(undefined);
  };

  const fieldsHint = fieldsQuery.isError
    ? 'Field suggestions are unavailable right now; you can still type a field name.'
    : undefined;

  return (
    <form className="search-builder" onSubmit={handleSubmit} aria-label="Search filters">
      <div className="filters__field">
        <label htmlFor={indexId}>What are you searching?</label>
        <select
          id={indexId}
          value={index}
          disabled={isSearching}
          onChange={(event) => setIndex(event.target.value as SearchIndex)}
        >
          {SEARCH_INDEXES.map((option) => (
            <option key={option.id} value={option.id}>
              {option.label}
            </option>
          ))}
        </select>
      </div>

      <datalist id={fieldListId}>
        {fieldOptions.map((field) => (
          <option key={field} value={field} />
        ))}
      </datalist>

      <fieldset className="search-builder__filters">
        <legend>Filters (all must match)</legend>
        {filters.map((filter) => {
          const needsValue = operatorNeedsValue(filter.operator);
          return (
            <div className="search-filter-row" key={filter.key}>
              <label className="search-filter-row__field">
                <span className="visually-hidden">Field name</span>
                <input
                  type="text"
                  list={fieldListId}
                  placeholder="Field (e.g. messageType)"
                  autoComplete="off"
                  value={filter.field}
                  disabled={isSearching}
                  onChange={(event) =>
                    updateFilter(filter.key, { field: event.target.value })
                  }
                />
              </label>
              <label className="search-filter-row__operator">
                <span className="visually-hidden">Operator</span>
                <select
                  value={filter.operator}
                  disabled={isSearching}
                  onChange={(event) =>
                    updateFilter(filter.key, {
                      operator: event.target.value as SearchOperator,
                    })
                  }
                >
                  {SEARCH_OPERATORS.map((operator) => (
                    <option key={operator.id} value={operator.id}>
                      {operator.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="search-filter-row__value">
                <span className="visually-hidden">Value</span>
                <input
                  type="text"
                  placeholder={needsValue ? 'Value' : 'No value needed'}
                  autoComplete="off"
                  value={needsValue ? filter.value : ''}
                  disabled={isSearching || !needsValue}
                  hidden={!needsValue}
                  onChange={(event) =>
                    updateFilter(filter.key, { value: event.target.value })
                  }
                />
              </label>
              <button
                type="button"
                className="button search-filter-row__remove"
                aria-label="Remove filter"
                disabled={isSearching}
                onClick={() => removeFilter(filter.key)}
              >
                Remove
              </button>
            </div>
          );
        })}
        <button
          type="button"
          className="button"
          disabled={isSearching}
          onClick={addFilter}
        >
          Add filter
        </button>
        {fieldsHint ? (
          <p className="search-builder__hint" role="status">
            {fieldsHint}
          </p>
        ) : null}
      </fieldset>

      <div className="filters__field">
        <label htmlFor={facilityId}>Facility</label>
        <select
          id={facilityId}
          value={facility}
          disabled={isSearching}
          onChange={(event) => setFacility(event.target.value)}
        >
          <option value="">All facilities</option>
          {facilityOptions.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      </div>

      <div className="filters__field">
        <label htmlFor={fromId}>Time from (inclusive)</label>
        <input
          id={fromId}
          type="datetime-local"
          value={from}
          disabled={isSearching}
          onChange={(event) => setFrom(event.target.value)}
        />
      </div>
      <div className="filters__field">
        <label htmlFor={toId}>Time before (exclusive)</label>
        <input
          id={toId}
          type="datetime-local"
          value={to}
          disabled={isSearching}
          onChange={(event) => setTo(event.target.value)}
        />
      </div>

      <div className="filters__actions">
        <button
          type="submit"
          className="button button--primary"
          disabled={isSearching}
        >
          {isSearching ? 'Searching…' : 'Apply'}
        </button>
        <button
          type="button"
          className="button"
          disabled={isSearching}
          onClick={handleReset}
        >
          Reset
        </button>
      </div>
      {error ? (
        <p className="filters__error" role="alert">
          {error}
        </p>
      ) : null}
    </form>
  );
}
