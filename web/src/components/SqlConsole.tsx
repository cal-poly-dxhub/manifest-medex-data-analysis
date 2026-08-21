import { useMemo, useState, type FormEvent, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useSqlQuery } from '../api/queries';

interface SavedQuery {
  readonly id: string;
  readonly name: string;
  readonly sql: string;
}

const STORAGE_KEY = 'phi-explorer.saved-sql.v1';
const MAX_SAVED_QUERIES = 50;
const DEFAULT_SQL = `SELECT
  document_id,
  source_format,
  document_time,
  ingested_time
FROM document_metadata
ORDER BY ingested_time DESC, document_id DESC
LIMIT 50;`;

function readSavedQueries(): SavedQuery[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]');
    if (!Array.isArray(value)) {
      return [];
    }
    return value
      .filter(
        (item): item is SavedQuery =>
          typeof item === 'object' &&
          item !== null &&
          typeof (item as Record<string, unknown>)['id'] === 'string' &&
          typeof (item as Record<string, unknown>)['name'] === 'string' &&
          typeof (item as Record<string, unknown>)['sql'] === 'string',
      )
      .slice(0, MAX_SAVED_QUERIES);
  } catch {
    return [];
  }
}

function formatCell(value: unknown): string {
  if (value === null) {
    return 'NULL';
  }
  if (typeof value === 'string') {
    return value;
  }
  if (
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'bigint'
  ) {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return '[Unrenderable value]';
  }
}

function queryErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to run SQL queries.';
    }
    if (error.status === 413) {
      return 'The SQL result exceeds the response-size limit.';
    }
  }
  return 'The SQL query failed. Check the statement and try again.';
}

export function SqlConsole(): ReactNode {
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [queryName, setQueryName] = useState('');
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(readSavedQueries);
  const [selectedId, setSelectedId] = useState('');
  const [storageError, setStorageError] = useState<string | undefined>(undefined);
  const query = useSqlQuery();

  const selected = useMemo(
    () => savedQueries.find((item) => item.id === selectedId),
    [savedQueries, selectedId],
  );

  const persist = (next: SavedQuery[]): boolean => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      setSavedQueries(next);
      setStorageError(undefined);
      return true;
    } catch {
      setStorageError('Unable to save queries in this browser.');
      return false;
    }
  };

  const handleRun = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (!sql.trim() || query.isPending) {
      return;
    }
    query.mutate(sql);
  };

  const handleSave = (): void => {
    const name = queryName.trim();
    if (!name || !sql.trim()) {
      setStorageError('Enter a query name and SQL before saving.');
      return;
    }
    const saved: SavedQuery = {
      id: crypto.randomUUID(),
      name,
      sql,
    };
    const next = [
      saved,
      ...savedQueries.filter((item) => item.name.toLowerCase() !== name.toLowerCase()),
    ].slice(0, MAX_SAVED_QUERIES);
    if (persist(next)) {
      setSelectedId(saved.id);
      setQueryName('');
    }
  };

  const handleLoad = (): void => {
    if (selected) {
      setSql(selected.sql);
      setQueryName(selected.name);
      query.reset();
    }
  };

  const handleDelete = (): void => {
    if (!selected) {
      return;
    }
    if (persist(savedQueries.filter((item) => item.id !== selected.id))) {
      setSelectedId('');
    }
  };

  return (
    <section className="sql-console" aria-label="SQL query console">
      <div className="sql-console__warning" role="note">
        SQL runs with the explorer database credential and is unrestricted. Saved queries are
        stored only in this browser; do not save clinical values or identifiers on shared devices.
      </div>

      <div className="saved-query-controls">
        <label>
          Saved query
          <select value={selectedId} onChange={(event) => setSelectedId(event.target.value)}>
            <option value="">Select a saved query</option>
            {savedQueries.map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <button type="button" className="button" disabled={!selected} onClick={handleLoad}>
          Load
        </button>
        <button type="button" className="button" disabled={!selected} onClick={handleDelete}>
          Delete
        </button>
      </div>

      <form onSubmit={handleRun}>
        <label className="sql-console__editor-label" htmlFor="sql-query-editor">
          SQL query
        </label>
        <textarea
          id="sql-query-editor"
          className="sql-console__editor"
          value={sql}
          spellCheck={false}
          onChange={(event) => setSql(event.target.value)}
        />
        <div className="sql-console__actions">
          <button
            type="submit"
            className="button button--primary"
            disabled={query.isPending || !sql.trim()}
          >
            {query.isPending ? (
              <>
                <span className="spinner spinner--button" aria-hidden="true" />
                Running…
              </>
            ) : (
              'Run query'
            )}
          </button>
          <input
            aria-label="Saved query name"
            placeholder="Query name"
            value={queryName}
            onChange={(event) => setQueryName(event.target.value)}
          />
          <button type="button" className="button" onClick={handleSave}>
            Save query
          </button>
        </div>
      </form>

      {storageError ? (
        <p className="sql-console__error" role="alert">
          {storageError}
        </p>
      ) : null}
      {query.isError ? (
        <p className="sql-console__error" role="alert">
          {queryErrorMessage(query.error)}
        </p>
      ) : null}
      {query.isPending ? (
        <div className="sql-console__running" role="status" aria-live="polite">
          <span className="spinner" aria-hidden="true" />
          Running SQL query…
        </div>
      ) : null}

      {query.data ? (
        <div className="sql-results" aria-live="polite">
          <p className="sql-results__summary">
            {query.data.rows.length} row(s); {query.data.numberOfRecordsUpdated} record(s)
            updated
          </p>
          {query.data.columns.length > 0 ? (
            <div className="table-wrap" role="region" aria-label="SQL query results" tabIndex={0}>
              <table className="table sql-results__table">
                <thead>
                  <tr>
                    {query.data.columns.map((column, index) => (
                      <th key={`${column}-${index}`} scope="col">
                        {column}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {query.data.rows.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {row.map((value, columnIndex) => (
                        <td key={columnIndex} className="sql-results__cell">
                          {formatCell(value)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
