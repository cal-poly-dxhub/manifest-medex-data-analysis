import { useId, useState, type FormEvent, type ReactNode } from 'react';
import { SOURCE_FORMAT_OPTIONS, type MessageFilters, type SourceFormat } from '../api/types';

interface MessageFiltersFormProps {
  readonly value: MessageFilters;
  readonly onChange: (next: MessageFilters) => void;
}

function toIso(local: string): string | undefined {
  if (!local) {
    return undefined;
  }
  const date = new Date(local);
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString();
}

function toLocalInput(iso: string | undefined): string {
  if (!iso) {
    return '';
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (n: number): string => String(n).padStart(2, '0');
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}`
  );
}

export function MessageFiltersForm({
  value,
  onChange,
}: MessageFiltersFormProps): ReactNode {
  const fromId = useId();
  const toId = useId();
  const sourceId = useId();

  const [from, setFrom] = useState(toLocalInput(value.from));
  const [to, setTo] = useState(toLocalInput(value.to));
  const [sourceFormat, setSourceFormat] = useState<SourceFormat | ''>(
    value.sourceFormat ?? '',
  );
  const [error, setError] = useState<string | undefined>(undefined);

  const handleSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    const fromTime = toIso(from);
    const toTime = toIso(to);
    if (fromTime && toTime && new Date(fromTime) > new Date(toTime)) {
      setError('Ingested from must be before ingested before.');
      return;
    }
    setError(undefined);
    onChange({
      ...(fromTime ? { from: fromTime } : {}),
      ...(toTime ? { to: toTime } : {}),
      ...(sourceFormat ? { sourceFormat } : {}),
    });
  };

  const handleReset = (): void => {
    setFrom('');
    setTo('');
    setSourceFormat('');
    setError(undefined);
    onChange({});
  };

  return (
    <form className="filters" onSubmit={handleSubmit} aria-label="Message filters">
      <div className="filters__field">
        <label htmlFor={fromId}>Ingested from (inclusive)</label>
        <input
          id={fromId}
          type="datetime-local"
          value={from}
          onChange={(event) => setFrom(event.target.value)}
        />
      </div>
      <div className="filters__field">
        <label htmlFor={toId}>Ingested before (exclusive)</label>
        <input
          id={toId}
          type="datetime-local"
          value={to}
          onChange={(event) => setTo(event.target.value)}
        />
      </div>
      <div className="filters__field">
        <label htmlFor={sourceId}>Source format</label>
        <select
          id={sourceId}
          value={sourceFormat}
          onChange={(event) =>
            setSourceFormat(event.target.value as SourceFormat | '')
          }
        >
          <option value="">All</option>
          {SOURCE_FORMAT_OPTIONS.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      </div>
      <div className="filters__actions">
        <button type="submit" className="button button--primary">
          Apply
        </button>
        <button type="button" className="button" onClick={handleReset}>
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
