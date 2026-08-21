/** Formats an ISO-8601 timestamp for display without exposing invalid internals. */
export function formatTimestamp(iso: string | null): string {
  if (iso === null) {
    return '—';
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return 'Invalid timestamp';
  }
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
