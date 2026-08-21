/** Source document formats surfaced by the backend. */
export type SourceFormat = 'hl7-v2' | 'ccda';

/** Selectable source-format filter values. */
export const SOURCE_FORMAT_OPTIONS: readonly SourceFormat[] = ['hl7-v2', 'ccda'];

/** A single row in the message list. Metadata only — never a message body. */
export interface MessageSummary {
  /** Deterministic document identifier. Treated as sensitive: never logged. */
  readonly documentId: string;
  readonly sourceFormat: SourceFormat;
  /** ISO-8601 timestamp of the document's own clinical time, when available. */
  readonly documentTime: string | null;
  /** ISO-8601 timestamp of when the document was ingested. */
  readonly ingestedTime: string;
}

/** Cursor-paginated list response. Cursors are opaque, backend-defined tokens. */
export interface MessageListResponse {
  readonly items: readonly MessageSummary[];
  /** Exact number of rows matching the active filters, independent of cursor. */
  readonly totalCount: number;
  /** Cursor to fetch the next page, or null if this is the last page. */
  readonly nextCursor: string | null;
}

/** Filters applied to the message list query. */
export interface MessageFilters {
  /** Inclusive lower ingested-time bound (ISO-8601), or undefined. */
  readonly from?: string;
  /** Exclusive upper ingested-time bound (ISO-8601), or undefined. */
  readonly to?: string;
  /** Restrict to a single source format, or undefined for all. */
  readonly sourceFormat?: SourceFormat;
}

/** Which representation of a message body to fetch. */
export type BodyVariant = 'parsed' | 'raw';

/** Parsed bodies are JSON values; raw bodies are text. */
export type MessageBody = unknown;

/** Generic Data API result returned by the unrestricted SQL console. */
export interface SqlQueryResult {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly unknown[])[];
  readonly numberOfRecordsUpdated: number;
}
