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


/** Full metadata response used before loading raw or parsed message content. */
export interface MessageDetailRecord extends MessageSummary {
  readonly rawS3Uri: string;
  readonly rawVersionId: string | null;
  readonly parsedS3Uri: string;
  readonly parsedVersionId: string | null;
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

/* ---- Reports ---- */

/** Lightweight catalog listing entry for one report definition. */
export interface ReportSummary {
  /** Stable, URL- and key-safe report identifier. */
  readonly reportId: string;
  readonly name: string;
  readonly description: string;
  /** ISO-8601 timestamp of the last catalog change, when known. */
  readonly updatedAt?: string | null;
  /** Subject identifier of the last editor, when known. */
  readonly updatedBy?: string | null;
}

/** GET /reports response envelope. */
export interface ReportListResponse {
  readonly items: readonly ReportSummary[];
}

/** One counted row within a report section. Read-only in the UI. */
export interface ReportDefinitionRow {
  readonly seq: number;
  readonly label: string;
  readonly description: string;
  readonly index: string;
  /**
   * Opaque OpenSearch query clause, edited as JSON in the row editor panel. A `null`
   * query marks a placeholder row: it holds its grid position and label but has no
   * query yet, is skipped by runs, and always renders a blank count.
   */
  readonly query: Record<string, unknown> | null;
}

/** A named, ordered group of rows. */
export interface ReportDefinitionSection {
  readonly seq: number;
  readonly name: string;
  readonly rows: readonly ReportDefinitionRow[];
}

/** The full report definition document (source of truth) for one report. */
export interface ReportDefinition {
  readonly report_id: string;
  readonly name: string;
  readonly description: string;
  readonly partition_field: string;
  readonly time_field: string;
  readonly sections: readonly ReportDefinitionSection[];
}

/**
 * Editor metadata for one row: its storage address and optimistic-lock token.
 * Kept separate from the clean definition body so row edits never leak addresses
 * or locks into the definition itself.
 */
export interface ReportEditorRow {
  /** Catalog-allocated ordering/address key for the row within its section. */
  readonly storageSeq: number;
  /** The row's own definition seq (display order), stored verbatim. */
  readonly seq: number;
  readonly label: string;
  /** Optimistic-lock token; the precondition for a conditional row write. */
  readonly updatedAt: string | null;
  readonly updatedBy: string | null;
}

/** Editor metadata for one section: its storage address and rows. */
export interface ReportEditorSection {
  readonly storageSeq: number;
  readonly seq: number;
  readonly name: string;
  readonly rows: readonly ReportEditorRow[];
}

/** Editor projection returned alongside the clean definition. Index-aligned with it. */
export interface ReportEditor {
  readonly reportId: string;
  readonly draft: boolean;
  readonly updatedAt: string | null;
  readonly updatedBy: string | null;
  readonly sections: readonly ReportEditorSection[];
}

/** GET /reports/{reportId} response: the clean definition plus separate editor metadata. */
export interface ReportDetailResponse {
  readonly reportId: string;
  readonly definition: ReportDefinition;
  readonly editor: ReportEditor;
}

/** The allowed OpenSearch indexes a report row query may target. */
export const REPORT_ROW_INDEXES = ['hl7-messages-v1', 'ccda-documents-v1'] as const;
export type ReportRowIndex = (typeof REPORT_ROW_INDEXES)[number];

/** Editable row payload sent on add/update and reused by the query tester. */
export interface ReportRowInput {
  readonly seq: number;
  readonly label: string;
  readonly description: string;
  readonly index: string;
  /** The row query, or `null` to save the row as a placeholder with no query yet. */
  readonly query: Record<string, unknown> | null;
}

/** Result of a row add or update: the row's storage address and new lock token. */
export interface RowEditResult {
  readonly reportId: string;
  readonly sectionStorageSeq: number;
  readonly rowStorageSeq: number;
  readonly updatedAt: string;
  readonly updatedBy: string;
}

/** Result of a section add: the allocated section storage address. */
export interface AddSectionResult {
  readonly reportId: string;
  readonly sectionStorageSeq: number;
}

/** Result of an atomic report import. */
export interface ImportReportResult {
  readonly reportId: string;
  readonly name: string;
  readonly description: string;
  readonly updatedAt: string;
  readonly updatedBy: string;
}

/**
 * Result of an atomic full-definition replace (PUT /reports/{reportId}). Carries the
 * report's new optimistic-lock token so a follow-up edit can guard on the fresh value.
 */
export interface UpdateReportResult {
  readonly reportId: string;
  readonly name: string;
  readonly description: string;
  readonly updatedAt: string;
  readonly updatedBy: string;
}

/** GET /reports/{reportId}/export response: the clean definition and its canonical text. */
export interface ExportReportResponse {
  readonly reportId: string;
  readonly definition: ReportDefinition;
  readonly text: string;
}

/** POST /query-test request: a candidate row query plus optional facility/time scope. */
export interface QueryTestRequest {
  readonly index: string;
  readonly query: Record<string, unknown>;
  /** Exact facility identifier from the directory, never free text. */
  readonly facility?: string;
  /** Inclusive lower bound (ISO-8601). Must be paired with `to`. */
  readonly from?: string;
  /** Exclusive upper bound (ISO-8601). Must be paired with `from`. */
  readonly to?: string;
}

/** POST /query-test response: only a total-hits count is ever returned. */
export interface QueryTestResult {
  readonly count: number;
}

/** GET /facilities response: exact keyword identifiers, never free text. */
export interface FacilityListResponse {
  readonly facilities: readonly string[];
}

/** Lifecycle status shared with the report worker. */
export type ReportRunStatus = 'running' | 'complete' | 'failed';

/** The set of statuses that warrant continued polling. */
export const ACTIVE_RUN_STATUSES: ReadonlySet<ReportRunStatus> = new Set(['running']);

/** Public projection of one report run. Never carries clinical content. */
export interface ReportRun {
  readonly runId: string;
  readonly reportId: string;
  readonly status: ReportRunStatus;
  readonly requestedBy: string;
  readonly startedAt: string;
  readonly params: {
    readonly from: string;
    readonly to: string;
    readonly partitionValues: readonly string[];
  };
  readonly progress: {
    readonly completedPartitions: number;
    readonly totalPartitions: number;
  };
  readonly downloadReady: boolean;
  readonly finishedAt?: string;
  /** Present on a failed run: the partition value whose count failed. */
  readonly failingPartition?: string;
  /**
   * Aggregate counts keyed by row identity (`S<section seq>:R<row seq>`), so two sections
   * may reuse the same row label without collision. Present only on complete runs (when
   * persisted); consumed by the grid to fill count cells. A `null` value marks a
   * placeholder row that was skipped rather than counted. Runs recorded before the
   * identity-keyed worker may instead be keyed by row label; the grid falls back to the
   * label when the identity key is absent.
   */
  readonly rowCounts?: Readonly<Record<string, number | null>>;
  /**
   * Optional run-level tally. When present, `rowsExecuted` counts the rows whose
   * queries actually ran and `placeholdersSkipped` counts placeholder rows skipped.
   */
  readonly summary?: {
    readonly rowsExecuted?: number;
    readonly placeholdersSkipped?: number;
  };
}

/** GET /reports/{reportId}/runs response envelope. */
export interface ReportRunListResponse {
  readonly items: readonly ReportRun[];
}

/** Parameters for starting a new report run. */
export interface StartReportRunRequest {
  /** Inclusive lower bound as an ISO-8601 timestamp with timezone offset. */
  readonly from: string;
  /** Exclusive upper bound as an ISO-8601 timestamp with timezone offset. */
  readonly to: string;
  /**
   * Unique facility identifiers to scope the run. May come from the directory
   * selection and/or manually-entered UIDs. The UI allows 1..200 entries.
   */
  readonly partitions: readonly string[];
}

/* ---- Attribute search ---- */

/**
 * The two clinical indexes the metadata attribute search can target. Labels are written
 * for non-technical users; `sourceFormat` mirrors the value each index stamps on its hits
 * and `timeField` names the per-index clinical time key surfaced in results.
 */
export const SEARCH_INDEXES = [
  {
    id: 'hl7-messages-v1',
    label: 'HL7 v2 messages',
    sourceFormat: 'hl7-v2',
    timeField: 'messageTime',
  },
  {
    id: 'ccda-documents-v1',
    label: 'C-CDA documents',
    sourceFormat: 'ccda',
    timeField: 'documentTime',
  },
] as const;

/** One of the two exact searchable index identifiers. */
export type SearchIndex = (typeof SEARCH_INDEXES)[number]['id'];

/** The bounded set of attribute-filter operators the backend accepts. */
export const SEARCH_OPERATORS = [
  { id: 'equals', label: 'Equals', needsValue: true },
  { id: 'prefix', label: 'Starts with', needsValue: true },
  { id: 'contains', label: 'Contains', needsValue: true },
  { id: 'exists', label: 'Has any value', needsValue: false },
] as const;

/** One attribute-filter operator identifier. */
export type SearchOperator = (typeof SEARCH_OPERATORS)[number]['id'];

/** Minimum length the backend enforces for a `contains` value. */
export const SEARCH_CONTAINS_MIN_LENGTH = 3;

/** Fixed page size for every attribute search request. */
export const SEARCH_PAGE_SIZE = 25;

/**
 * A single typed attribute filter. `value` is omitted for the `exists` operator (the
 * backend rejects a value paired with `exists`) and required for all other operators.
 */
export interface SearchFilter {
  readonly field: string;
  readonly operator: SearchOperator;
  readonly value?: string;
}

/**
 * A fully-formed, validated attribute-search request body (without a cursor). Optional
 * scope keys are present only when set; all filters combine with AND semantics. No raw
 * OpenSearch DSL is ever sent — only this typed, bounded shape.
 */
export interface SearchRequest {
  readonly index: SearchIndex;
  readonly filters: readonly SearchFilter[];
  /** Exact facility identifier from the directory, never free text. */
  readonly facility?: string;
  /** Inclusive lower bound (ISO-8601). Always paired with `to`. */
  readonly from?: string;
  /** Exclusive upper bound (ISO-8601). Always paired with `from`. */
  readonly to?: string;
  readonly limit: number;
}

/** A search request plus an opaque, index-bound pagination cursor. */
export interface SearchQuery extends SearchRequest {
  /** Opaque backend cursor from a prior response's `nextCursor`. */
  readonly cursor?: string;
}

/**
 * One metadata-only search hit. Every field is optional because the backend copies only
 * the allowlisted keys that are present on a document; no clinical narrative value is ever
 * included. Exactly one of `messageTime` / `documentTime` is populated per index.
 */
export interface SearchHit {
  readonly documentId: string;
  readonly sourceFormat?: SourceFormat;
  readonly sourceFacilityId?: string;
  readonly messageType?: string;
  readonly triggerEvent?: string;
  /** HL7 clinical message time (ISO-8601). */
  readonly messageTime?: string;
  /** C-CDA clinical document time (ISO-8601). */
  readonly documentTime?: string;
  readonly ingestTime?: string;
}

/** POST /search response: metadata hits, a bounded total, and an opaque next cursor. */
export interface SearchResponse {
  readonly items: readonly SearchHit[];
  /** Bounded total-hits count for the active filters, independent of the cursor. */
  readonly total: number;
  /** Opaque cursor to fetch the next page, or null when this page is terminal. */
  readonly nextCursor: string | null;
}

/** GET /search/fields response: the searchable leaf field names for one index. */
export interface SearchFieldsResponse {
  readonly fields: readonly string[];
}

/* ---- Parsed-zone reingestion ---- */

/** Lifecycle status shared with the reingest planner and reindexer backend. */
export type ReingestJobStatus = 'queued' | 'running' | 'complete' | 'failed';

/** Statuses that warrant continued polling of the jobs list. */
export const ACTIVE_REINGEST_STATUSES: ReadonlySet<ReingestJobStatus> = new Set([
  'queued',
  'running',
]);

/** How a job selected its documents: a guarded SELECT or an explicit id list. */
export type ReingestJobMode = 'sql' | 'ids';

/** Atomic per-job counters the planner and reindexer maintain. */
export interface ReingestJobCounters {
  readonly enqueued: number;
  readonly reindexed: number;
  readonly reindexedStaleParser: number;
  readonly missingParsed: number;
  readonly failed: number;
}

/**
 * Public projection of one reingestion job returned by the JWT-protected routes. `sql`
 * and `sqlSha256` are present only for a SQL job; `idCount` only for an id job. The
 * verbatim `sql` is surfaced so the UI can expand a job and show exactly what was run.
 */
export interface ReingestJob {
  readonly jobId: string;
  readonly status: ReingestJobStatus;
  readonly mode: ReingestJobMode;
  readonly requestedBy: string;
  readonly createdAt: string;
  readonly expected: number;
  readonly enqueueComplete: boolean;
  readonly counters: ReingestJobCounters;
  readonly startedAt?: string;
  readonly finishedAt?: string;
  readonly sqlSha256?: string;
  readonly sql?: string;
  readonly idCount?: number;
}

/** GET /reingest/jobs response envelope. */
export interface ReingestJobListResponse {
  readonly items: readonly ReingestJob[];
}

/** POST /reingest/preview response: the exact distinct-document count for a selection. */
export interface ReingestPreviewResult {
  readonly count: number;
}

/** POST /reingest/jobs request body: exactly one of `sql` or `documentIds`. */
export interface CreateReingestJobRequest {
  readonly sql?: string;
  readonly documentIds?: readonly string[];
}

/**
 * The upper bound the backend enforces on a single reingestion selection. A confirmation
 * whose count is 0 or exceeds this is rejected before any job is created.
 */
export const REINGEST_MAX_SELECTION = 100_000;
