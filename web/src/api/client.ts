import type {
  AddSectionResult,
  BodyVariant,
  CreateReingestJobRequest,
  ExportReportResponse,
  FacilityListResponse,
  ImportReportResult,
  MessageBody,
  MessageFilters,
  MessageDetailRecord,
  MessageListResponse,
  QueryTestRequest,
  QueryTestResult,
  ReingestJob,
  ReingestJobListResponse,
  ReingestPreviewResult,
  ReportDetailResponse,
  ReportListResponse,
  ReportRowInput,
  ReportRunListResponse,
  ReportRun,
  RowEditResult,
  SqlQueryResult,
  SearchFieldsResponse,
  SearchQuery,
  SearchResponse,
  StartReportRunRequest,
  UpdateReportResult,
} from './types';

/**
 * Error carrying the HTTP status for a failed API call. The `message` never includes a
 * response body or resource identifier. `code` is an optional short, sanitized backend
 * error code (e.g. `invalid_index`) parsed only from a strict `{ "error": string }` JSON
 * body; it is validated against a snake_case allowlist pattern so no free-form backend
 * text or identifier can leak through it.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly code?: string;
  constructor(status: number, message: string, code?: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    if (code !== undefined) {
      this.code = code;
    }
  }
}

/**
 * Matches a short, safe backend error code (snake_case, <= 64 chars). Anything that does
 * not match is discarded so no free-form backend text or identifier is ever retained.
 */
const SAFE_ERROR_CODE = /^[a-z][a-z0-9_]{0,63}$/;

/** Longest error body we will read at all; larger bodies are ignored entirely. */
const MAX_ERROR_BODY_LENGTH = 256;

/** Returns the current OIDC access token, or undefined when unauthenticated. */
export type AccessTokenProvider = () => string | undefined;

export interface ApiClientOptions {
  /** Base path for same-origin API calls, e.g. "/api". No trailing slash. */
  readonly basePath: string;
  readonly getAccessToken: AccessTokenProvider;
}

export interface ListMessagesParams extends MessageFilters {
  /** Opaque pagination cursor from a prior response. */
  readonly cursor?: string;
  /** Page size hint. */
  readonly limit?: number;
}

export class ApiClient {
  private readonly basePath: string;
  private readonly getAccessToken: AccessTokenProvider;

  constructor(options: ApiClientOptions) {
    this.basePath = options.basePath;
    this.getAccessToken = options.getAccessToken;
  }

  private buildHeaders(
    hasJsonBody: boolean,
    extraHeaders?: Readonly<Record<string, string>>,
  ): Headers {
    const headers = new Headers();
    headers.set('Accept', 'application/json');
    if (hasJsonBody) {
      headers.set('Content-Type', 'application/json');
    }
    const token = this.getAccessToken();
    if (!token) {
      throw new ApiError(401, 'Authentication is required');
    }
    headers.set('Authorization', `Bearer ${token}`);
    if (extraHeaders) {
      for (const [name, value] of Object.entries(extraHeaders)) {
        headers.set(name, value);
      }
    }
    return headers;
  }

  private async fetchResponse(
    path: string,
    init: RequestInit,
    hasJsonBody: boolean,
    extraHeaders?: Readonly<Record<string, string>>,
  ): Promise<Response> {
    const response = await fetch(`${this.basePath}${path}`, {
      ...init,
      headers: this.buildHeaders(hasJsonBody, extraHeaders),
      credentials: 'same-origin',
    });
    if (!response.ok) {
      // Never include response content or resource identifiers in the message. Only a
      // strictly-validated short code (if any) is retained, on ApiError.code.
      const code = await this.extractErrorCode(response);
      throw new ApiError(
        response.status,
        `Request failed with status ${response.status}`,
        code,
      );
    }
    return response;
  }

  /**
   * Best-effort parse of a sanitized backend error code from a failed response. Reads the
   * body only when it is short JSON, accepts only an object shaped exactly like
   * `{ "error": string }`, and returns the string only when it matches the safe
   * snake_case code pattern. Any other shape, oversized body, or parse failure yields
   * `undefined` so no response body or identifier is ever surfaced. Never throws.
   */
  private async extractErrorCode(response: Response): Promise<string | undefined> {
    try {
      const contentType = response.headers.get('content-type') ?? '';
      if (!contentType.toLowerCase().includes('application/json')) {
        return undefined;
      }
      const text = await response.text();
      if (text.length === 0 || text.length > MAX_ERROR_BODY_LENGTH) {
        return undefined;
      }
      const parsed: unknown = JSON.parse(text);
      if (typeof parsed !== 'object' || parsed === null) {
        return undefined;
      }
      const code = (parsed as Record<string, unknown>).error;
      if (typeof code !== 'string' || !SAFE_ERROR_CODE.test(code)) {
        return undefined;
      }
      return code;
    } catch {
      // A missing, non-JSON, oversized, or unparseable body must never leak; swallow it.
      return undefined;
    }
  }

  /** Fetch a page of message metadata. */
  async listMessages(
    params: ListMessagesParams,
    signal?: AbortSignal,
  ): Promise<MessageListResponse> {
    const search = new URLSearchParams();
    if (params.from) {
      search.set('from', params.from);
    }
    if (params.to) {
      search.set('to', params.to);
    }
    if (params.sourceFormat) {
      search.set('source_format', params.sourceFormat);
    }
    if (params.cursor) {
      search.set('cursor', params.cursor);
    }
    if (typeof params.limit === 'number') {
      search.set('limit', String(params.limit));
    }
    const query = search.toString();
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/messages${query ? `?${query}` : ''}`,
      init,
      false,
    );
    return (await response.json()) as MessageListResponse;
  }

  /** Fetch full metadata for one selected document. */
  async getMessage(id: string, signal?: AbortSignal): Promise<MessageDetailRecord> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/messages/${encodeURIComponent(id)}`,
      init,
      false,
    );
    return (await response.json()) as MessageDetailRecord;
  }

  /** Fetch a body lazily. The direct response is JSON for parsed or text for raw. */
  async fetchMessageBody(
    id: string,
    variant: BodyVariant,
    signal?: AbortSignal,
  ): Promise<MessageBody> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify({ variant }),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/messages/${encodeURIComponent(id)}/body`,
      init,
      true,
    );
    return variant === 'parsed' ? response.json() : response.text();
  }

  /** Execute one unrestricted SQL statement through the authenticated backend. */
  async executeSql(sql: string, signal?: AbortSignal): Promise<SqlQueryResult> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify({ sql }),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/query', init, true);
    return (await response.json()) as SqlQueryResult;
  }

  /**
   * List the searchable leaf field names for one index. Used to populate the filter
   * builder's field autocomplete; the backend validates that `index` names one of the two
   * searchable indexes and returns its exact `_field_caps` leaf names.
   */
  async searchFields(
    index: string,
    signal?: AbortSignal,
  ): Promise<SearchFieldsResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/search/fields?index=${encodeURIComponent(index)}`,
      init,
      false,
    );
    return (await response.json()) as SearchFieldsResponse;
  }

  /**
   * Run one metadata-only attribute search. The request body is the typed, bounded
   * {@link SearchQuery} shape (never raw DSL); the response carries only allowlisted
   * metadata hits, a bounded total, and an opaque next cursor.
   */
  async search(request: SearchQuery, signal?: AbortSignal): Promise<SearchResponse> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(request),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/search', init, true);
    return (await response.json()) as SearchResponse;
  }

  /**
   * Preview a candidate reingestion selection, returning only the exact distinct-document
   * count the guarded SELECT resolves to. Nothing is persisted and no planner is invoked.
   */
  async previewReingest(
    sql: string,
    signal?: AbortSignal,
  ): Promise<ReingestPreviewResult> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify({ sql }),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/reingest/preview', init, true);
    return (await response.json()) as ReingestPreviewResult;
  }

  /**
   * Create one queued reingestion job from exactly one of a guarded SELECT or an explicit
   * document-id list, and return its initial public projection. The backend enforces the
   * exactly-one-of rule and every selection guard.
   */
  async createReingestJob(
    request: CreateReingestJobRequest,
    signal?: AbortSignal,
  ): Promise<ReingestJob> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(request),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/reingest/jobs', init, true);
    return (await response.json()) as ReingestJob;
  }

  /** List reingestion jobs newest-first, up to an optional bounded page size. */
  async listReingestJobs(
    limit?: number,
    signal?: AbortSignal,
  ): Promise<ReingestJobListResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const query = typeof limit === 'number' ? `?limit=${encodeURIComponent(limit)}` : '';
    const response = await this.fetchResponse(`/reingest/jobs${query}`, init, false);
    return (await response.json()) as ReingestJobListResponse;
  }

  /** Fetch one reingestion job's public projection by job id. */
  async getReingestJob(jobId: string, signal?: AbortSignal): Promise<ReingestJob> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reingest/jobs/${encodeURIComponent(jobId)}`,
      init,
      false,
    );
    return (await response.json()) as ReingestJob;
  }

  /** List every report definition in the catalog (listing metadata only). */
  async listReports(signal?: AbortSignal): Promise<ReportListResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/reports', init, false);
    return (await response.json()) as ReportListResponse;
  }

  /** Fetch one report's full definition plus its optimistic-lock ETag. */
  async getReport(
    reportId: string,
    signal?: AbortSignal,
  ): Promise<ReportDetailResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}`,
      init,
      false,
    );
    return (await response.json()) as ReportDetailResponse;
  }

  /**
   * Import a whole definition as an atomically-published set of catalog items. The
   * canonical definition JSON is sent verbatim in the body; the backend validates it,
   * assigns storage sequences, and rejects a duplicate report id as a 409 conflict.
   */
  async importReport(
    definitionText: string,
    signal?: AbortSignal,
  ): Promise<ImportReportResult> {
    const init: RequestInit = {
      method: 'POST',
      body: definitionText,
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/reports/import', init, true);
    return (await response.json()) as ImportReportResult;
  }

  /** Fetch the clean, canonical export of one report definition plus its JSON text. */
  async exportReport(
    reportId: string,
    signal?: AbortSignal,
  ): Promise<ExportReportResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/export`,
      init,
      false,
    );
    return (await response.json()) as ExportReportResponse;
  }

  /**
   * Replace one report's whole definition atomically, guarding on its `updatedAt` lock.
   * The parsed definition object and the report's current lock token are sent together;
   * a concurrent change surfaces as a 409 conflict rather than a lost update. The caller
   * is responsible for having validated the definition's shape and report id beforehand.
   */
  async updateReport(
    reportId: string,
    definition: Record<string, unknown>,
    updatedAt: string,
    signal?: AbortSignal,
  ): Promise<UpdateReportResult> {
    const init: RequestInit = {
      method: 'PUT',
      body: JSON.stringify({ definition, updated_at: updatedAt }),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}`,
      init,
      true,
    );
    return (await response.json()) as UpdateReportResult;
  }

  /** Permanently delete one report definition and all of its catalog items. */
  async deleteReport(reportId: string, signal?: AbortSignal): Promise<void> {
    const init: RequestInit = { method: 'DELETE' };
    if (signal) {
      init.signal = signal;
    }
    await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}`,
      init,
      false,
    );
  }

  /**
   * Overwrite one row, guarding on its `updatedAt` lock. A concurrent edit surfaces as a
   * 409 conflict rather than a lost update. The row body carries the whole row schema.
   */
  async updateRow(
    reportId: string,
    sectionStorageSeq: number,
    rowStorageSeq: number,
    row: ReportRowInput,
    updatedAt: string,
    signal?: AbortSignal,
  ): Promise<RowEditResult> {
    const init: RequestInit = {
      method: 'PUT',
      body: JSON.stringify({ row, updated_at: updatedAt }),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/sections/${sectionStorageSeq}/rows/${rowStorageSeq}`,
      init,
      true,
    );
    return (await response.json()) as RowEditResult;
  }

  /**
   * Insert a new row into a section at a gapped storage sequence. With no
   * `afterStorageSeq` the row is appended after the section's last row; otherwise it is
   * placed at the midpoint of the gap following that address.
   */
  async addRow(
    reportId: string,
    sectionStorageSeq: number,
    row: ReportRowInput,
    afterStorageSeq: number | undefined,
    signal?: AbortSignal,
  ): Promise<RowEditResult> {
    const payload: Record<string, unknown> = { row };
    if (typeof afterStorageSeq === 'number') {
      payload.after_seq = afterStorageSeq;
    }
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(payload),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/sections/${sectionStorageSeq}/rows`,
      init,
      true,
    );
    return (await response.json()) as RowEditResult;
  }

  /** Conditionally delete one row, guarding on its `updatedAt` lock. */
  async deleteRow(
    reportId: string,
    sectionStorageSeq: number,
    rowStorageSeq: number,
    updatedAt: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const init: RequestInit = {
      method: 'DELETE',
      body: JSON.stringify({ updated_at: updatedAt }),
    };
    if (signal) {
      init.signal = signal;
    }
    await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/sections/${sectionStorageSeq}/rows/${rowStorageSeq}`,
      init,
      true,
    );
  }

  /**
   * Insert a new section header at a gapped storage sequence. With no `afterStorageSeq`
   * the section is appended after the last section.
   */
  async addSection(
    reportId: string,
    name: string,
    seq: number,
    afterStorageSeq: number | undefined,
    signal?: AbortSignal,
  ): Promise<AddSectionResult> {
    const payload: Record<string, unknown> = { name, seq };
    if (typeof afterStorageSeq === 'number') {
      payload.after_seq = afterStorageSeq;
    }
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(payload),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/sections`,
      init,
      true,
    );
    return (await response.json()) as AddSectionResult;
  }

  /**
   * Dry-run count a candidate row query against one index, with an optional facility and
   * an optional paired time window. Returns only a total-hits count; nothing is persisted.
   */
  async testQuery(
    request: QueryTestRequest,
    signal?: AbortSignal,
  ): Promise<QueryTestResult> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(request),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/query-test', init, true);
    return (await response.json()) as QueryTestResult;
  }

  /** List the exact facility identifiers eligible for report scoping. */
  async listFacilities(signal?: AbortSignal): Promise<FacilityListResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse('/facilities', init, false);
    return (await response.json()) as FacilityListResponse;
  }

  /** List the runs recorded for one report, newest first. */
  async listReportRuns(
    reportId: string,
    signal?: AbortSignal,
  ): Promise<ReportRunListResponse> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/runs`,
      init,
      false,
    );
    return (await response.json()) as ReportRunListResponse;
  }

  /** Start a new report run and return its initial running projection. */
  async startReportRun(
    reportId: string,
    request: StartReportRunRequest,
    signal?: AbortSignal,
  ): Promise<ReportRun> {
    const init: RequestInit = {
      method: 'POST',
      body: JSON.stringify(request),
    };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/reports/${encodeURIComponent(reportId)}/runs`,
      init,
      true,
    );
    return (await response.json()) as ReportRun;
  }

  /**
   * Download one completed run's archive as a Blob using the authenticated
   * bearer token. No presigned URL is used, so the token never leaves the
   * request headers and the response bytes are handed straight to the browser.
   */
  async downloadReportRunOutput(
    runId: string,
    signal?: AbortSignal,
  ): Promise<Blob> {
    const init: RequestInit = { method: 'GET' };
    if (signal) {
      init.signal = signal;
    }
    const response = await this.fetchResponse(
      `/runs/${encodeURIComponent(runId)}/download`,
      init,
      false,
    );
    return await response.blob();
  }
}
