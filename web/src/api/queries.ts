import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from '@tanstack/react-query';
import { useApiClient } from './ApiClientContext';
import {
  ACTIVE_REINGEST_STATUSES,
  ACTIVE_RUN_STATUSES,
  type AddSectionResult,
  type BodyVariant,
  type CreateReingestJobRequest,
  type ExportReportResponse,
  type FacilityListResponse,
  type ImportReportResult,
  type MessageBody,
  type MessageDetailRecord,
  type MessageFilters,
  type MessageListResponse,
  type QueryTestRequest,
  type QueryTestResult,
  type ReingestJob,
  type ReingestJobListResponse,
  type ReingestPreviewResult,
  type ReportDetailResponse,
  type ReportListResponse,
  type ReportRowInput,
  type ReportRun,
  type ReportRunListResponse,
  type RowEditResult,
  type SqlQueryResult,
  type SearchFieldsResponse,
  type SearchQuery,
  type SearchRequest,
  type SearchResponse,
  type StartReportRunRequest,
  type UpdateReportResult,
} from './types';

export const MESSAGE_PAGE_SIZE = 50;

const queryKeys = {
  messages: (filters: MessageFilters, cursor: string | undefined) =>
    ['messages', filters, cursor ?? null] as const,
  detail: (id: string) => ['message-detail', id] as const,
  body: (id: string, variant: BodyVariant) =>
    ['message-body', id, variant] as const,
  reports: () => ['reports'] as const,
  reportDetail: (id: string) => ['report-detail', id] as const,
  facilities: () => ['facilities'] as const,
  reportRuns: (id: string) => ['report-runs', id] as const,
  searchFields: (index: string) => ['search-fields', index] as const,
  search: (request: SearchRequest, cursor: string | undefined) =>
    ['search', request, cursor ?? null] as const,
  reingestJobs: () => ['reingest-jobs'] as const,
};

/** Poll interval, in milliseconds, while a reingestion job is still active. */
export const REINGEST_JOB_POLL_INTERVAL_MS = 4_000;

/** Page size requested when listing reingestion jobs (GET /reingest/jobs?limit=50). */
export const REINGEST_JOBS_LIST_LIMIT = 50;

/** Poll interval, in milliseconds, while a report run is still active. */
export const REPORT_RUN_POLL_INTERVAL_MS = 4_000;

export function useMessageList(
  filters: MessageFilters,
  cursor: string | undefined,
): UseQueryResult<MessageListResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.messages(filters, cursor),
    queryFn: ({ signal }) =>
      client.listMessages(
        { ...filters, ...(cursor ? { cursor } : {}), limit: MESSAGE_PAGE_SIZE },
        signal,
      ),
    staleTime: 30_000,
  });
}

export function useMessageDetail(
  id: string | undefined,
): UseQueryResult<MessageDetailRecord> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.detail(id ?? ''),
    queryFn: ({ signal }) => client.getMessage(id as string, signal),
    enabled: typeof id === 'string' && id.length > 0,
    staleTime: 30_000,
  });
}

/** Fetches a message body only after its active tab is rendered. */
export function useMessageBody(
  id: string | undefined,
  variant: BodyVariant,
  enabled: boolean,
): UseQueryResult<MessageBody> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.body(id ?? '', variant),
    queryFn: ({ signal }) => client.fetchMessageBody(id as string, variant, signal),
    enabled: enabled && typeof id === 'string' && id.length > 0,
    staleTime: 0,
    // Bodies contain PHI; discard inactive cache entries promptly.
    gcTime: 10_000,
  });
}


export function useSqlQuery(): UseMutationResult<SqlQueryResult, Error, string> {
  const client = useApiClient();
  return useMutation({
    mutationFn: (sql: string) => client.executeSql(sql),
  });
}

/* ---- Parsed-zone reingestion ---- */

/** Preview the exact distinct-document count a candidate reingestion SELECT resolves to. */
export function useReingestPreview(): UseMutationResult<
  ReingestPreviewResult,
  Error,
  string
> {
  const client = useApiClient();
  return useMutation({
    mutationFn: (sql: string) => client.previewReingest(sql),
  });
}

/**
 * Create one reingestion job. On success the jobs list is invalidated so the newly queued
 * job appears immediately and polling picks up its progress.
 */
export function useCreateReingestJob(): UseMutationResult<
  ReingestJob,
  Error,
  CreateReingestJobRequest
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (request: CreateReingestJobRequest) => client.createReingestJob(request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.reingestJobs() });
    },
  });
}

/** Lists reingestion jobs newest-first and polls while any job remains active. */
export function useReingestJobs(
  enabled: boolean,
): UseQueryResult<ReingestJobListResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.reingestJobs(),
    queryFn: ({ signal }) => client.listReingestJobs(REINGEST_JOBS_LIST_LIMIT, signal),
    enabled,
    staleTime: 0,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) {
        return false;
      }
      const hasActive = data.items.some((job) =>
        ACTIVE_REINGEST_STATUSES.has(job.status),
      );
      return hasActive ? REINGEST_JOB_POLL_INTERVAL_MS : false;
    },
  });
}

/* ---- Attribute search ---- */

/**
 * Loads the searchable field names for one index, used by the filter builder's field
 * autocomplete. Disabled until an index is chosen; field lists are stable, so results are
 * cached for several minutes.
 */
export function useSearchFields(
  index: string | undefined,
): UseQueryResult<SearchFieldsResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.searchFields(index ?? ''),
    queryFn: ({ signal }) => client.searchFields(index as string, signal),
    enabled: typeof index === 'string' && index.length > 0,
    staleTime: 5 * 60_000,
  });
}

/**
 * Runs a metadata attribute search for an applied request and cursor. The query is
 * disabled until a request has been applied, and each distinct request/cursor pair is a
 * separate cache entry so cursor navigation reuses discovered pages. Hits carry only
 * metadata, but entries are still discarded promptly to keep the cache small.
 */
export function useSearch(
  request: SearchRequest | undefined,
  cursor: string | undefined,
): UseQueryResult<SearchResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.search(request ?? EMPTY_SEARCH_REQUEST, cursor),
    queryFn: ({ signal }) => {
      const query: SearchQuery = {
        ...(request as SearchRequest),
        ...(cursor ? { cursor } : {}),
      };
      return client.search(query, signal);
    },
    enabled: request !== undefined,
    staleTime: 0,
    gcTime: 10_000,
  });
}

/** Stable placeholder key used only while no search request has been applied. */
const EMPTY_SEARCH_REQUEST: SearchRequest = {
  index: 'hl7-messages-v1',
  filters: [],
  limit: 0,
};

/* ---- Reports ---- */

export function useReportList(): UseQueryResult<ReportListResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.reports(),
    queryFn: ({ signal }) => client.listReports(signal),
    staleTime: 30_000,
  });
}

export function useReportDetail(
  reportId: string | undefined,
): UseQueryResult<ReportDetailResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.reportDetail(reportId ?? ''),
    queryFn: ({ signal }) => client.getReport(reportId as string, signal),
    enabled: typeof reportId === 'string' && reportId.length > 0,
    staleTime: 30_000,
  });
}

export interface UpdateRowVariables {
  readonly reportId: string;
  readonly sectionStorageSeq: number;
  readonly rowStorageSeq: number;
  readonly row: ReportRowInput;
  /** The row's current `updatedAt` lock, supplied as the write precondition. */
  readonly updatedAt: string;
}

export function useUpdateRow(): UseMutationResult<
  RowEditResult,
  Error,
  UpdateRowVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: UpdateRowVariables) =>
      client.updateRow(
        variables.reportId,
        variables.sectionStorageSeq,
        variables.rowStorageSeq,
        variables.row,
        variables.updatedAt,
      ),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportDetail(variables.reportId),
      });
    },
  });
}

export interface AddRowVariables {
  readonly reportId: string;
  readonly sectionStorageSeq: number;
  readonly row: ReportRowInput;
  /** Optional insert-after position; omitted to append at the section's end. */
  readonly afterStorageSeq?: number;
}

export function useAddRow(): UseMutationResult<
  RowEditResult,
  Error,
  AddRowVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: AddRowVariables) =>
      client.addRow(
        variables.reportId,
        variables.sectionStorageSeq,
        variables.row,
        variables.afterStorageSeq,
      ),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportDetail(variables.reportId),
      });
    },
  });
}

export interface DeleteRowVariables {
  readonly reportId: string;
  readonly sectionStorageSeq: number;
  readonly rowStorageSeq: number;
  readonly updatedAt: string;
}

export function useDeleteRow(): UseMutationResult<
  void,
  Error,
  DeleteRowVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: DeleteRowVariables) =>
      client.deleteRow(
        variables.reportId,
        variables.sectionStorageSeq,
        variables.rowStorageSeq,
        variables.updatedAt,
      ),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportDetail(variables.reportId),
      });
    },
  });
}

export interface AddSectionVariables {
  readonly reportId: string;
  readonly name: string;
  readonly seq: number;
  readonly afterStorageSeq?: number;
}

export function useAddSection(): UseMutationResult<
  AddSectionResult,
  Error,
  AddSectionVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: AddSectionVariables) =>
      client.addSection(
        variables.reportId,
        variables.name,
        variables.seq,
        variables.afterStorageSeq,
      ),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportDetail(variables.reportId),
      });
    },
  });
}

export function useImportReport(): UseMutationResult<
  ImportReportResult,
  Error,
  string
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (definitionText: string) => client.importReport(definitionText),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.reports() });
    },
  });
}

export interface UpdateReportVariables {
  readonly reportId: string;
  /** The parsed, validated definition object to persist verbatim. */
  readonly definition: Record<string, unknown>;
  /** The report's current `updatedAt` lock, supplied as the write precondition. */
  readonly updatedAt: string;
}

/**
 * Replace one report's whole definition. On success both the report's detail and the
 * catalog listing are invalidated so the grid and list reflect the new definition and
 * lock token.
 */
export function useUpdateReport(): UseMutationResult<
  UpdateReportResult,
  Error,
  UpdateReportVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: UpdateReportVariables) =>
      client.updateReport(
        variables.reportId,
        variables.definition,
        variables.updatedAt,
      ),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportDetail(variables.reportId),
      });
      void queryClient.invalidateQueries({ queryKey: queryKeys.reports() });
    },
  });
}

/**
 * Permanently delete one report. On success the catalog listing is invalidated and the
 * now-stale detail cache entry is removed so it cannot be re-read.
 */
export function useDeleteReport(): UseMutationResult<void, Error, string> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (reportId: string) => client.deleteReport(reportId),
    onSuccess: (_data, reportId) => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.reports() });
      queryClient.removeQueries({ queryKey: queryKeys.reportDetail(reportId) });
    },
  });
}

export function useExportReport(): UseMutationResult<
  ExportReportResponse,
  Error,
  string
> {
  const client = useApiClient();
  return useMutation({
    mutationFn: (reportId: string) => client.exportReport(reportId),
  });
}

export function useTestQuery(): UseMutationResult<
  QueryTestResult,
  Error,
  QueryTestRequest
> {
  const client = useApiClient();
  return useMutation({
    mutationFn: (request: QueryTestRequest) => client.testQuery(request),
  });
}

export function useFacilities(
  enabled: boolean,
): UseQueryResult<FacilityListResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.facilities(),
    queryFn: ({ signal }) => client.listFacilities(signal),
    enabled,
    staleTime: 5 * 60_000,
  });
}

/** Lists runs for a report and polls while any run remains active. */
export function useReportRuns(
  reportId: string | undefined,
): UseQueryResult<ReportRunListResponse> {
  const client = useApiClient();
  return useQuery({
    queryKey: queryKeys.reportRuns(reportId ?? ''),
    queryFn: ({ signal }) => client.listReportRuns(reportId as string, signal),
    enabled: typeof reportId === 'string' && reportId.length > 0,
    staleTime: 0,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) {
        return false;
      }
      const hasActive = data.items.some((run) =>
        ACTIVE_RUN_STATUSES.has(run.status),
      );
      return hasActive ? REPORT_RUN_POLL_INTERVAL_MS : false;
    },
  });
}

export interface StartReportRunVariables {
  readonly reportId: string;
  readonly request: StartReportRunRequest;
}

export function useStartReportRun(): UseMutationResult<
  ReportRun,
  Error,
  StartReportRunVariables
> {
  const client = useApiClient();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (variables: StartReportRunVariables) =>
      client.startReportRun(variables.reportId, variables.request),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: queryKeys.reportRuns(variables.reportId),
      });
    },
  });
}

export interface DownloadReportOutputVariables {
  readonly runId: string;
}

export function useDownloadReportOutput(): UseMutationResult<
  Blob,
  Error,
  DownloadReportOutputVariables
> {
  const client = useApiClient();
  return useMutation({
    mutationFn: (variables: DownloadReportOutputVariables) =>
      client.downloadReportRunOutput(variables.runId),
  });
}
