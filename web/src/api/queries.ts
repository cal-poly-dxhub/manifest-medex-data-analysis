import {
  useMutation,
  useQuery,
  type UseMutationResult,
  type UseQueryResult,
} from '@tanstack/react-query';
import { useApiClient } from './ApiClientContext';
import type {
  BodyVariant,
  MessageBody,
  MessageDetailRecord,
  MessageFilters,
  MessageListResponse,
  SqlQueryResult,
} from './types';

export const MESSAGE_PAGE_SIZE = 50;

const queryKeys = {
  messages: (filters: MessageFilters, cursor: string | undefined) =>
    ['messages', filters, cursor ?? null] as const,
  detail: (id: string) => ['message-detail', id] as const,
  body: (id: string, variant: BodyVariant) =>
    ['message-body', id, variant] as const,
};

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
