import type {
  BodyVariant,
  MessageBody,
  MessageFilters,
  MessageDetailRecord,
  MessageListResponse,
  SqlQueryResult,
} from './types';

/** Error carrying the HTTP status for a failed API call. Never includes bodies/IDs. */
export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

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

  private buildHeaders(hasJsonBody: boolean): Headers {
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
    return headers;
  }

  private async fetchResponse(
    path: string,
    init: RequestInit,
    hasJsonBody: boolean,
  ): Promise<Response> {
    const response = await fetch(`${this.basePath}${path}`, {
      ...init,
      headers: this.buildHeaders(hasJsonBody),
      credentials: 'same-origin',
    });
    if (!response.ok) {
      // Never include response content or resource identifiers in errors.
      throw new ApiError(
        response.status,
        `Request failed with status ${response.status}`,
      );
    }
    return response;
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
}
