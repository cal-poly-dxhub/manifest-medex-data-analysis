import { useCallback, useRef, useState, type ReactNode } from 'react';
import { useSearch } from '../api/queries';
import { ApiError } from '../api/client';
import {
  SEARCH_INDEXES,
  type MessageSummary,
  type SearchHit,
  type SearchRequest,
} from '../api/types';
import { SearchFilterBuilder } from './SearchFilterBuilder';
import { SearchResultsTable } from './SearchResultsTable';
import { MessageDetail } from './MessageDetail';
import { DetailDrawer } from './DetailDrawer';
import { ErrorState, LoadingState } from './StateViews';

/**
 * Backend codes that mean a named field is not a valid, present field on the chosen index.
 * These drive a distinct "field not found" hint rather than a generic error, and are kept
 * separate from the "no matches" empty state. `invalid_search_field` is accepted for
 * forward-compatibility alongside the codes the service emits today.
 */
const FIELD_NOT_FOUND_CODES: ReadonlySet<string> = new Set([
  'unknown_field',
  'invalid_field',
  'invalid_search_field',
]);

/** Friendly, PHI-free messages for the sanitized request codes the search can return. */
const CODE_MESSAGES: Readonly<Record<string, string>> = {
  invalid_index: 'Choose one of the available document types.',
  too_many_filters: 'Too many filters. Remove some and try again.',
  invalid_operator: 'One of the filters uses an unsupported operator.',
  invalid_value: 'One of the filter values is not valid.',
  value_too_short: 'A "Contains" value is too short. Use at least three characters.',
  value_unsafe: 'A filter value is not allowed. Try a more specific value.',
  invalid_facility: 'The selected facility is not valid.',
  invalid_time: 'The time range is not a valid date and time.',
  invalid_time_range: 'Enter a start time that is before the end time.',
  invalid_limit: 'The page size is not valid.',
  invalid_cursor: 'This page is no longer available. Run the search again.',
  invalid_filter: 'One of the filters is not valid.',
};

interface SearchErrorInfo {
  readonly message: string;
  readonly isFieldError: boolean;
}

function describeSearchError(error: unknown): SearchErrorInfo {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return { message: 'You are not authorized to search.', isFieldError: false };
    }
    if (typeof error.code === 'string') {
      if (FIELD_NOT_FOUND_CODES.has(error.code)) {
        return {
          message:
            'That field was not found for this document type. Pick a field from the suggestions as you type.',
          isFieldError: true,
        };
      }
      const mapped = CODE_MESSAGES[error.code];
      if (mapped) {
        return { message: mapped, isFieldError: false };
      }
    }
  }
  return { message: 'Unable to run the search. Please try again.', isFieldError: false };
}

/** Resolves the display source format for a hit, falling back to the index's format. */
function resolveSourceFormat(hit: SearchHit, request: SearchRequest) {
  if (hit.sourceFormat) {
    return hit.sourceFormat;
  }
  const config = SEARCH_INDEXES.find((entry) => entry.id === request.index);
  return (config?.sourceFormat ?? 'hl7-v2') as MessageSummary['sourceFormat'];
}

/**
 * Adapts a metadata search hit into the {@link MessageSummary} shape so the existing
 * message detail panel and Raw/Parsed body viewer can be reused unchanged. Only metadata
 * is mapped; the body is fetched lazily by the detail panel from the document id.
 */
function toMessageSummary(hit: SearchHit, request: SearchRequest): MessageSummary {
  return {
    documentId: hit.documentId,
    sourceFormat: resolveSourceFormat(hit, request),
    documentTime: hit.documentTime ?? hit.messageTime ?? null,
    ingestedTime: hit.ingestTime ?? '',
  };
}

/**
 * Metadata attribute-search view for non-technical users: a guided filter builder on the
 * left, a metadata-only results grid with cursor pagination in the middle, and the shared
 * message detail panel on the right. No clinical values are ever listed; a body is only
 * loaded when a result is opened.
 */
export function SearchExplorer(): ReactNode {
  const [request, setRequest] = useState<SearchRequest | undefined>(undefined);
  // Opaque cursor stack: index 0 is always the first page (undefined cursor).
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [pageIndex, setPageIndex] = useState(0);
  const [selected, setSelected] = useState<SearchHit | undefined>(undefined);
  const selectedRowRef = useRef<HTMLTableRowElement | null>(null);

  const cursor = cursors[pageIndex];
  const query = useSearch(request, cursor);
  const data = query.data;
  const nextCursor = data?.nextCursor ?? null;
  const total = data?.total ?? 0;

  // Move the selection to the adjacent hit on the current page. Navigation stops at the
  // first and last rows and never crosses a page boundary (no pagination side effects).
  const navigateSelection = useCallback(
    (delta: -1 | 1): void => {
      const items = data?.items;
      if (!items || !selected) {
        return;
      }
      const currentIndex = items.findIndex(
        (hit) => hit.documentId === selected.documentId,
      );
      if (currentIndex < 0) {
        return;
      }
      const nextIndex = currentIndex + delta;
      if (nextIndex < 0 || nextIndex >= items.length) {
        return;
      }
      setSelected(items[nextIndex]);
    },
    [data?.items, selected],
  );

  const handleApply = useCallback((next: SearchRequest): void => {
    setRequest(next);
    setCursors([undefined]);
    setPageIndex(0);
    setSelected(undefined);
  }, []);

  const goPrevious = (): void => {
    if (query.isFetching || pageIndex === 0) {
      return;
    }
    setSelected(undefined);
    setPageIndex((current) => current - 1);
  };

  const goNext = (): void => {
    if (query.isFetching || nextCursor === null) {
      return;
    }
    setSelected(undefined);
    setCursors((current) => {
      // Only append the discovered cursor once, when advancing past the known end.
      if (pageIndex === current.length - 1) {
        return [...current, nextCursor];
      }
      return current;
    });
    setPageIndex((current) => current + 1);
  };

  const renderPagination = (): ReactNode => {
    if (!data) {
      return null;
    }
    return (
      <nav className="pagination" aria-label="Search result pages">
        <span className="pagination__status" aria-live="polite">
          {query.isFetching
            ? 'Loading…'
            : `Page ${pageIndex + 1} · ${total.toLocaleString()} total match${total === 1 ? '' : 'es'}`}
        </span>
        <div className="pagination__controls">
          <button
            type="button"
            className="button"
            aria-label="Previous page"
            disabled={pageIndex === 0 || query.isFetching}
            onClick={goPrevious}
          >
            Previous
          </button>
          <button
            type="button"
            className="button"
            aria-label="Next page"
            disabled={nextCursor === null || query.isFetching}
            onClick={goNext}
          >
            Next
          </button>
        </div>
      </nav>
    );
  };

  const renderResults = (): ReactNode => {
    if (request === undefined) {
      return (
        <div className="state state--empty" role="status">
          <p>Build your filters and select Apply to search.</p>
        </div>
      );
    }
    if (query.isLoading) {
      return <LoadingState label="Searching…" />;
    }
    if (query.isError) {
      const info = describeSearchError(query.error);
      if (info.isFieldError) {
        return <ErrorState message={info.message} />;
      }
      return (
        <ErrorState message={info.message} onRetry={() => void query.refetch()} />
      );
    }
    if (!data || data.items.length === 0) {
      return (
        <div className="state state--empty" role="status" aria-live="polite">
          <p>No documents match these filters. Try broadening your search.</p>
        </div>
      );
    }
    return (
      <>
        <SearchResultsTable
          rows={data.items}
          selectedId={selected?.documentId}
          onSelect={setSelected}
          selectedRowRef={selectedRowRef}
        />
        {renderPagination()}
      </>
    );
  };

  return (
    <div className="explorer">
      <section className="explorer__main" aria-label="Search">
        <SearchFilterBuilder onApply={handleApply} isSearching={query.isFetching} />
        {renderResults()}
      </section>
      {selected && request ? (
        <DetailDrawer
          title="Message detail"
          onClose={() => setSelected(undefined)}
          returnFocusRef={selectedRowRef}
          onNavigate={navigateSelection}
        >
          <MessageDetail message={toMessageSummary(selected, request)} />
        </DetailDrawer>
      ) : null}
    </div>
  );
}
