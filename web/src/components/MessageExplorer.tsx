import { useCallback, useMemo, useState, type ReactNode } from 'react';
import { MESSAGE_PAGE_SIZE, useMessageList } from '../api/queries';
import type { MessageFilters, MessageSummary } from '../api/types';
import { ApiError } from '../api/client';
import { MessageFiltersForm } from './MessageFiltersForm';
import { MessageTable } from './MessageTable';
import { MessageDetail } from './MessageDetail';
import { EmptyState, ErrorState, LoadingState } from './StateViews';

function listErrorMessage(error: unknown): string {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
    return 'You are not authorized to view messages.';
  }
  return 'Unable to load messages. Please try again.';
}

function visiblePageIndexes(
  currentIndex: number,
  knownPageCount: number,
  canDiscoverNext: boolean,
): number[] {
  const maximumIndex = knownPageCount - 1 + (canDiscoverNext ? 1 : 0);
  const candidates = new Set<number>([0, maximumIndex]);
  for (let index = currentIndex - 2; index <= currentIndex + 2; index += 1) {
    if (index >= 0 && index <= maximumIndex) {
      candidates.add(index);
    }
  }
  return [...candidates].sort((left, right) => left - right);
}

export function MessageExplorer(): ReactNode {
  const [filters, setFilters] = useState<MessageFilters>({});
  const [pageCursors, setPageCursors] = useState<(string | undefined)[]>([
    undefined,
  ]);
  const [pageIndex, setPageIndex] = useState(0);
  const [selected, setSelected] = useState<MessageSummary | undefined>(undefined);
  const cursor = pageCursors[pageIndex];
  const query = useMessageList(filters, cursor);

  const handleFiltersChange = useCallback((next: MessageFilters): void => {
    setFilters(next);
    setPageCursors([undefined]);
    setPageIndex(0);
    setSelected(undefined);
  }, []);

  const data = query.data;
  const nextCursor = data?.nextCursor ?? null;
  const totalCount = data?.totalCount ?? 0;
  const totalPages = Math.ceil(totalCount / MESSAGE_PAGE_SIZE);
  const canDiscoverNext =
    nextCursor !== null && pageIndex === pageCursors.length - 1;
  const pageIndexes = useMemo(
    () => visiblePageIndexes(pageIndex, pageCursors.length, canDiscoverNext),
    [pageIndex, pageCursors.length, canDiscoverNext],
  );

  const goToPage = (targetIndex: number): void => {
    if (query.isFetching || targetIndex < 0) {
      return;
    }
    if (targetIndex < pageCursors.length) {
      setSelected(undefined);
      setPageIndex(targetIndex);
      return;
    }
    if (
      targetIndex === pageCursors.length &&
      pageIndex === pageCursors.length - 1 &&
      nextCursor !== null
    ) {
      setSelected(undefined);
      setPageCursors((current) => [...current, nextCursor]);
      setPageIndex(targetIndex);
    }
  };

  const renderPagination = (): ReactNode => {
    if (!data) {
      return null;
    }
    const firstResult = totalCount === 0 ? 0 : pageIndex * MESSAGE_PAGE_SIZE + 1;
    const lastResult =
      totalCount === 0 ? 0 : Math.min(firstResult + data.items.length - 1, totalCount);

    return (
      <nav className="pagination" aria-label="Message result pages">
        <span className="pagination__status" aria-live="polite">
          {query.isFetching
            ? 'Loading…'
            : `${firstResult.toLocaleString()}–${lastResult.toLocaleString()} of ${totalCount.toLocaleString()} results · Page ${totalPages === 0 ? 0 : pageIndex + 1} of ${totalPages.toLocaleString()}`}
        </span>
        <div className="pagination__controls">
          <button
            type="button"
            className="button"
            aria-label="Previous page"
            disabled={pageIndex === 0 || query.isFetching}
            onClick={() => goToPage(pageIndex - 1)}
          >
            Previous
          </button>
          <div className="pagination__pages" aria-label="Page numbers">
            {pageIndexes.map((index, position) => {
              const previousIndex = pageIndexes[position - 1];
              const showGap = previousIndex !== undefined && index - previousIndex > 1;
              const isCurrent = index === pageIndex;
              return (
                <span key={index} className="pagination__page-item">
                  {showGap ? <span className="pagination__ellipsis">…</span> : null}
                  <button
                    type="button"
                    className={isCurrent ? 'button pagination__page pagination__page--current' : 'button pagination__page'}
                    aria-current={isCurrent ? 'page' : undefined}
                    disabled={query.isFetching}
                    onClick={() => goToPage(index)}
                  >
                    {index + 1}
                  </button>
                </span>
              );
            })}
          </div>
          <button
            type="button"
            className="button"
            aria-label="Next page"
            disabled={nextCursor === null || query.isFetching}
            onClick={() => goToPage(pageIndex + 1)}
          >
            Next
          </button>
        </div>
      </nav>
    );
  };

  const renderResults = (): ReactNode => {
    if (query.isLoading) {
      return <LoadingState label="Loading messages…" />;
    }
    if (query.isError) {
      return (
        <ErrorState
          message={listErrorMessage(query.error)}
          onRetry={() => void query.refetch()}
        />
      );
    }
    if (!data || data.items.length === 0) {
      return (
        <>
          <EmptyState />
          {renderPagination()}
        </>
      );
    }
    return (
      <>
        <MessageTable
          rows={data.items}
          selectedId={selected?.documentId}
          onSelect={setSelected}
        />
        {renderPagination()}
      </>
    );
  };

  return (
    <div className="explorer">
      <section className="explorer__main" aria-label="Message list">
        <MessageFiltersForm value={filters} onChange={handleFiltersChange} />
        {renderResults()}
      </section>
      <MessageDetail message={selected} />
    </div>
  );
}
