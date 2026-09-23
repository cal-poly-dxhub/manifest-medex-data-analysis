import { useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import { useReportList } from '../api/queries';
import type { ReportSummary } from '../api/types';
import { EmptyState, ErrorState, LoadingState } from './StateViews';
import { ImportReportDialog } from './ImportReportDialog';
import { ReportList } from './ReportList';
import { ReportDetail } from './ReportDetail';

function listErrorMessage(error: unknown): string {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
    return 'You are not authorized to view reports.';
  }
  return 'Unable to load reports. Please try again.';
}

export function ReportsExplorer(): ReactNode {
  const reports = useReportList();
  const [selectedId, setSelectedId] = useState<string | undefined>(undefined);
  const [showImport, setShowImport] = useState(false);

  const renderList = (): ReactNode => {
    if (reports.isLoading) {
      return <LoadingState label="Loading reports…" />;
    }
    if (reports.isError) {
      return (
        <ErrorState
          message={listErrorMessage(reports.error)}
          onRetry={() => void reports.refetch()}
        />
      );
    }
    const items = reports.data?.items ?? [];
    if (items.length === 0) {
      return <EmptyState message="No report definitions are available." />;
    }
    return (
      <ReportList
        reports={items}
        selectedId={selectedId}
        onSelect={(report: ReportSummary) => setSelectedId(report.reportId)}
      />
    );
  };

  return (
    <div className="explorer">
      {selectedId ? (
        <section className="explorer__detail" aria-label="Report detail">
          <ReportDetail
            reportId={selectedId}
            onBack={() => setSelectedId(undefined)}
            onDeleted={() => setSelectedId(undefined)}
          />
        </section>
      ) : (
        <section className="explorer__main" aria-label="Report definitions">
          <div className="reports-toolbar">
            <button
              type="button"
              className="button"
              onClick={() => setShowImport(true)}
            >
              Import report
            </button>
          </div>
          {renderList()}
        </section>
      )}

      {showImport ? (
        <ImportReportDialog
          onClose={() => setShowImport(false)}
          onImported={(result) => {
            setShowImport(false);
            setSelectedId(result.reportId);
          }}
        />
      ) : null}
    </div>
  );
}
