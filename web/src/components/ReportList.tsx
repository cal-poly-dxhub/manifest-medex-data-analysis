import type { ReactNode } from 'react';
import type { ReportSummary } from '../api/types';

interface ReportListProps {
  readonly reports: readonly ReportSummary[];
  readonly selectedId: string | undefined;
  readonly onSelect: (report: ReportSummary) => void;
}

/** A selectable list of report definitions (metadata only). */
export function ReportList({
  reports,
  selectedId,
  onSelect,
}: ReportListProps): ReactNode {
  return (
    <ul className="report-list" aria-label="Report definitions">
      {reports.map((report) => {
        const isSelected = report.reportId === selectedId;
        return (
          <li key={report.reportId}>
            <button
              type="button"
              className={
                isSelected
                  ? 'report-list__item report-list__item--selected'
                  : 'report-list__item'
              }
              aria-current={isSelected ? 'true' : undefined}
              onClick={() => onSelect(report)}
            >
              <span className="report-list__name">{report.name}</span>
              {report.description ? (
                <span className="report-list__description">
                  {report.description}
                </span>
              ) : null}
            </button>
          </li>
        );
      })}
    </ul>
  );
}
