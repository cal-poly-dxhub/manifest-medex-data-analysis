import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { ApiError } from '../api/client';
import {
  useAddSection,
  useExportReport,
  useReportDetail,
  useReportRuns,
} from '../api/queries';
import {
  buildGridSections,
  latestCompletedRowCounts,
  nextRowSeq,
  type GridRow,
  type GridSection,
} from '../util/reportGrid';
import { ErrorState, LoadingState } from './StateViews';
import { ReportGrid } from './ReportGrid';
import { ReportRunsList } from './ReportRunsList';
import { RowEditorPanel, type RowEditorTarget } from './RowEditorPanel';
import { RunReportDialog } from './RunReportDialog';
import { EditReportJsonDialog } from './EditReportJsonDialog';
import { DeleteReportDialog } from './DeleteReportDialog';

interface ReportDetailProps {
  readonly reportId: string;
  /** Optional handler to return to the report list. When provided, a Back button is shown. */
  readonly onBack?: () => void;
  /** Optional handler invoked after the report is deleted. Falls back to onBack. */
  readonly onDeleted?: () => void;
}

function detailErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to view this report.';
    }
    if (error.status === 404) {
      return 'This report no longer exists.';
    }
  }
  return 'Unable to load the selected report.';
}

function addSectionErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 400) {
      return 'The section is invalid. Enter a non-empty name.';
    }
    if (error.status === 409) {
      return 'The section could not be placed. Reload the report and try again.';
    }
    if (error.status === 401 || error.status === 403) {
      return 'You are not authorized to edit this report.';
    }
  }
  return 'Unable to add the section. Please try again.';
}

/** The next logical section seq for a new section: max existing seq + 1. */
function nextSectionSeq(sections: readonly GridSection[]): number {
  return sections.reduce((max, section) => Math.max(max, section.seq), 0) + 1;
}

export function ReportDetail({ reportId, onBack, onDeleted }: ReportDetailProps): ReactNode {
  const detail = useReportDetail(reportId);
  const runs = useReportRuns(reportId);
  const exportReport = useExportReport();
  const addSection = useAddSection();

  const [panelTarget, setPanelTarget] = useState<RowEditorTarget | undefined>(undefined);
  const [showRunDialog, setShowRunDialog] = useState(false);
  const [showAddSection, setShowAddSection] = useState(false);
  const [sectionName, setSectionName] = useState('');
  const [showEditJson, setShowEditJson] = useState(false);
  const [showDelete, setShowDelete] = useState(false);

  // Reset transient UI whenever the selected report changes.
  useEffect(() => {
    setPanelTarget(undefined);
    setShowRunDialog(false);
    setShowAddSection(false);
    setSectionName('');
    setShowEditJson(false);
    setShowDelete(false);
    exportReport.reset();
    addSection.reset();
    // The mutation identities are stable enough for this reset-on-select effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reportId]);

  const sections = useMemo(
    () => (detail.data ? buildGridSections(detail.data) : []),
    [detail.data],
  );
  const rowCounts = useMemo(
    () => latestCompletedRowCounts(runs.data?.items),
    [runs.data],
  );

  if (detail.isLoading) {
    return (
      <div className="report-detail">
        {onBack ? (
          <div className="report-detail__nav">
            <button type="button" className="button" onClick={onBack}>
              ← Back to reports
            </button>
          </div>
        ) : null}
        <LoadingState label="Loading report…" />
      </div>
    );
  }
  if (detail.isError) {
    return (
      <div className="report-detail">
        {onBack ? (
          <div className="report-detail__nav">
            <button type="button" className="button" onClick={onBack}>
              ← Back to reports
            </button>
          </div>
        ) : null}
        <ErrorState
          message={detailErrorMessage(detail.error)}
          onRetry={() => void detail.refetch()}
        />
      </div>
    );
  }
  const data = detail.data;
  if (!data) {
    return null;
  }
  const { definition } = data;

  const selected =
    panelTarget?.mode === 'edit'
      ? {
          sectionStorageSeq: panelTarget.sectionStorageSeq,
          rowStorageSeq: panelTarget.rowStorageSeq,
        }
      : undefined;

  const handleSelectRow = (_section: GridSection, row: GridRow): void => {
    setPanelTarget({
      mode: 'edit',
      sectionStorageSeq: row.sectionStorageSeq,
      rowStorageSeq: row.rowStorageSeq,
      definition: row.definition,
      updatedAt: row.updatedAt,
    });
  };

  const handleAddRow = (section: GridSection): void => {
    // Insert after the currently-selected row when it belongs to this section, else append.
    const afterStorageSeq =
      panelTarget?.mode === 'edit' &&
      panelTarget.sectionStorageSeq === section.sectionStorageSeq
        ? panelTarget.rowStorageSeq
        : undefined;
    const nextSeq = nextRowSeq(section);
    setPanelTarget(
      afterStorageSeq === undefined
        ? { mode: 'create', sectionStorageSeq: section.sectionStorageSeq, nextSeq }
        : {
            mode: 'create',
            sectionStorageSeq: section.sectionStorageSeq,
            afterStorageSeq,
            nextSeq,
          },
    );
  };

  const handleExport = (): void => {
    exportReport.mutate(reportId, {
      onSuccess: (result) => {
        const blob = new Blob([result.text], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = `${reportId}.json`;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        URL.revokeObjectURL(url);
      },
    });
  };

  const handleAddSection = (): void => {
    const name = sectionName.trim();
    if (!name) {
      return;
    }
    addSection.mutate(
      { reportId, name, seq: nextSectionSeq(sections) },
      {
        onSuccess: () => {
          setShowAddSection(false);
          setSectionName('');
        },
      },
    );
  };

  return (
    <div className="report-detail">
      {onBack ? (
        <div className="report-detail__nav">
          <button type="button" className="button" onClick={onBack}>
            ← Back to reports
          </button>
        </div>
      ) : null}
      <header className="report-detail__header">
        <div>
          <h3 className="report-detail__title">{definition.name}</h3>
          {definition.description ? (
            <p className="report-detail__description">{definition.description}</p>
          ) : null}
        </div>
        <div className="report-detail__actions">
          <button
            type="button"
            className="button"
            onClick={() => setShowAddSection((current) => !current)}
          >
            Add section
          </button>
          <button
            type="button"
            className="button"
            disabled={exportReport.isPending}
            onClick={handleExport}
          >
            {exportReport.isPending ? 'Exporting…' : 'Export JSON'}
          </button>
          <button
            type="button"
            className="button"
            onClick={() => setShowEditJson(true)}
          >
            Edit full report JSON
          </button>
          <button
            type="button"
            className="button button--primary"
            onClick={() => setShowRunDialog(true)}
          >
            Run report
          </button>
          <button
            type="button"
            className="button button--danger"
            onClick={() => setShowDelete(true)}
          >
            Delete report
          </button>
        </div>
      </header>

      {exportReport.isError ? (
        <p className="report-editor__error" role="alert">
          Unable to export the report. Please try again.
        </p>
      ) : null}

      {showAddSection ? (
        <div className="add-section" role="group" aria-label="Add section">
          <label className="add-section__field">
            <span>Section name</span>
            <input
              type="text"
              value={sectionName}
              onChange={(event) => setSectionName(event.target.value)}
            />
          </label>
          {addSection.isError ? (
            <p className="report-editor__error" role="alert">
              {addSectionErrorMessage(addSection.error)}
            </p>
          ) : null}
          <div className="add-section__actions">
            <button
              type="button"
              className="button"
              onClick={() => {
                setShowAddSection(false);
                setSectionName('');
                addSection.reset();
              }}
            >
              Cancel
            </button>
            <button
              type="button"
              className="button button--primary"
              disabled={sectionName.trim().length === 0 || addSection.isPending}
              onClick={handleAddSection}
            >
              {addSection.isPending ? 'Adding…' : 'Add section'}
            </button>
          </div>
        </div>
      ) : null}

      <ReportGrid
        sections={sections}
        rowCounts={rowCounts}
        selected={selected}
        onSelectRow={handleSelectRow}
        onAddRow={handleAddRow}
      />

      <section className="report-detail__runs" aria-label="Report runs">
        <h4 className="report-detail__runs-title">Runs</h4>
        <ReportRunsList reportId={reportId} />
      </section>

      {panelTarget ? (
        <RowEditorPanel
          reportId={reportId}
          target={panelTarget}
          onClose={() => setPanelTarget(undefined)}
        />
      ) : null}

      {showRunDialog ? (
        <RunReportDialog
          reportId={reportId}
          reportName={definition.name}
          onClose={() => setShowRunDialog(false)}
        />
      ) : null}

      {showEditJson ? (
        <EditReportJsonDialog
          reportId={reportId}
          definition={definition}
          updatedAt={data.editor.updatedAt}
          onClose={() => setShowEditJson(false)}
        />
      ) : null}

      {showDelete ? (
        <DeleteReportDialog
          reportId={reportId}
          reportName={definition.name}
          onClose={() => setShowDelete(false)}
          onDeleted={() => {
            setShowDelete(false);
            if (onDeleted) {
              onDeleted();
            } else if (onBack) {
              onBack();
            }
          }}
        />
      ) : null}
    </div>
  );
}
