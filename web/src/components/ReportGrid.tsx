import { Fragment, type ReactNode } from 'react';
import type { GridRow, GridSection } from '../util/reportGrid';
import { gridRowSpan, resolveRowCount } from '../util/reportGrid';

interface SelectedRow {
  readonly sectionStorageSeq: number;
  readonly rowStorageSeq: number;
}

interface ReportGridProps {
  readonly sections: readonly GridSection[];
  /** Counts from the latest completed run, keyed by row identity, or undefined for blank cells. */
  readonly rowCounts: Readonly<Record<string, number | null>> | undefined;
  readonly selected: SelectedRow | undefined;
  readonly onSelectRow: (section: GridSection, row: GridRow) => void;
  readonly onAddRow: (section: GridSection) => void;
}

/** Em dash shown for a placeholder row's count cell. */
const PLACEHOLDER_COUNT = '—';

/**
 * Format a row's count cell. A placeholder row (null query) always shows an em dash,
 * regardless of any run counts. Otherwise the count (including 0) is shown, or blank
 * when the latest completed run has no count for the row. Counts are resolved by row
 * identity (with a label fallback for older runs); a count explicitly recorded as null
 * (a skipped placeholder in that run) also renders as an em dash.
 */
function countLabel(
  section: GridSection,
  row: GridRow,
  rowCounts: Readonly<Record<string, number | null>> | undefined,
): string {
  if (row.definition.query === null) {
    return PLACEHOLDER_COUNT;
  }
  const count = resolveRowCount(rowCounts, section.seq, row);
  if (count === undefined) {
    return '';
  }
  return count === null ? PLACEHOLDER_COUNT : String(count);
}

/**
 * Render report sections side by side as column groups. Each section is an adjacent
 * Label/Count column pair, sections are ordered by definition seq, and rows are ordered
 * by seq. Shorter sections are padded with empty cell pairs so every column group aligns.
 * A row is selectable to open the editor panel; counts come from the latest completed run.
 */
export function ReportGrid({
  sections,
  rowCounts,
  selected,
  onSelectRow,
  onAddRow,
}: ReportGridProps): ReactNode {
  if (sections.length === 0) {
    return (
      <p className="report-grid__empty">
        This report has no sections yet. Add a section to begin.
      </p>
    );
  }

  const span = gridRowSpan(sections);
  const rowIndexes = Array.from({ length: span }, (_, index) => index);

  return (
    <div
      className="table-wrap report-grid__wrap"
      role="region"
      aria-label="Report sections"
      tabIndex={0}
    >
      <table className="table report-grid">
        <thead>
          <tr>
            {sections.map((section) => (
              <th
                key={`section-${section.sectionStorageSeq}`}
                colSpan={2}
                scope="colgroup"
                className="report-grid__section-head"
              >
                <div className="report-grid__section-head-content">
                  <span className="report-grid__section-name">{section.name}</span>
                  <button
                    type="button"
                    className="button report-grid__add-row"
                    onClick={() => onAddRow(section)}
                  >
                    Add row
                  </button>
                </div>
              </th>
            ))}
          </tr>
          <tr>
            {sections.map((section) => (
              <Fragment key={`head-${section.sectionStorageSeq}`}>
                <th scope="col" className="report-grid__label-head">
                  Label
                </th>
                <th scope="col" className="report-grid__count-head">
                  Count
                </th>
              </Fragment>
            ))}
          </tr>
        </thead>
        <tbody>
          {rowIndexes.map((rowIndex) => (
            <tr key={`row-${rowIndex}`}>
              {sections.map((section) => {
                const row = section.rows[rowIndex];
                if (!row) {
                  return (
                    <Fragment key={`pad-${section.sectionStorageSeq}`}>
                      <td className="report-grid__pad" aria-hidden="true" />
                      <td className="report-grid__pad" aria-hidden="true" />
                    </Fragment>
                  );
                }
                const isSelected =
                  selected !== undefined &&
                  selected.sectionStorageSeq === row.sectionStorageSeq &&
                  selected.rowStorageSeq === row.rowStorageSeq;
                const isPlaceholder = row.definition.query === null;
                return (
                  <Fragment key={`cell-${section.sectionStorageSeq}-${row.rowStorageSeq}`}>
                    <td
                      className={
                        isSelected
                          ? 'report-grid__label report-grid__cell--selected'
                          : 'report-grid__label'
                      }
                    >
                      <button
                        type="button"
                        className="report-grid__row-button"
                        aria-current={isSelected ? 'true' : undefined}
                        title={isPlaceholder ? 'No query yet' : undefined}
                        aria-label={
                          isPlaceholder
                            ? `${row.definition.label} (no query yet)`
                            : undefined
                        }
                        onClick={() => onSelectRow(section, row)}
                      >
                        {row.definition.label}
                        {isPlaceholder ? (
                          <span className="report-grid__placeholder-tag" aria-hidden="true">
                            No query yet
                          </span>
                        ) : null}
                      </button>
                    </td>
                    <td
                      className={
                        isSelected
                          ? 'report-grid__count report-grid__cell--selected'
                          : 'report-grid__count'
                      }
                    >
                      {countLabel(section, row, rowCounts)}
                    </td>
                  </Fragment>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
