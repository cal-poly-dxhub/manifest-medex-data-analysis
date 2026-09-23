/**
 * Join a report's clean definition with its editor metadata into a spreadsheet-friendly
 * model. The backend builds the definition and editor projections from the same ordered
 * item list, so `definition.sections[i]` corresponds to `editor.sections[i]` and, within
 * a section, `rows[j]` corresponds by position. This module relies only on that positional
 * alignment to attach each row's storage address and optimistic-lock token, then sorts by
 * the definition's own `seq` values for display. No count or run data is mixed in here.
 */
import type {
  ReportDefinitionRow,
  ReportDetailResponse,
  ReportRun,
} from '../api/types';

/** One grid row: the display fields plus the storage address needed to edit it. */
export interface GridRow {
  readonly definition: ReportDefinitionRow;
  readonly sectionStorageSeq: number;
  readonly rowStorageSeq: number;
  /** Optimistic-lock token used as the precondition on save/delete; null blocks editing. */
  readonly updatedAt: string | null;
}

/** One grid column group: a section rendered as an adjacent Label/Count column pair. */
export interface GridSection {
  readonly sectionStorageSeq: number;
  readonly seq: number;
  readonly name: string;
  readonly rows: readonly GridRow[];
}

/** Build the ordered grid sections by joining the definition and editor projections. */
export function buildGridSections(detail: ReportDetailResponse): GridSection[] {
  const editorSections = detail.editor.sections;
  const sections = detail.definition.sections.map((defSection, sectionIndex) => {
    const editorSection = editorSections[sectionIndex];
    const rows: GridRow[] = defSection.rows.map((definitionRow, rowIndex) => {
      const editorRow = editorSection?.rows[rowIndex];
      return {
        definition: definitionRow,
        sectionStorageSeq: editorSection?.storageSeq ?? 0,
        rowStorageSeq: editorRow?.storageSeq ?? 0,
        updatedAt: editorRow?.updatedAt ?? null,
      };
    });
    rows.sort((a, b) => a.definition.seq - b.definition.seq);
    return {
      sectionStorageSeq: editorSection?.storageSeq ?? 0,
      seq: defSection.seq,
      name: defSection.name,
      rows,
    };
  });
  sections.sort((a, b) => a.seq - b.seq);
  return sections;
}

/** The largest row count across all sections; the grid pads shorter sections to this. */
export function gridRowSpan(sections: readonly GridSection[]): number {
  return sections.reduce((max, section) => Math.max(max, section.rows.length), 0);
}

/**
 * Return the aggregate counts of the latest completed run that carries them. The runs
 * list is newest-first, so the first complete run with `rowCounts` wins. Returns undefined
 * when no completed run has counts, so callers render blank count cells.
 *
 * The returned map is keyed by row identity (`rowCountKey`) for runs recorded by the
 * identity-keyed worker; older runs may instead be keyed by row label. `resolveRowCount`
 * handles both by trying the identity key first and falling back to the label.
 */
export function latestCompletedRowCounts(
  runs: readonly ReportRun[] | undefined,
): Readonly<Record<string, number | null>> | undefined {
  if (!runs) {
    return undefined;
  }
  for (const run of runs) {
    if (run.status === 'complete' && run.rowCounts) {
      return run.rowCounts;
    }
  }
  return undefined;
}

/**
 * Stable per-row identity key used for run counts, mirroring the backend
 * `row_count_key`: `S<section seq>:R<row seq>`. Two sections may reuse the same row
 * label, so counts are addressed by this identity rather than by label.
 */
export function rowCountKey(sectionSeq: number, rowSeq: number): string {
  return `S${sectionSeq}:R${rowSeq}`;
}

/**
 * Resolve one row's count from a run's `rowCounts` map. The identity key
 * (`rowCountKey`) is tried first; for older runs that were keyed by label, the row's
 * label is used as a fallback. Returns undefined when neither key is present.
 */
export function resolveRowCount(
  rowCounts: Readonly<Record<string, number | null>> | undefined,
  sectionSeq: number,
  row: GridRow,
): number | null | undefined {
  if (!rowCounts) {
    return undefined;
  }
  const identity = rowCountKey(sectionSeq, row.definition.seq);
  if (Object.hasOwn(rowCounts, identity)) {
    return rowCounts[identity];
  }
  const label = row.definition.label;
  if (Object.hasOwn(rowCounts, label)) {
    return rowCounts[label];
  }
  return undefined;
}

/** The next logical definition seq for a new row in a section: max existing seq + 1. */
export function nextRowSeq(section: GridSection): number {
  return section.rows.reduce((max, row) => Math.max(max, row.definition.seq), 0) + 1;
}
