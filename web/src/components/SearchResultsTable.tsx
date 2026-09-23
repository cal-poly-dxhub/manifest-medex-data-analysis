import { useMemo, type ReactNode, type RefObject } from 'react';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table';
import type { SearchHit } from '../api/types';
import { formatTimestamp } from '../util/format';

interface SearchResultsTableProps {
  readonly rows: readonly SearchHit[];
  readonly selectedId: string | undefined;
  readonly onSelect: (row: SearchHit) => void;
  /** Receives the currently selected row element so callers can restore focus to it. */
  readonly selectedRowRef?: RefObject<HTMLTableRowElement | null>;
}

const columnHelper = createColumnHelper<SearchHit>();

/** Joins the message type and trigger event for display, omitting missing parts. */
function typeTrigger(hit: SearchHit): string {
  const parts = [hit.messageType, hit.triggerEvent].filter(
    (part): part is string => typeof part === 'string' && part.length > 0,
  );
  return parts.length > 0 ? parts.join(' · ') : '—';
}

/** The single clinical time surfaced per index: HL7 message time or C-CDA document time. */
function clinicalTime(hit: SearchHit): string {
  return formatTimestamp(hit.messageTime ?? hit.documentTime ?? null);
}

/**
 * Metadata-only results grid. Every column is administrative metadata — document id,
 * format, facility, message type/trigger, and timestamps. No clinical narrative value is
 * ever rendered here.
 */
export function SearchResultsTable({
  rows,
  selectedId,
  onSelect,
  selectedRowRef,
}: SearchResultsTableProps): ReactNode {
  const columns = useMemo(
    () => [
      columnHelper.accessor('documentId', {
        header: 'Document ID',
        cell: (info) => <span className="cell-mono">{info.getValue()}</span>,
      }),
      columnHelper.accessor('sourceFormat', {
        header: 'Format',
        cell: (info) => info.getValue() ?? '—',
      }),
      columnHelper.accessor((row) => row.sourceFacilityId ?? '—', {
        id: 'facility',
        header: 'Facility',
        cell: (info) => info.getValue(),
      }),
      columnHelper.accessor((row) => typeTrigger(row), {
        id: 'typeTrigger',
        header: 'Type / trigger',
        cell: (info) => info.getValue(),
      }),
      columnHelper.accessor((row) => clinicalTime(row), {
        id: 'clinicalTime',
        header: 'Message / document time',
        cell: (info) => info.getValue(),
      }),
      columnHelper.accessor((row) => row.ingestTime ?? null, {
        id: 'ingestTime',
        header: 'Ingest time',
        cell: (info) => formatTimestamp(info.getValue()),
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: rows as SearchHit[],
    columns,
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.documentId,
  });

  return (
    <div className="table-wrap" role="region" aria-label="Search results" tabIndex={0}>
      <table className="table">
        <thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <th key={header.id} scope="col">
                  {header.isPlaceholder
                    ? null
                    : flexRender(
                        header.column.columnDef.header,
                        header.getContext(),
                      )}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => {
            const isSelected = row.original.documentId === selectedId;
            return (
              <tr
                key={row.id}
                ref={isSelected ? selectedRowRef : undefined}
                className={isSelected ? 'row row--selected' : 'row'}
                aria-selected={isSelected}
                tabIndex={0}
                onClick={() => onSelect(row.original)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    onSelect(row.original);
                  }
                }}
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
