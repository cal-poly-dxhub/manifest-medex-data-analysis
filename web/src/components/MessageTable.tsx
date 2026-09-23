import { useMemo, type ReactNode, type RefObject } from 'react';
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table';
import type { MessageSummary } from '../api/types';
import { formatTimestamp } from '../util/format';

interface MessageTableProps {
  readonly rows: readonly MessageSummary[];
  readonly selectedId: string | undefined;
  readonly onSelect: (row: MessageSummary) => void;
  /** Receives the currently selected row element so callers can restore focus to it. */
  readonly selectedRowRef?: RefObject<HTMLTableRowElement | null>;
}

const columnHelper = createColumnHelper<MessageSummary>();

export function MessageTable({
  rows,
  selectedId,
  onSelect,
  selectedRowRef,
}: MessageTableProps): ReactNode {
  const columns = useMemo(
    () => [
      columnHelper.accessor('documentId', {
        header: 'Document ID',
        cell: (info) => <span className="cell-mono">{info.getValue()}</span>,
      }),
      columnHelper.accessor('sourceFormat', {
        header: 'Source format',
        cell: (info) => info.getValue(),
      }),
      columnHelper.accessor('documentTime', {
        header: 'Document time',
        cell: (info) => formatTimestamp(info.getValue()),
      }),
      columnHelper.accessor('ingestedTime', {
        header: 'Ingested time',
        cell: (info) => formatTimestamp(info.getValue()),
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: rows as MessageSummary[],
    columns,
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.documentId,
  });

  return (
    <div className="table-wrap" role="region" aria-label="Messages" tabIndex={0}>
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
