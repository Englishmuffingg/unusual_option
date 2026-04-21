import * as React from 'react'
import { ColumnDef, flexRender, getCoreRowModel, getSortedRowModel, SortingState, useReactTable } from '@tanstack/react-table'
import { SummaryRow } from '@/lib/types'

type Props = {
  title: string
  data: SummaryRow[]
}

const columns: ColumnDef<SummaryRow>[] = [
  { header: 'ticker', accessorKey: 'ticker' },
  { header: '总权利金', accessorKey: 'total_est_premium' },
  { header: 'Call%', accessorFn: (r) => (r.call_premium_pct * 100).toFixed(1) },
  { header: 'Put%', accessorFn: (r) => (r.put_premium_pct * 100).toFixed(1) },
  { header: '本次刷新数', accessorKey: 'refreshed_rows' },
  { header: '今日新增数', accessorKey: 'new_rows' },
  { header: 'median_dte', accessorKey: 'median_dte' },
  { header: 'avg_ratio', accessorKey: 'avg_ratio' },
  { header: 'max_ratio', accessorKey: 'max_ratio' },
  { header: 'bullish_score', accessorKey: 'bullish_score' },
  { header: 'flow', accessorKey: 'flow_desc' },
]

const FOCUS = new Set(['SPY', 'GLD', 'QQQ', 'MSFT'])

export function SummaryTable({ title, data }: Props) {
  const [sorting, setSorting] = React.useState<SortingState>([{ id: 'total_est_premium', desc: true }])
  const [keyword, setKeyword] = React.useState('')
  const [focusOnly, setFocusOnly] = React.useState(false)
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({})

  const filtered = React.useMemo(() => {
    return data.filter((r) => {
      const okKeyword = r.ticker.toLowerCase().includes(keyword.toLowerCase())
      const okFocus = !focusOnly || FOCUS.has(r.ticker)
      return okKeyword && okFocus
    })
  }, [data, keyword, focusOnly])

  const table = useReactTable({
    data: filtered,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  })

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        <input className="rounded bg-slate-800 px-2 py-1 text-xs" placeholder="筛选 ticker" value={keyword} onChange={(e) => setKeyword(e.target.value)} />
        <button className={`rounded px-2 py-1 text-xs ${focusOnly ? 'bg-amber-500 text-black' : 'bg-slate-700'}`} onClick={() => setFocusOnly((v) => !v)}>
          {focusOnly ? 'Focus:开' : 'Focus:关'}
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            {table.getHeaderGroups().map((hg) => (
              <tr key={hg.id} className="border-b border-slate-700">
                {hg.headers.map((h) => (
                  <th key={h.id} className="cursor-pointer px-2 py-2 text-left" onClick={h.column.getToggleSortingHandler()}>
                    {flexRender(h.column.columnDef.header, h.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <React.Fragment key={row.id}>
                <tr className="border-b border-slate-800 hover:bg-slate-800/60" onClick={() => setExpanded((s) => ({ ...s, [row.id]: !s[row.id] }))}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-2 py-2">{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                  ))}
                </tr>
                {expanded[row.id] && (
                  <tr className="bg-slate-800/40">
                    <td colSpan={11} className="px-2 py-2 text-slate-300">
                      详情：刷新占比 {(row.original.refreshed_row_pct * 100 || 0).toFixed(1)}%，今日新增占比 {(row.original.new_row_pct * 100 || 0).toFixed(1)}%。
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
