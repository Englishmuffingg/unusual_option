import * as React from 'react'

const focusTickers = new Set(['SPY', 'GLD', 'QQQ', 'MSFT'])

type Row = Record<string, string | number>

type Props = {
  title: string
  rows: Row[]
  useDeltaRows?: boolean
  selectedContract?: string | null
  onSelectContract?: (contractSymbol: string) => void
}

function n(v: string | number, digits = 0) {
  const k = Number(v || 0)
  return Number.isFinite(k) ? k.toLocaleString('zh-CN', { maximumFractionDigits: digits }) : '-'
}

export function ContractChangesTable({ title, rows, useDeltaRows = false, selectedContract, onSelectContract }: Props) {
  const [keyword, setKeyword] = React.useState('')
  const [focusOnly, setFocusOnly] = React.useState(false)
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({})

  const filtered = React.useMemo(
    () =>
      rows.filter((r) => {
        const ticker = String(r.ticker || '')
        const contract = String(r.contract_display_name || r.contract_display_name_cur || r.contract_symbol || '')
        const hit = ticker.toLowerCase().includes(keyword.toLowerCase()) || contract.toLowerCase().includes(keyword.toLowerCase())
        const focusHit = !focusOnly || focusTickers.has(ticker)
        return hit && focusHit
      }),
    [rows, keyword, focusOnly],
  )

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <h3 className="text-sm font-semibold">{title}</h3>
        <input className="rounded border border-slate-300 bg-slate-50 px-2 py-1 text-xs" placeholder="筛选 ticker / 合约" value={keyword} onChange={(e) => setKeyword(e.target.value)} />
        <button className={`rounded px-2 py-1 text-xs ${focusOnly ? 'bg-amber-200 text-amber-900' : 'bg-slate-200 text-slate-700'}`} onClick={() => setFocusOnly((v) => !v)}>
          {focusOnly ? '仅Focus' : '全部'}
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-slate-200 text-slate-600">
              <th className="px-2 py-2 text-left">ticker</th>
              <th className="px-2 py-2 text-left">合约</th>
              <th className="px-2 py-2 text-left">类型</th>
              <th className="px-2 py-2 text-left">strike</th>
              <th className="px-2 py-2 text-left">expiration</th>
              <th className="px-2 py-2 text-left">dte</th>
              <th className="px-2 py-2 text-left">volume</th>
              <th className="px-2 py-2 text-left">estimated_premium</th>
              <th className="px-2 py-2 text-left">delta_volume</th>
              <th className="px-2 py-2 text-left">delta_premium</th>
              <th className="px-2 py-2 text-left">状态</th>
            </tr>
          </thead>
          <tbody>
            {filtered.slice(0, 250).map((r, idx) => {
              const id = `${r.ticker}-${r.contract_symbol}-${idx}`
              const status = String(r.status || (Number(r.is_refreshed || 0) === 1 ? '新增' : '持续'))
              return (
                <React.Fragment key={id}>
                  <tr
                    className={`cursor-pointer border-b border-slate-100 hover:bg-slate-100 ${selectedContract && selectedContract === String(r.contract_symbol || '') ? 'bg-blue-50' : ''}`}
                    onClick={() => {
                      setExpanded((s) => ({ ...s, [id]: !s[id] }))
                      onSelectContract?.(String(r.contract_symbol || ''))
                    }}
                  >
                    <td className="px-2 py-2">{String(r.ticker || '-')}</td>
                    <td className="px-2 py-2">{String(r.contract_display_name || r.contract_display_name_cur || r.contract_symbol || '-')}</td>
                    <td className="px-2 py-2">{String(r.option_type || r.option_type_cur || '-')}</td>
                    <td className="px-2 py-2">{n((r.strike || r.strike_cur) as number, 2)}</td>
                    <td className="px-2 py-2">{String(r.expiration_date || r.expiration_date_cur || '-')}</td>
                    <td className="px-2 py-2">{n((r.dte || r.dte_cur) as number, 0)}</td>
                    <td className="px-2 py-2">{n((r.options_volume || r.options_volume_cur) as number, 0)}</td>
                    <td className="px-2 py-2">{n((r.estimated_premium || r.estimated_premium_cur) as number, 0)}</td>
                    <td className="px-2 py-2">{useDeltaRows ? n((r.delta_volume || 0) as number, 0) : '-'}</td>
                    <td className="px-2 py-2">{useDeltaRows ? n((r.delta_premium || 0) as number, 0) : '-'}</td>
                    <td className="px-2 py-2"><span className="rounded bg-cyan-100 px-2 py-0.5 text-[11px] text-cyan-700">{status}</span></td>
                  </tr>
                  {expanded[id] && (
                    <tr className="bg-slate-50">
                      <td colSpan={11} className="px-2 py-2 text-slate-600">
                        OI: {n((r.open_interest || r.open_interest_cur || 0) as number, 0)} ｜ 比率: {n((r.volume_to_open_interest_ratio || 0) as number, 2)} ｜ contract_symbol: {String(r.contract_symbol || '-')}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
