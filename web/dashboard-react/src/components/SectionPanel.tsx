import { SectionPayload } from '@/lib/types'
import { useMemo, useState } from 'react'
import { SectionCharts } from './SectionCharts'
import { FocusTickerBlocks } from './FocusTickerBlocks'
import { SummaryTable } from './SummaryTable'
import { ContractChangesTable } from './ContractChangesTable'
import { RefreshDeltaPanel } from './RefreshDeltaPanel'
import { dteBucket, flowFromOptionType } from '@/lib/format'

type Props = {
  title: string
  subtitle: string
  section: SectionPayload
  focusTitle: string
  refreshDelta?: {
    contract_count_delta: number
    options_volume_delta: number
    estimated_premium_delta: number
    open_interest_delta: number
    new_contract_count: number
    disappeared_contract_count: number
    persistent_contract_count: number
    snapshot_time: string | null
    previous_snapshot_time: string | null
    ticker_rank: Array<{ ticker: string; premium_delta: number; volume_delta: number; open_interest_delta: number; contract_count_delta: number }>
    contract_changes: Array<Record<string, string | number>>
  }
}

export function SectionPanel({ title, subtitle, section, focusTitle, refreshDelta }: Props) {
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [selectedFlow, setSelectedFlow] = useState<string | null>(null)
  const [selectedDte, setSelectedDte] = useState<string | null>(null)
  const [selectedStrikeExp, setSelectedStrikeExp] = useState<{ strike: string; expiration: string } | null>(null)
  const [selectedContract, setSelectedContract] = useState<string | null>(null)

  const baseRows = section.key === 'refreshed' && refreshDelta ? refreshDelta.contract_changes : section.contracts

  const filteredRows = useMemo(() => {
    return baseRows.filter((r) => {
      const ticker = String(r.ticker || '')
      const optionType = String(r.option_type || r.option_type_cur || '')
      const dte = Number(r.dte || r.dte_cur || 0)
      const strike = String(r.strike || r.strike_cur || '')
      const expiration = String(r.expiration_date || r.expiration_date_cur || '')
      const flow = String(r.status || flowFromOptionType(optionType))
      const passTicker = !selectedTicker || ticker === selectedTicker
      const passFlow = !selectedFlow || flow === selectedFlow
      const passDte = !selectedDte || dteBucket(dte) === selectedDte
      const passStrikeExp = !selectedStrikeExp || (strike === selectedStrikeExp.strike && expiration === selectedStrikeExp.expiration)
      return passTicker && passFlow && passDte && passStrikeExp
    })
  }, [baseRows, selectedTicker, selectedFlow, selectedDte, selectedStrikeExp])

  const clearFilters = () => {
    setSelectedTicker(null)
    setSelectedFlow(null)
    setSelectedDte(null)
    setSelectedStrikeExp(null)
  }

  const focusOiDeltaByTicker = useMemo(() => {
    const out: Record<string, number> = {}
    if (section.key !== 'refreshed' || !refreshDelta) return out
    for (const r of refreshDelta.ticker_rank) {
      out[String(r.ticker)] = Number(r.open_interest_delta || 0)
    }
    return out
  }, [section.key, refreshDelta])

  return (
    <section className="space-y-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div>
        <h2 className="text-lg font-semibold text-slate-900">{title}</h2>
        <p className="text-xs text-slate-600">{subtitle}</p>
      </div>
      <div className="flex flex-wrap gap-2 text-xs">
        {selectedTicker && <span className="rounded bg-blue-100 px-2 py-1">Ticker: {selectedTicker}</span>}
        {selectedFlow && <span className="rounded bg-emerald-100 px-2 py-1">方向: {selectedFlow}</span>}
        {selectedDte && <span className="rounded bg-amber-100 px-2 py-1">期限: {selectedDte}</span>}
        {selectedStrikeExp && <span className="rounded bg-fuchsia-100 px-2 py-1">Strike×Exp: {selectedStrikeExp.strike} / {selectedStrikeExp.expiration}</span>}
        {(selectedTicker || selectedFlow || selectedDte || selectedStrikeExp) && (
          <button className="rounded bg-slate-200 px-2 py-1" onClick={clearFilters}>清空筛选</button>
        )}
      </div>
      {section.key === 'refreshed' && refreshDelta && <RefreshDeltaPanel data={refreshDelta} />}
      <SummaryTable title={`${title} ticker 排行`} data={section.summary} />
      <SectionCharts
        section={section}
        tickerRank={section.key === 'refreshed' ? refreshDelta?.ticker_rank : []}
        onSelectTicker={setSelectedTicker}
        onSelectFlow={setSelectedFlow}
        onSelectDte={setSelectedDte}
        onSelectStrikeExp={setSelectedStrikeExp}
        selectedTicker={selectedTicker}
        selectedFlow={selectedFlow}
        selectedDte={selectedDte}
        selectedStrikeExp={selectedStrikeExp}
        selectedContract={selectedContract}
        contractRows={baseRows}
      />
      {section.key === 'refreshed' && refreshDelta ? (
        <ContractChangesTable title="本次刷新合约明细（含新增/持续/扩大/消失）" rows={filteredRows} useDeltaRows selectedContract={selectedContract} onSelectContract={setSelectedContract} />
      ) : (
        <ContractChangesTable title={`${title} 合约明细`} rows={filteredRows} selectedContract={selectedContract} onSelectContract={setSelectedContract} />
      )}
      <FocusTickerBlocks blocks={section.focus_blocks} title={focusTitle} variant={section.key} oiDeltaByTicker={focusOiDeltaByTicker} />
    </section>
  )
}
