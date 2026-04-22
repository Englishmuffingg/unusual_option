import { SectionPayload } from '@/lib/types'
import { SectionCharts } from './SectionCharts'
import { FocusTickerBlocks } from './FocusTickerBlocks'
import { SummaryTable } from './SummaryTable'
import { ContractChangesTable } from './ContractChangesTable'
import { RefreshDeltaPanel } from './RefreshDeltaPanel'

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
    ticker_rank: Array<{ ticker: string; premium_delta: number; volume_delta: number; contract_count_delta: number }>
    contract_changes: Array<Record<string, string | number>>
  }
}

export function SectionPanel({ title, subtitle, section, focusTitle, refreshDelta }: Props) {
  return (
    <section className="space-y-4 rounded-2xl border border-slate-800 bg-slate-950/50 p-4">
      <div>
        <h2 className="text-lg font-semibold text-cyan-300">{title}</h2>
        <p className="text-xs text-slate-400">{subtitle}</p>
      </div>
      {section.key === 'refreshed' && refreshDelta && <RefreshDeltaPanel data={refreshDelta} />}
      <SummaryTable title={`${title} ticker 排行`} data={section.summary} />
      <SectionCharts section={section} tickerRank={section.key === 'refreshed' ? refreshDelta?.ticker_rank : []} />
      {section.key === 'refreshed' && refreshDelta ? (
        <ContractChangesTable title="本次刷新合约明细（含新增/持续/扩大/消失）" rows={refreshDelta.contract_changes} useDeltaRows />
      ) : (
        <ContractChangesTable title={`${title} 合约明细`} rows={section.contracts} />
      )}
      <FocusTickerBlocks blocks={section.focus_blocks} title={focusTitle} variant={section.key} />
    </section>
  )
}
