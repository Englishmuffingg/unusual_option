import { DashboardResponse } from '@/lib/types'

function num(v: number | string, digits = 0) {
  const n = Number(v || 0)
  return n.toLocaleString('zh-CN', { maximumFractionDigits: digits })
}

type Props = {
  data: DashboardResponse['refresh_delta']
}

const items = [
  { key: 'contract_count_delta', label: '合约数量变化' },
  { key: 'options_volume_delta', label: '成交量变化' },
  { key: 'estimated_premium_delta', label: '估算权利金变化' },
  { key: 'open_interest_delta', label: 'OI 变化' },
  { key: 'new_contract_count', label: '新增合约数' },
  { key: 'persistent_contract_count', label: '延续合约数' },
] as const

export function RefreshDeltaPanel({ data }: Props) {
  return (
    <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-3">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-emerald-900">本轮变化量（基于最近一次有效变化对比）</h3>
        <div className="text-[11px] text-slate-600">
          当前: {data.snapshot_time || '-'} · 上一有效快照: {data.previous_snapshot_time || '-'}
        </div>
      </div>
      <div className="grid gap-2 md:grid-cols-3 xl:grid-cols-6">
        {items.map((item) => (
          <div key={item.key} className="rounded-lg bg-white p-2">
            <div className="text-[11px] text-slate-500">{item.label}</div>
            <div className="text-base font-semibold text-slate-900">{num(data[item.key], 2)}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
