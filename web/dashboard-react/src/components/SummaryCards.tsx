import { Activity, BarChart3, Clock, Target } from 'lucide-react'

type Props = {
  cards: Record<string, number | string>
}

const items = [
  { k: 'refreshed_ticker_count', t: '本次刷新新增 ticker 数', i: Activity },
  { k: 'refreshed_contract_count', t: '本次刷新新增异常合约数', i: Target },
  { k: 'refreshed_total_premium', t: '本次刷新新增总 premium', i: BarChart3 },
  { k: 'today_new_ticker_count', t: '今日新增 ticker 数', i: Activity },
  { k: 'today_new_contract_count', t: '今日新增异常合约数', i: Target },
  { k: 'overall_ticker_count', t: '当前整体异常 ticker 数', i: BarChart3 },
  { k: 'overall_median_dte', t: '当前整体平均 median DTE', i: Clock },
  { k: 'overall_avg_ratio', t: '当前整体平均 avg_ratio', i: BarChart3 },
  { k: 'best_refreshed_ticker', t: '本次刷新最值得关注', i: Target },
  { k: 'best_today_new_ticker', t: '今日新增最值得关注', i: Target },
  { k: 'strongest_focus_ticker', t: 'Focus 最强标的', i: Activity },
]

export function SummaryCards({ cards }: Props) {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
      {items.map(({ k, t, i: Icon }) => (
        <div key={k} className="rounded-xl border border-slate-800 bg-slate-900/80 p-4">
          <div className="mb-2 flex items-center gap-2 text-xs text-slate-400">
            <Icon className="h-4 w-4" />
            {t}
          </div>
          <div className="text-2xl font-semibold text-emerald-300">{String(cards[k] ?? '-')}</div>
        </div>
      ))}
    </div>
  )
}
