import { FocusBlock } from '@/lib/types'

type Props = { blocks: FocusBlock[]; title: string }

export function FocusTickerBlocks({ blocks, title }: Props) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
      <h3 className="mb-3 text-sm font-semibold text-amber-300">{title}</h3>
      <div className="grid gap-3 lg:grid-cols-2">
        {blocks.map((b) => (
          <div key={b.ticker} className="rounded-lg border border-amber-400/30 bg-slate-800 p-3">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-lg font-semibold">{b.ticker}</div>
              <span className="rounded bg-amber-500/20 px-2 py-0.5 text-xs text-amber-300">{b.flow_desc}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>Call占比: {(b.call_premium_pct * 100).toFixed(1)}%</div>
              <div>Put占比: {(b.put_premium_pct * 100).toFixed(1)}%</div>
              <div>本次刷新新增: {b.refreshed_rows}</div>
              <div>今日新增: {b.new_rows}</div>
              <div>median_dte: {b.median_dte}</div>
              <div>avg/max ratio: {b.avg_ratio} / {b.max_ratio}</div>
            </div>
            <div className="mt-2 text-xs text-slate-300">
              主要strike：
              {b.top_strikes.slice(0, 3).map((s) => `${s.option_type} ${s.strike}`).join(' / ') || '-'}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
