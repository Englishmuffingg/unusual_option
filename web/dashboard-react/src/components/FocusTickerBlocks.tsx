import { FocusBlock } from '@/lib/types'

type Props = { blocks: FocusBlock[]; title: string; variant?: 'refreshed' | 'today_new' | 'overall' }

function blockHint(variant: 'refreshed' | 'today_new' | 'overall') {
  if (variant === 'refreshed') return '语义：刚刚这一轮“新进入结果集”的变化'
  if (variant === 'today_new') return '语义：今天累计“新进入异常池”的扩散'
  return '语义：当前异常池整体结构与持续活跃层'
}

export function FocusTickerBlocks({ blocks, title, variant = 'overall' }: Props) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-amber-300">{title}</h3>
        <span className="text-[11px] text-slate-400">{blockHint(variant)}</span>
      </div>
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
              <div>{variant === 'refreshed' ? '本轮刷新新增' : '本次刷新新增'}: {b.refreshed_rows}</div>
              <div>{variant === 'today_new' ? '今日累计新增' : '今日新增'}: {b.new_rows}</div>
              <div>median_dte: {b.median_dte}</div>
              <div>avg/max ratio: {b.avg_ratio} / {b.max_ratio}</div>
            </div>
            <div className="mt-2 text-xs text-slate-300">
              主要strike：
              {b.top_strikes.slice(0, 3).map((s) => `${s.option_type} ${s.strike}`).join(' / ') || '-'}
            </div>
            <div className="mt-2 text-xs text-slate-400">
              重点到期：{String(b.top_contracts[0]?.expiration_date || '-')} ｜ 顶部合约：{String(b.top_contracts[0]?.contract_display_name || '-')}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
