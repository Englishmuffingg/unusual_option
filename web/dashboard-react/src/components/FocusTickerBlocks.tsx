import { FocusBlock } from '@/lib/types'

type Variant = 'refreshed' | 'today_new' | 'overall' | 'inactive_helper'
type Props = { blocks: FocusBlock[]; title: string; variant?: Variant; oiDeltaByTicker?: Record<string, number> }

function blockHint(variant: Variant) {
  if (variant === 'refreshed') return '璇箟锛氬垰鍒氳繖涓€杞€滄柊杩涘叆缁撴灉闆嗏€濈殑鍙樺寲'
  if (variant === 'today_new') return '语义：相对于最近一次有效变化快照新增'
  if (variant === 'inactive_helper') return '语义：相对于最近一次有效变化快照未继续被异常捕捉'
  return '璇箟锛氬綋鍓嶅紓甯告睜鏁翠綋缁撴瀯涓庢寔缁椿璺冨眰'
}

export function FocusTickerBlocks({ blocks, title, variant = 'overall', oiDeltaByTicker = {} }: Props) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-amber-800">{title}</h3>
        <span className="text-[11px] text-slate-500">{blockHint(variant)}</span>
      </div>
      <div className="grid gap-3 lg:grid-cols-2">
        {blocks.map((b) => (
          <div key={b.ticker} className="rounded-lg border border-amber-200 bg-amber-50 p-3">
            <div className="mb-2 flex items-center justify-between">
              <div className="text-lg font-semibold">{b.ticker}</div>
              <span className="rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-800">{b.flow_desc}</span>
            </div>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>Call鍗犳瘮: {(b.call_premium_pct * 100).toFixed(1)}%</div>
              <div>Put鍗犳瘮: {(b.put_premium_pct * 100).toFixed(1)}%</div>
              <div>{variant === 'refreshed' ? '鏈疆鍒锋柊鏂板' : '鏈鍒锋柊鏂板'}: {b.refreshed_rows}</div>
              <div>{variant === 'today_new' ? '浠婃棩绱鏂板' : '浠婃棩鏂板'}: {b.new_rows}</div>
              {variant === 'refreshed' && <div>鏈疆 OI 鍙樺寲: {Number(oiDeltaByTicker[b.ticker] || 0).toLocaleString('zh-CN')}</div>}
              <div>median_dte: {b.median_dte}</div>
              <div>avg/max ratio: {b.avg_ratio} / {b.max_ratio}</div>
            </div>
            <div className="mt-2 text-xs text-slate-700">
              涓昏strike锛?
              {b.top_strikes.slice(0, 3).map((s) => `${s.option_type} ${s.strike}`).join(' / ') || '-'}
            </div>
            <div className="mt-2 text-xs text-slate-500">
              重点到期：{String(b.top_contracts[0]?.expiration_date || '-')} · 顶部合约：{String(b.top_contracts[0]?.contract_display_name || '-')}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

