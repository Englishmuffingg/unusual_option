import { useMemo } from 'react'
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from 'recharts'
import { SectionPayload } from '@/lib/types'
import { dteBucket, flowFromOptionType } from '@/lib/format'

type Props = {
  section: SectionPayload
  tickerRank?: Array<{ ticker: string; premium_delta: number }>
  onSelectTicker?: (ticker: string | null) => void
  onSelectFlow?: (flow: string | null) => void
  onSelectDte?: (bucket: string | null) => void
  onSelectStrikeExp?: (value: { strike: string; expiration: string } | null) => void
  selectedTicker?: string | null
  selectedFlow?: string | null
  selectedDte?: string | null
  selectedStrikeExp?: { strike: string; expiration: string } | null
  selectedContract?: string | null
}

const POS = '#2563eb'
const NEG = '#dc2626'
const MUTED = '#94a3b8'

export function SectionCharts({
  section,
  tickerRank = [],
  onSelectTicker,
  onSelectFlow,
  onSelectDte,
  onSelectStrikeExp,
  selectedTicker,
  selectedFlow,
  selectedDte,
  selectedStrikeExp,
  selectedContract,
}: Props) {
  const directionData = section.summary.slice(0, 10).map((r) => ({ ticker: r.ticker, score: Number((r.bullish_score * 100).toFixed(1)) }))

  const flowDist = useMemo(() => {
    if (section.key === 'refreshed') {
      const rows = section.contracts
      const m: Record<string, number> = { 新增: 0, 持续: 0, 扩大: 0, 消失: 0 }
      rows.forEach((r) => {
        const key = String(r.status || '持续')
        m[key] = (m[key] || 0) + 1
      })
      return Object.entries(m).map(([flow, count]) => ({ flow, count }))
    }

    const m: Record<string, number> = {}
    section.contracts.forEach((r) => {
      const flow = flowFromOptionType(String(r.option_type || ''))
      m[flow] = (m[flow] || 0) + 1
    })
    return Object.entries(m).map(([flow, count]) => ({ flow, count }))
  }, [section])

  const dteDist = useMemo(() => {
    const m: Record<string, number> = {}
    section.contracts.forEach((r) => {
      const bucket = dteBucket(Number(r.dte || 0))
      m[bucket] = (m[bucket] || 0) + 1
    })
    return Object.entries(m).map(([bucket, count]) => ({ bucket, count }))
  }, [section.contracts])

  const ticker = selectedTicker || section.summary[0]?.ticker
  const pointRaw = section.contracts
    .filter((r) => String(r.ticker) === ticker)
    .slice(0, 180)
    .map((r) => ({
      strike: String(r.strike || ''),
      expiration: String(r.expiration_date || ''),
      size: Number(r.options_volume || 0),
      contract_symbol: String(r.contract_symbol || ''),
    }))

  const strikeList = Array.from(new Set(pointRaw.map((p) => p.strike)))
  const expList = Array.from(new Set(pointRaw.map((p) => p.expiration)))
  const strikeExpPoints = pointRaw.map((p) => ({ ...p, x: strikeList.indexOf(p.strike), y: expList.indexOf(p.expiration) }))

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="rounded-xl border border-slate-200 bg-white p-3">
        <div className="mb-2 text-sm font-semibold text-slate-900">
          {section.key === 'refreshed' ? '本次刷新 ticker 变化排行' : section.key === 'today_new' ? '今日新增方向分布' : '整体方向结构'}
        </div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={section.key === 'refreshed' ? (tickerRank.length ? tickerRank.slice(0, 12) : directionData) : flowDist}
              layout="vertical"
              margin={{ left: 20 }}
              onClick={(st: any) => {
                if (section.key === 'refreshed') {
                  const t = st?.activePayload?.[0]?.payload?.ticker
                  onSelectTicker?.(t || null)
                } else {
                  const f = st?.activePayload?.[0]?.payload?.flow
                  onSelectFlow?.(f || null)
                }
              }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis type="number" stroke="#334155" />
              <YAxis type="category" dataKey={section.key === 'refreshed' ? 'ticker' : 'flow'} width={70} stroke="#334155" />
              <Tooltip />
              <Bar dataKey={section.key === 'refreshed' ? (tickerRank.length ? 'premium_delta' : 'score') : 'count'}>
                {(section.key === 'refreshed' ? (tickerRank.length ? tickerRank : directionData) : flowDist).map((entry: any) => {
                  const active = section.key === 'refreshed' ? selectedTicker === entry.ticker : selectedFlow === entry.flow
                  const baseColor = section.key === 'refreshed' ? (Number(entry.premium_delta ?? entry.score) >= 0 ? POS : NEG) : '#0ea5e9'
                  return <Cell key={entry.ticker || entry.flow} fill={active ? '#111827' : baseColor} opacity={active || (!selectedTicker && !selectedFlow) ? 1 : 0.45} />
                })}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-3">
        <div className="mb-2 text-sm font-semibold text-slate-900">期限结构（按合约数）</div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={dteDist} onClick={(st: any) => onSelectDte?.(st?.activePayload?.[0]?.payload?.bucket || null)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="bucket" stroke="#334155" />
              <YAxis stroke="#334155" />
              <Tooltip />
              <Bar dataKey="count">
                {dteDist.map((d) => <Cell key={d.bucket} fill={selectedDte === d.bucket ? '#111827' : '#10b981'} opacity={selectedDte && selectedDte !== d.bucket ? 0.45 : 1} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-xl border border-slate-200 bg-white p-3 xl:col-span-2">
        <div className="mb-2 text-sm font-semibold text-slate-900">{ticker ? `${ticker} 的 Strike × Expiration 点阵图（点击联动明细）` : 'Strike × Expiration'}</div>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart onClick={(st: any) => {
              const p = st?.activePayload?.[0]?.payload
              if (!p) return
              onSelectStrikeExp?.({ strike: p.strike, expiration: p.expiration })
            }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis type="number" dataKey="x" tickFormatter={(v) => strikeList[v] || ''} stroke="#334155" name="strike" />
              <YAxis type="number" dataKey="y" tickFormatter={(v) => expList[v] || ''} stroke="#334155" width={120} name="expiration" />
              <Tooltip formatter={(val: any, name: string, props: any) => {
                if (name === 'x') return props.payload.strike
                if (name === 'y') return props.payload.expiration
                return val
              }} />
              <Scatter data={strikeExpPoints} fill="#0284c7">
                {strikeExpPoints.map((p, idx) => {
                  const activeStrikeExp = selectedStrikeExp && selectedStrikeExp.strike === p.strike && selectedStrikeExp.expiration === p.expiration
                  const activeContract = selectedContract && selectedContract === p.contract_symbol
                  const active = activeStrikeExp || activeContract
                  const hasSelection = Boolean(selectedStrikeExp || selectedContract)
                  return <Cell key={`${p.strike}-${p.expiration}-${idx}`} fill={active ? '#111827' : '#0284c7'} opacity={hasSelection && !active ? 0.45 : 1} />
                })}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
        </div>
        {!ticker && <div className="text-xs text-slate-500">请先点击 ticker 排行图选择标的。</div>}
      </div>
    </div>
  )
}
