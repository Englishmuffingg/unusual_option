import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { SectionPayload } from '@/lib/types'

type Props = {
  section: SectionPayload
  tickerRank?: Array<{ ticker: string; premium_delta: number }>
}

const POS = '#22c55e'
const NEG = '#f97316'

export function SectionCharts({ section, tickerRank = [] }: Props) {
  const directionData = section.summary.slice(0, 10).map((r) => ({ ticker: r.ticker, score: Number((r.bullish_score * 100).toFixed(1)) }))

  const dteData = useMemo(() => {
    const bucket: Record<string, number> = {}
    for (const item of section.dte_profile) {
      bucket[item.dte_bucket] = (bucket[item.dte_bucket] || 0) + Number(item.total_est_premium || 0)
    }
    return Object.entries(bucket).map(([dte_bucket, total_est_premium]) => ({ dte_bucket, total_est_premium }))
  }, [section.dte_profile])

  const ticker = section.summary[0]?.ticker
  const pointsRaw = section.contracts
    .filter((r) => String(r.ticker) === ticker)
    .slice(0, 180)
    .map((r) => ({
      strike: Number(r.strike || 0),
      expiration: String(r.expiration_date || ''),
      premium: Number(r.estimated_premium || 0),
      option_type: String(r.option_type || '-'),
    }))

  const expIndex = Array.from(new Set(pointsRaw.map((p) => p.expiration)))
  const points = pointsRaw.map((p) => ({ ...p, expIdx: expIndex.indexOf(p.expiration) }))

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">{section.key === 'refreshed' ? '本次刷新 ticker 变化排行' : '方向结构（右偏多 / 左偏防守）'}</div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={section.key === 'refreshed' && tickerRank.length > 0 ? tickerRank.slice(0, 12) : directionData} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" />
              <YAxis type="category" dataKey="ticker" width={60} />
              <Tooltip />
              <Bar dataKey={section.key === 'refreshed' ? 'premium_delta' : 'score'}>
                {(section.key === 'refreshed' && tickerRank.length > 0 ? tickerRank : directionData).slice(0, 12).map((entry) => (
                  <Cell key={entry.ticker} fill={Number((entry as any).premium_delta ?? (entry as any).score) >= 0 ? POS : NEG} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">{ticker ? `${ticker} 的 strike × expiration` : '期限结构'}</div>
        <div className="h-72">
          {ticker ? (
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis type="number" dataKey="strike" name="strike" />
                <YAxis type="number" dataKey="expIdx" name="expiration" tickFormatter={(v) => expIndex[v] || ''} width={90} />
                <Tooltip formatter={(value: number, name: string, p: any) => (name === 'expIdx' ? p.payload.expiration : value)} />
                <Scatter data={points} fill="#38bdf8" />
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dteData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis dataKey="dte_bucket" interval={0} angle={-25} textAnchor="end" height={60} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="total_est_premium" fill="#22d3ee" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  )
}
