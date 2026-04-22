import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
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
const COLOR_SET = ['#22d3ee', '#34d399', '#f59e0b', '#a78bfa', '#f43f5e']

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

  const flowDist = useMemo(() => {
    const c: Record<string, number> = {}
    for (const r of section.summary) {
      const key = r.flow_desc || '双向博弈'
      c[key] = (c[key] || 0) + 1
    }
    return Object.entries(c).map(([name, value]) => ({ name, value }))
  }, [section.summary])

  const dispersionData = section.summary.slice(0, 35).map((r) => ({
    ticker: r.ticker,
    dte: Number(r.median_dte || 0),
    ratio: Number(r.avg_ratio || 0),
    premium: Number(r.total_est_premium || 0),
  }))

  const hotspotData = section.summary.slice(0, 12).map((r) => ({
    ticker: r.ticker,
    premium: Number(r.total_est_premium || 0),
    ratio: Number(r.avg_ratio || 0),
  }))

  if (section.key === 'today_new') {
    return (
      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
          <div className="mb-2 text-sm font-semibold">今日新增扩散图（DTE × 异常程度）</div>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis type="number" dataKey="dte" name="median_dte" />
                <YAxis type="number" dataKey="ratio" name="avg_ratio" />
                <Tooltip />
                <Scatter data={dispersionData} fill="#22d3ee" />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
          <div className="mb-2 text-sm font-semibold">今日新增方向分布</div>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={flowDist} dataKey="value" nameKey="name" outerRadius={100} label>
                  {flowDist.map((entry, idx) => (
                    <Cell key={entry.name} fill={COLOR_SET[idx % COLOR_SET.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    )
  }

  if (section.key === 'overall') {
    return (
      <div className="grid gap-4 xl:grid-cols-2">
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
          <div className="mb-2 text-sm font-semibold">整体结构背景：持续热点（按估算权利金）</div>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={hotspotData} layout="vertical" margin={{ left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis type="number" />
                <YAxis type="category" dataKey="ticker" width={60} />
                <Tooltip />
                <Bar dataKey="premium" fill="#a78bfa" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
          <div className="mb-2 text-sm font-semibold">整体期限结构（DTE 背景层）</div>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dteData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis dataKey="dte_bucket" interval={0} angle={-25} textAnchor="end" height={60} />
                <YAxis />
                <Tooltip />
                <Bar dataKey="total_est_premium" fill="#34d399" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">本次刷新 ticker 变化排行</div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={tickerRank.length > 0 ? tickerRank.slice(0, 12) : directionData} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" />
              <YAxis type="category" dataKey="ticker" width={60} />
              <Tooltip />
              <Bar dataKey={tickerRank.length > 0 ? 'premium_delta' : 'score'}>
                {(tickerRank.length > 0 ? tickerRank : directionData).slice(0, 12).map((entry) => (
                  <Cell key={entry.ticker} fill={Number((entry as any).premium_delta ?? (entry as any).score) >= 0 ? POS : NEG} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">{ticker ? `${ticker} 刷新主战场：strike × expiration` : '期限结构'}</div>
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
