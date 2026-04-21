import { Bar, BarChart, Cell, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from 'recharts'
import { SectionPayload } from '@/lib/types'

type Props = {
  section: SectionPayload
}

const colors = ['#22d3ee', '#34d399', '#f59e0b', '#a78bfa', '#f43f5e', '#eab308']

export function SectionCharts({ section }: Props) {
  const diverging = section.summary.slice(0, 10).map((r) => ({ ticker: r.ticker, score: Number((r.bullish_score * 100).toFixed(2)) }))

  return (
    <div className="grid gap-4 xl:grid-cols-2">
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">主图：期限 × 方向 × 权利金</div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart>
              <XAxis type="number" dataKey="median_dte" name="median_dte" />
              <YAxis type="number" dataKey="bullish_score" name="bullish_score" />
              <Tooltip cursor={{ strokeDasharray: '3 3' }} />
              <Scatter data={section.bubble} fill="#22d3ee" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-3">
        <div className="mb-2 text-sm font-semibold">方向概览（右偏多 / 左偏防守）</div>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={diverging} layout="vertical" margin={{ left: 20 }}>
              <XAxis type="number" />
              <YAxis type="category" dataKey="ticker" width={60} />
              <Tooltip />
              <Bar dataKey="score">
                {diverging.map((entry, idx) => (
                  <Cell key={entry.ticker} fill={entry.score >= 0 ? colors[idx % colors.length] : '#f97316'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
