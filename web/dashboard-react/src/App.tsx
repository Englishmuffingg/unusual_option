import { useEffect, useMemo, useState } from 'react'
import { fetchDashboard } from './lib/api'
import { DashboardResponse } from './lib/types'
import { MetricGuide } from './components/MetricGuide'
import { SectionPanel } from './components/SectionPanel'
import { SummaryCards } from './components/SummaryCards'

export default function App() {
  const [table, setTable] = useState<'stock' | 'etf'>('stock')
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setErr(null)
    fetchDashboard(table)
      .then(setData)
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [table])

  const dbPath = useMemo(() => data?.db_path ?? '-', [data])

  return (
    <div className="mx-auto max-w-[1700px] space-y-6 bg-slate-100 p-4">
      <header className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">异常期权盘中监控面板（React 版）</h1>
            <p className="text-xs text-slate-500">数据库只读：{dbPath}</p>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <button className={`rounded px-3 py-1 ${table === 'stock' ? 'bg-cyan-100 text-cyan-900' : 'bg-slate-200 text-slate-700'}`} onClick={() => setTable('stock')}>Stock</button>
            <button className={`rounded px-3 py-1 ${table === 'etf' ? 'bg-cyan-100 text-cyan-900' : 'bg-slate-200 text-slate-700'}`} onClick={() => setTable('etf')}>ETF</button>
          </div>
        </div>
        {loading && <div className="text-sm text-slate-600">加载中...</div>}
        {err && <div className="text-sm text-rose-400">数据加载失败：{err}。请确认 Python API 已启动在 8000 端口。</div>}
        {data && <SummaryCards cards={data.cards} />}
      </header>

      {!loading && !data && !err && (
        <div className="rounded-xl border border-amber-300 bg-amber-50 p-4 text-sm text-amber-800">
          暂无数据：请先启动 API（`python run_dashboard_api.py --host 127.0.0.1 --port 8000`），再刷新页面。
        </div>
      )}

      {data && <MetricGuide items={data.metric_explanations} />}

      {data && (
        <SectionPanel
          title="第一主区：本次刷新新增（最高优先级）"
          subtitle="刚刚这一轮新增了什么：ticker 排行、方向、期限、重点合约"
          section={data.sections.refreshed}
          focusTitle="本次刷新 Focus 标的（SPY/GLD/QQQ/MSFT）"
          refreshDelta={data.refresh_delta}
        />
      )}

      {data && (
        <SectionPanel
          title="第二主区：今日新增"
          subtitle="今天到目前为止新增进入异常池的信号"
          section={data.sections.today_new}
          focusTitle="今日新增 Focus 标的"
        />
      )}

      {data && (
        <SectionPanel
          title="第三主区：当前整体异常池"
          subtitle="全量结构与持续热点，作为全局背景"
          section={data.sections.overall}
          focusTitle="整体 Focus 标的"
        />
      )}
    </div>
  )
}
