import { useEffect, useMemo, useState } from 'react'
import { fetchDashboard } from './lib/api'
import { DashboardResponse } from './lib/types'

type SourceType = 'etf' | 'stock'
type OptionFilter = 'all' | 'call' | 'put'
type DteFilter = 'all' | '0dte' | '1_3' | '4_10' | '11_30' | 'gt30'
type StatusFilter = 'all' | 'new' | 'continuing'
type TickerSort = 'new_volume' | 'total_volume' | 'vol_oi' | 'alphabet'

type RawContract = {
  id?: number
  recorded_at?: string
  is_new?: number | boolean
  is_refreshed?: number | boolean
  ticker?: string
  ticker_type?: string
  contract_symbol?: string
  contract_display_name?: string
  options_volume?: number
  open_interest?: number
  volume_to_open_interest_ratio?: number
  bid_price?: number
  ask_price?: number
  mid_price?: number
  expiration_date?: string
  strike?: number
  option_type?: string
  dte?: number
}

type ContractRow = {
  id: string
  recordedAt: string
  isNew: boolean
  isRefreshed: boolean
  ticker: string
  tickerType: string
  contractSymbol: string
  contractDisplayName: string
  optionsVolume: number
  openInterest: number
  volOi: number
  bidPrice: number
  askPrice: number
  midPrice: number
  expirationDate: string
  strike: number
  optionType: 'Call' | 'Put'
  dte: number
}

type StrikeBucket = { label: string; min: number; max: number; volume: number }

const CARD_CLASS = 'rounded-2xl border border-slate-200 bg-white p-4 shadow-sm'

const dteLabelMap: Record<DteFilter, string> = {
  all: '全部',
  '0dte': '0DTE',
  '1_3': '1到3天',
  '4_10': '4到10天',
  '11_30': '11到30天',
  gt30: '30天以上',
}

function toNumber(value: unknown): number {
  const n = Number(value)
  return Number.isFinite(n) ? n : 0
}

function normalizeRow(raw: RawContract, idx: number): ContractRow {
  const optionType = String(raw.option_type ?? '').toLowerCase().startsWith('p') ? 'Put' : 'Call'
  return {
    id: String(raw.id ?? `${raw.contract_symbol ?? 'na'}-${idx}`),
    recordedAt: raw.recorded_at ? String(raw.recorded_at) : '-',
    isNew: Boolean(raw.is_new),
    isRefreshed: Boolean(raw.is_refreshed),
    ticker: String(raw.ticker ?? '-').toUpperCase(),
    tickerType: String(raw.ticker_type ?? '-'),
    contractSymbol: String(raw.contract_symbol ?? '-'),
    contractDisplayName: String(raw.contract_display_name ?? raw.contract_symbol ?? '-'),
    optionsVolume: toNumber(raw.options_volume),
    openInterest: toNumber(raw.open_interest),
    volOi: toNumber(raw.volume_to_open_interest_ratio),
    bidPrice: toNumber(raw.bid_price),
    askPrice: toNumber(raw.ask_price),
    midPrice: toNumber(raw.mid_price),
    expirationDate: raw.expiration_date ? String(raw.expiration_date) : '-',
    strike: toNumber(raw.strike),
    optionType,
    dte: toNumber(raw.dte),
  }
}

function formatCompact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return `${Math.round(n)}`
}

function dteBucket(dte: number): DteFilter {
  if (dte <= 0) return '0dte'
  if (dte <= 3) return '1_3'
  if (dte <= 10) return '4_10'
  if (dte <= 30) return '11_30'
  return 'gt30'
}

function getBias(callShare: number, putShare: number): { text: string; cls: string } {
  if (putShare > 0.6) return { text: 'Put偏重', cls: 'text-rose-700 bg-rose-50 border-rose-200' }
  if (callShare > 0.6) return { text: 'Call偏重', cls: 'text-emerald-700 bg-emerald-50 border-emerald-200' }
  return { text: '流向混合', cls: 'text-amber-700 bg-amber-50 border-amber-200' }
}

function strikeBuckets(rows: ContractRow[]): StrikeBucket[] {
  const strikeMap = new Map<number, number>()
  rows.forEach((r) => strikeMap.set(r.strike, (strikeMap.get(r.strike) ?? 0) + r.optionsVolume))
  const unique = Array.from(strikeMap.keys()).sort((a, b) => a - b)
  if (unique.length <= 12) {
    return unique.map((k) => ({ label: String(k), min: k, max: k, volume: strikeMap.get(k) ?? 0 }))
  }

  const min = unique[0]
  const max = unique[unique.length - 1]
  const binCount = 10
  const width = (max - min) / binCount || 1
  const bins = Array.from({ length: binCount }, (_, i) => {
    const bMin = min + i * width
    const bMax = i === binCount - 1 ? max : min + (i + 1) * width
    return { label: `${bMin.toFixed(0)}-${bMax.toFixed(0)}`, min: bMin, max: bMax, volume: 0 }
  })

  rows.forEach((r) => {
    const idx = Math.min(Math.floor((r.strike - min) / width), binCount - 1)
    bins[idx].volume += r.optionsVolume
  })

  return bins.filter((b) => b.volume > 0)
}

export default function App() {
  const [source, setSource] = useState<SourceType>('etf')
  const [data, setData] = useState<DashboardResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [searchTicker, setSearchTicker] = useState('')
  const [tickerSort, setTickerSort] = useState<TickerSort>('new_volume')
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [optionFilter, setOptionFilter] = useState<OptionFilter>('all')
  const [dteFilter, setDteFilter] = useState<DteFilter>('all')
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')
  const [selectedContract, setSelectedContract] = useState<string | null>(null)
  const [selectedStrikeRange, setSelectedStrikeRange] = useState<{ min: number; max: number } | null>(null)

  const [tableSort, setTableSort] = useState<{ key: keyof ContractRow; dir: 'asc' | 'desc' }>({
    key: 'optionsVolume',
    dir: 'desc',
  })
  const [page, setPage] = useState(1)

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchDashboard(source)
      .then(setData)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [source])

  const rows = useMemo(() => {
    const raw = (data?.sections?.overall?.contracts ?? []) as RawContract[]
    return raw.map(normalizeRow)
  }, [data])

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      if (selectedTicker && row.ticker !== selectedTicker) return false
      if (optionFilter === 'call' && row.optionType !== 'Call') return false
      if (optionFilter === 'put' && row.optionType !== 'Put') return false
      if (dteFilter !== 'all' && dteBucket(row.dte) !== dteFilter) return false
      if (statusFilter === 'new' && !row.isNew) return false
      if (statusFilter === 'continuing' && row.isNew) return false
      if (selectedContract && row.contractSymbol !== selectedContract) return false
      if (selectedStrikeRange && (row.strike < selectedStrikeRange.min || row.strike > selectedStrikeRange.max)) return false
      return true
    })
  }, [rows, selectedTicker, optionFilter, dteFilter, statusFilter, selectedContract, selectedStrikeRange])

  const tickerStats = useMemo(() => {
    const m = new Map<string, { total: number; newVolume: number; volOi: number; call: number; put: number; count: number }>()
    rows.forEach((r) => {
      if (!r.ticker.toLowerCase().includes(searchTicker.trim().toLowerCase())) return
      const prev = m.get(r.ticker) ?? { total: 0, newVolume: 0, volOi: 0, call: 0, put: 0, count: 0 }
      prev.total += r.optionsVolume
      if (r.isNew) prev.newVolume += r.optionsVolume
      prev.volOi += r.volOi
      if (r.optionType === 'Call') prev.call += r.optionsVolume
      else prev.put += r.optionsVolume
      prev.count += 1
      m.set(r.ticker, prev)
    })
    let list = Array.from(m.entries()).map(([ticker, v]) => ({
      ticker,
      total: v.total,
      newVolume: v.newVolume,
      avgVolOi: v.count ? v.volOi / v.count : 0,
      callShare: v.total ? v.call / v.total : 0,
      putShare: v.total ? v.put / v.total : 0,
    }))

    if (tickerSort === 'new_volume') list = list.sort((a, b) => b.newVolume - a.newVolume)
    if (tickerSort === 'total_volume') list = list.sort((a, b) => b.total - a.total)
    if (tickerSort === 'vol_oi') list = list.sort((a, b) => b.avgVolOi - a.avgVolOi)
    if (tickerSort === 'alphabet') list = list.sort((a, b) => a.ticker.localeCompare(b.ticker))
    return list
  }, [rows, searchTicker, tickerSort])

  const summary = useMemo(() => {
    const totalVolume = filteredRows.reduce((s, r) => s + r.optionsVolume, 0)
    const callVolume = filteredRows.filter((r) => r.optionType === 'Call').reduce((s, r) => s + r.optionsVolume, 0)
    const putVolume = filteredRows.filter((r) => r.optionType === 'Put').reduce((s, r) => s + r.optionsVolume, 0)
    const avgVolOi = filteredRows.length ? filteredRows.reduce((s, r) => s + r.volOi, 0) / filteredRows.length : 0
    const maxVolOi = filteredRows.length ? Math.max(...filteredRows.map((r) => r.volOi)) : 0

    const dteDist = filteredRows.reduce<Record<DteFilter, number>>(
      (acc, r) => {
        acc[dteBucket(r.dte)] += r.optionsVolume
        return acc
      },
      { all: 0, '0dte': 0, '1_3': 0, '4_10': 0, '11_30': 0, gt30: 0 },
    )
    const topDte = (Object.entries(dteDist).filter(([k]) => k !== 'all') as [DteFilter, number][]).sort((a, b) => b[1] - a[1])[0]?.[0] ?? 'all'

    const strikeDist = new Map<number, number>()
    filteredRows.forEach((r) => strikeDist.set(r.strike, (strikeDist.get(r.strike) ?? 0) + r.optionsVolume))
    const topStrike = Array.from(strikeDist.entries()).sort((a, b) => b[1] - a[1])[0]?.[0]

    const expDist = new Map<string, number>()
    filteredRows.forEach((r) => expDist.set(r.expirationDate, (expDist.get(r.expirationDate) ?? 0) + r.optionsVolume))
    const topExp = Array.from(expDist.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] ?? '-'

    return {
      totalContracts: filteredRows.length,
      newContracts: filteredRows.filter((r) => r.isNew).length,
      continuingContracts: filteredRows.filter((r) => !r.isNew || r.isRefreshed).length,
      totalVolume,
      callShare: totalVolume ? callVolume / totalVolume : 0,
      putShare: totalVolume ? putVolume / totalVolume : 0,
      avgVolOi,
      maxVolOi,
      topDte,
      topStrike,
      topExp,
    }
  }, [filteredRows])

  const newRows = useMemo(
    () => filteredRows.filter((r) => r.isNew).sort((a, b) => b.optionsVolume - a.optionsVolume).slice(0, 8),
    [filteredRows],
  )
  const continuingRows = useMemo(
    () => filteredRows.filter((r) => !r.isNew || r.isRefreshed).sort((a, b) => b.optionsVolume - a.optionsVolume).slice(0, 8),
    [filteredRows],
  )

  const dteChartData = useMemo(() => {
    const labels: DteFilter[] = ['0dte', '1_3', '4_10', '11_30', 'gt30']
    const totalBy = new Map<DteFilter, number>()
    labels.forEach((k) => totalBy.set(k, 0))
    filteredRows.forEach((r) => totalBy.set(dteBucket(r.dte), (totalBy.get(dteBucket(r.dte)) ?? 0) + r.optionsVolume))
    const max = Math.max(...labels.map((k) => totalBy.get(k) ?? 0), 1)
    return labels.map((k) => ({ key: k, label: dteLabelMap[k], value: totalBy.get(k) ?? 0, max }))
  }, [filteredRows])

  const strikeChartData = useMemo(() => strikeBuckets(filteredRows), [filteredRows])

  const signalChips = useMemo(() => {
    const chips: string[] = []
    if (summary.putShare > 0.7) chips.push('Put 占主导')
    else if (summary.callShare > 0.7) chips.push('Call 占主导')
    else chips.push('流向混合')

    if (summary.topDte === '0dte' || summary.topDte === '1_3') chips.push('短期 DTE 集中')
    if ((dteChartData.find((d) => d.key === 'gt30')?.value ?? 0) / Math.max(summary.totalVolume, 1) > 0.2) chips.push('存在更长期合约')
    if (summary.maxVolOi >= 3) chips.push('存在高 Vol/OI 合约')

    const strikeTop2 = [...strikeChartData].sort((a, b) => b.volume - a.volume).slice(0, 2).reduce((s, b) => s + b.volume, 0)
    if (summary.totalVolume > 0) {
      if (strikeTop2 / summary.totalVolume > 0.65) chips.push('Strike 分布集中')
      else chips.push('Strike 分布分散')
    }
    return chips
  }, [summary, dteChartData, strikeChartData])

  const activeFilters = useMemo(() => {
    const list: Array<{ key: string; label: string; clear: () => void }> = []
    if (selectedTicker) list.push({ key: 'ticker', label: `标的：${selectedTicker}`, clear: () => setSelectedTicker(null) })
    if (optionFilter !== 'all') list.push({ key: 'option', label: `方向：${optionFilter === 'call' ? '仅Call' : '仅Put'}`, clear: () => setOptionFilter('all') })
    if (dteFilter !== 'all') list.push({ key: 'dte', label: `DTE：${dteLabelMap[dteFilter]}`, clear: () => setDteFilter('all') })
    if (statusFilter !== 'all') list.push({ key: 'status', label: `状态：${statusFilter === 'new' ? '仅新增' : '仅延续'}`, clear: () => setStatusFilter('all') })
    if (selectedContract) list.push({ key: 'contract', label: `合约：${selectedContract}`, clear: () => setSelectedContract(null) })
    if (selectedStrikeRange) list.push({ key: 'strike', label: `Strike：${selectedStrikeRange.min.toFixed(0)}-${selectedStrikeRange.max.toFixed(0)}`, clear: () => setSelectedStrikeRange(null) })
    return list
  }, [selectedTicker, optionFilter, dteFilter, statusFilter, selectedContract, selectedStrikeRange])

  const sortedTableRows = useMemo(() => {
    const copy = [...filteredRows]
    copy.sort((a, b) => {
      const va = a[tableSort.key]
      const vb = b[tableSort.key]
      if (typeof va === 'number' && typeof vb === 'number') return tableSort.dir === 'asc' ? va - vb : vb - va
      return tableSort.dir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va))
    })
    return copy
  }, [filteredRows, tableSort])

  const pageSize = 20
  const totalPages = Math.max(1, Math.ceil(sortedTableRows.length / pageSize))
  const pagedRows = sortedTableRows.slice((page - 1) * pageSize, page * pageSize)

  useEffect(() => setPage(1), [filteredRows.length])

  const clearAll = () => {
    setSearchTicker('')
    setTickerSort('new_volume')
    setSelectedTicker(null)
    setOptionFilter('all')
    setDteFilter('all')
    setStatusFilter('all')
    setSelectedContract(null)
    setSelectedStrikeRange(null)
  }

  const onSortHeader = (key: keyof ContractRow) => {
    setTableSort((prev) => ({ key, dir: prev.key === key && prev.dir === 'desc' ? 'asc' : 'desc' }))
  }

  return (
    <div className="mx-auto max-w-[1500px] space-y-4 bg-white p-4 text-slate-900">
      <section className={CARD_CLASS}>
        <h1 className="text-2xl font-bold">异常期权成交监控（异常流阅读器）</h1>
        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-6">
          <div className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">数据源</div>
            <div className="inline-flex rounded-xl border border-slate-300 p-1">
              <button className={`rounded-lg px-3 py-1 ${source === 'etf' ? 'bg-slate-900 text-white' : 'text-slate-700'}`} onClick={() => setSource('etf')}>ETF</button>
              <button className={`rounded-lg px-3 py-1 ${source === 'stock' ? 'bg-slate-900 text-white' : 'text-slate-700'}`} onClick={() => setSource('stock')}>Stock</button>
            </div>
          </div>
          <label className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">搜索标的</div>
            <input value={searchTicker} onChange={(e) => setSearchTicker(e.target.value)} placeholder="请输入 ticker" className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm" />
          </label>
          <label className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">排序方式</div>
            <select value={tickerSort} onChange={(e) => setTickerSort(e.target.value as TickerSort)} className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm">
              <option value="new_volume">按新增成交量降序</option>
              <option value="total_volume">按总成交量降序</option>
              <option value="vol_oi">按 Vol/OI 降序</option>
              <option value="alphabet">按字母排序</option>
            </select>
          </label>
          <label className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">期权方向</div>
            <select value={optionFilter} onChange={(e) => setOptionFilter(e.target.value as OptionFilter)} className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm">
              <option value="all">全部</option>
              <option value="call">仅 Call</option>
              <option value="put">仅 Put</option>
            </select>
          </label>
          <label className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">DTE 筛选</div>
            <select value={dteFilter} onChange={(e) => setDteFilter(e.target.value as DteFilter)} className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm">
              <option value="all">全部</option>
              <option value="0dte">0DTE</option>
              <option value="1_3">1到3天</option>
              <option value="4_10">4到10天</option>
              <option value="11_30">11到30天</option>
              <option value="gt30">30天以上</option>
            </select>
          </label>
          <div className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">状态筛选</div>
            <div className="flex gap-2">
              <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value as StatusFilter)} className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm">
                <option value="all">全部</option>
                <option value="new">仅新增</option>
                <option value="continuing">仅延续/已刷新</option>
              </select>
              <button onClick={clearAll} className="rounded-xl border border-slate-300 px-3 text-sm hover:bg-slate-50">清空</button>
            </div>
          </div>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          {activeFilters.map((f) => (
            <button key={f.key} onClick={f.clear} className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs">{f.label} ×</button>
          ))}
        </div>
      </section>

      {loading && <section className={CARD_CLASS}>加载中...</section>}
      {error && <section className={CARD_CLASS + ' text-rose-600'}>加载失败：{error}</section>}

      {!loading && !error && rows.length === 0 && (
        <section className={CARD_CLASS}>当前数据源暂无匹配结果。</section>
      )}

      {!loading && !error && rows.length > 0 && (
        <>
          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">全局概览</h2>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
              <MetricCard label="新增合约数" value={summary.newContracts} />
              <MetricCard label="延续合约数" value={summary.continuingContracts} />
              <MetricCard label="可见合约总数" value={summary.totalContracts} />
              <MetricCard label="Put 占比" value={`${(summary.putShare * 100).toFixed(1)}%`} />
              <MetricCard label="Call 占比" value={`${(summary.callShare * 100).toFixed(1)}%`} />
              <MetricCard label="当前总成交量" value={formatCompact(summary.totalVolume)} />
            </div>
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg font-semibold">标的选择</h2>
              <button onClick={() => setSelectedTicker(null)} className="rounded-lg border border-slate-300 px-3 py-1 text-sm">清除标的选择</button>
            </div>
            <div className="flex flex-wrap gap-2">
              {tickerStats.map((item) => {
                const bias = getBias(item.callShare, item.putShare)
                return (
                  <button
                    key={item.ticker}
                    onClick={() => setSelectedTicker(item.ticker)}
                    className={`rounded-xl border px-3 py-2 text-left text-sm ${selectedTicker === item.ticker ? 'border-slate-900 bg-slate-100' : `border-slate-200 ${bias.cls}`}`}
                  >
                    <div className="font-semibold">{item.ticker}</div>
                    <div className="text-xs">+{formatCompact(item.newVolume)} / 总{formatCompact(item.total)}</div>
                  </button>
                )
              })}
            </div>
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">当前标的概览</h2>
            {filteredRows.length === 0 ? (
              <div className="text-sm text-slate-500">当前筛选下没有匹配数据。</div>
            ) : (
              <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6 text-sm">
                <Fact label="当前标的" value={selectedTicker ?? '全局视图'} />
                <Fact label="平均 Vol/OI" value={summary.avgVolOi.toFixed(2)} />
                <Fact label="最高 Vol/OI" value={summary.maxVolOi.toFixed(2)} />
                <Fact label="最活跃到期日" value={summary.topExp} />
                <Fact label="最活跃 Strike" value={summary.topStrike ? String(summary.topStrike) : '-'} />
                <Fact label="最集中的 DTE" value={dteLabelMap[summary.topDte]} />
              </div>
            )}
          </section>

          <section className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <ActivityPanel title="新增活动" rows={newRows} onSelect={setSelectedContract} selected={selectedContract} />
            <ActivityPanel title="延续活动" rows={continuingRows} onSelect={setSelectedContract} selected={selectedContract} />
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">结构分布图</h2>
            <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
              <div>
                <div className="mb-2 font-medium">Call / Put 构成</div>
                <div className="h-8 overflow-hidden rounded-full border border-slate-200">
                  <button style={{ width: `${summary.callShare * 100}%` }} className="h-full bg-emerald-500 text-xs text-white" onClick={() => setOptionFilter('call')}>Call {(summary.callShare * 100).toFixed(1)}%</button>
                  <button style={{ width: `${summary.putShare * 100}%` }} className="h-full bg-rose-500 text-xs text-white" onClick={() => setOptionFilter('put')}>Put {(summary.putShare * 100).toFixed(1)}%</button>
                </div>
                <button onClick={() => setOptionFilter('all')} className="mt-2 text-xs text-slate-500 underline">清除方向筛选</button>
              </div>

              <div>
                <div className="mb-2 font-medium">DTE 分布</div>
                <div className="space-y-2">
                  {dteChartData.map((d) => (
                    <button key={d.key} onClick={() => setDteFilter(d.key)} className="block w-full text-left text-xs">
                      <div className="mb-1 flex justify-between"><span>{d.label}</span><span>{formatCompact(d.value)}</span></div>
                      <div className="h-2 rounded bg-slate-100">
                        <div className={`h-full rounded ${dteFilter === d.key ? 'bg-slate-900' : 'bg-sky-500'}`} style={{ width: `${(d.value / d.max) * 100}%` }} />
                      </div>
                    </button>
                  ))}
                </div>
                <button onClick={() => setDteFilter('all')} className="mt-2 text-xs text-slate-500 underline">清除 DTE 筛选</button>
              </div>

              <div>
                <div className="mb-2 font-medium">Strike 分布</div>
                <div className="space-y-2">
                  {strikeChartData.map((b) => {
                    const maxV = Math.max(...strikeChartData.map((v) => v.volume), 1)
                    const active = selectedStrikeRange && Math.abs(selectedStrikeRange.min - b.min) < 0.001 && Math.abs(selectedStrikeRange.max - b.max) < 0.001
                    return (
                      <button key={b.label} onClick={() => setSelectedStrikeRange({ min: b.min, max: b.max })} className="block w-full text-left text-xs">
                        <div className="mb-1 flex justify-between"><span>{b.label}</span><span>{formatCompact(b.volume)}</span></div>
                        <div className="h-2 rounded bg-slate-100"><div className={`h-full rounded ${active ? 'bg-slate-900' : 'bg-violet-500'}`} style={{ width: `${(b.volume / maxV) * 100}%` }} /></div>
                      </button>
                    )
                  })}
                </div>
                <button onClick={() => setSelectedStrikeRange(null)} className="mt-2 text-xs text-slate-500 underline">清除 Strike 筛选</button>
              </div>
            </div>
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">信号提示</h2>
            <div className="flex flex-wrap gap-2">
              {signalChips.map((s) => (
                <span key={s} className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-sm">{s}</span>
              ))}
            </div>
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg font-semibold">明细验证表格</h2>
              <div className="text-sm text-slate-500">共 {sortedTableRows.length} 条</div>
            </div>
            {sortedTableRows.length === 0 ? (
              <div className="text-sm text-slate-500">当前筛选下没有匹配数据。</div>
            ) : (
              <>
                <div className="max-h-[520px] overflow-auto rounded-xl border border-slate-200">
                  <table className="min-w-full text-xs">
                    <thead className="sticky top-0 bg-slate-100">
                      <tr>
                        {[
                          ['recordedAt', 'recorded_at'],
                          ['ticker', 'ticker'],
                          ['contractDisplayName', 'contract_display_name'],
                          ['optionType', 'option_type'],
                          ['expirationDate', 'expiration_date'],
                          ['strike', 'strike'],
                          ['dte', 'dte'],
                          ['optionsVolume', 'options_volume'],
                          ['openInterest', 'open_interest'],
                          ['volOi', 'Vol/OI'],
                          ['bidPrice', 'bid_price'],
                          ['askPrice', 'ask_price'],
                          ['midPrice', 'mid_price'],
                          ['isNew', 'is_new'],
                          ['isRefreshed', 'is_refreshed'],
                        ].map(([key, label]) => (
                          <th key={key} onClick={() => onSortHeader(key as keyof ContractRow)} className="cursor-pointer border-b border-slate-200 px-2 py-2 text-left font-semibold">{label}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {pagedRows.map((r) => (
                        <tr key={r.id} className={`border-b border-slate-100 hover:bg-sky-50 ${selectedContract === r.contractSymbol ? 'bg-amber-50' : ''}`}>
                          <td className="px-2 py-1">{r.recordedAt}</td>
                          <td className="px-2 py-1">{r.ticker}</td>
                          <td className="px-2 py-1">{r.contractDisplayName}</td>
                          <td className={`px-2 py-1 ${r.optionType === 'Put' ? 'text-rose-600' : 'text-emerald-600'}`}>{r.optionType}</td>
                          <td className="px-2 py-1">{r.expirationDate}</td>
                          <td className="px-2 py-1">{r.strike}</td>
                          <td className="px-2 py-1">{r.dte}</td>
                          <td className="px-2 py-1">{formatCompact(r.optionsVolume)}</td>
                          <td className="px-2 py-1">{formatCompact(r.openInterest)}</td>
                          <td className="px-2 py-1">{r.volOi.toFixed(2)}</td>
                          <td className="px-2 py-1">{r.bidPrice.toFixed(2)}</td>
                          <td className="px-2 py-1">{r.askPrice.toFixed(2)}</td>
                          <td className="px-2 py-1">{r.midPrice.toFixed(2)}</td>
                          <td className="px-2 py-1">{r.isNew ? '是' : '否'}</td>
                          <td className="px-2 py-1">{r.isRefreshed ? '是' : '否'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="mt-3 flex items-center justify-end gap-2 text-sm">
                  <button disabled={page <= 1} className="rounded border border-slate-300 px-2 py-1 disabled:opacity-50" onClick={() => setPage((p) => Math.max(1, p - 1))}>上一页</button>
                  <span>{page} / {totalPages}</span>
                  <button disabled={page >= totalPages} className="rounded border border-slate-300 px-2 py-1 disabled:opacity-50" onClick={() => setPage((p) => Math.min(totalPages, p + 1))}>下一页</button>
                </div>
              </>
            )}
          </section>
        </>
      )}
    </div>
  )
}

function MetricCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="mt-1 text-lg font-semibold">{value}</div>
    </div>
  )
}

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-slate-200 p-3">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="mt-1 font-semibold">{value}</div>
    </div>
  )
}

function ActivityPanel({
  title,
  rows,
  onSelect,
  selected,
}: {
  title: string
  rows: ContractRow[]
  onSelect: (s: string | null) => void
  selected: string | null
}) {
  const maxVolume = Math.max(...rows.map((r) => r.optionsVolume), 1)
  return (
    <section className={CARD_CLASS}>
      <h2 className="mb-3 text-lg font-semibold">{title}</h2>
      {rows.length === 0 ? (
        <div className="text-sm text-slate-500">暂无匹配合约</div>
      ) : (
        <div className="space-y-2">
          {rows.map((r) => (
            <button
              key={r.id}
              title={`OI ${r.openInterest} / Vol-OI ${r.volOi.toFixed(2)}`}
              onClick={() => onSelect(selected === r.contractSymbol ? null : r.contractSymbol)}
              className={`block w-full rounded-xl border p-2 text-left ${selected === r.contractSymbol ? 'border-slate-900 bg-slate-50' : 'border-slate-200'}`}
            >
              <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
                <div className="font-medium">{r.contractDisplayName}</div>
                <div className="text-slate-500">{formatCompact(r.optionsVolume)}</div>
              </div>
              <div className="mt-1 flex flex-wrap gap-2 text-xs text-slate-500">
                <span className={r.optionType === 'Put' ? 'text-rose-600' : 'text-emerald-600'}>{r.optionType}</span>
                <span>{r.expirationDate}</span>
                <span>Strike {r.strike}</span>
                <span>Vol/OI {r.volOi.toFixed(2)}</span>
              </div>
              <div className="mt-2 h-2 rounded bg-slate-100">
                <div
                  className={`h-full rounded ${r.optionType === 'Put' ? 'bg-rose-500' : 'bg-emerald-500'}`}
                  style={{ width: `${(r.optionsVolume / maxVolume) * 100}%` }}
                />
              </div>
            </button>
          ))}
        </div>
      )}
    </section>
  )
}
