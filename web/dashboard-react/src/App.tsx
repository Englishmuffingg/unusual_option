import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { fetchDashboard } from './lib/api'
import { DashboardResponse } from './lib/types'

type SourceType = 'etf' | 'stock'
type OptionFilter = 'all' | 'call' | 'put'
type DteFilter = 'all' | '0dte' | '1_3' | '4_10' | '11_30' | 'gt30'
type StatusFilter = 'all' | 'new' | 'continuing'
type TickerSort = 'new_volume' | 'total_volume' | 'vol_oi' | 'alphabet'

type RawContract = {
  id?: number
  snapshot_time?: string
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
  previous_options_volume?: number
  previous_open_interest?: number
  delta_volume?: number
  delta_open_interest?: number
  current_options_volume?: number
  current_open_interest?: number
  estimated_premium?: number
  status?: string
}

type RawInactiveContract = {
  contract_signature?: string
  ticker?: string
  contract_symbol?: string
  contract_display_name?: string
  option_type?: string
  expiration_date?: string
  strike?: number
  previous_options_volume?: number
  previous_open_interest?: number
}

type ContractRow = {
  id: string
  snapshotTime: string
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
  previousOptionsVolume: number | null
  previousOpenInterest: number | null
  deltaVolume: number | null
  deltaOpenInterest: number | null
  estimatedPremium: number
  status: string
}

type InactiveRow = {
  id: string
  ticker: string
  contractSymbol: string
  contractDisplayName: string
  optionType: 'Call' | 'Put'
  expirationDate: string
  strike: number
  previousOptionsVolume: number
  previousOpenInterest: number
}

type ChangeEventRow = {
  id: string
  eventTime: string
  eventType: 'NEW' | 'UPDATE' | 'INACTIVE'
  ticker: string
  contractSignature: string
  contractSymbol: string
  contractDisplayName: string
  optionType: 'Call' | 'Put'
  expirationDate: string
  strike: number
  dte: number
  previousOptionsVolume: number | null
  currentOptionsVolume: number | null
  deltaVolume: number | null
  previousOpenInterest: number | null
  currentOpenInterest: number | null
  deltaOpenInterest: number | null
  estimatedPremium: number | null
}

type SummaryTickerRow = {
  ticker: string
  windowStartTime: string
  windowEndTime: string
  totalNewCount: number
  totalUpdateCount: number
  totalInactiveCount: number
  cumulativeDeltaVolume: number
  cumulativeDeltaOpenInterest: number
  cumulativeEstimatedPremium: number
  putRatio: number
  callRatio: number
  eventCount: number
  lastEventTime: string
}

type CurrentOverview = {
  activeContractCount: number
  activeTickerCount: number
  topTicker: string
  putRatio: number
  callRatio: number
  dteDistribution: Array<{ label: string; value: number }>
  tickerVolumeTop: Array<{ ticker: string; totalVolume: number; rows: number }>
  topActiveContracts: ContractRow[]
}

type TableMode = 'current' | 'events' | 'summary'
type SummaryWindowMode = 'daily' | 'three_day'

type StrikeBucket = { label: string; min: number; max: number; volume: number }

const CARD_CLASS = 'rounded-2xl border border-slate-200 bg-white p-4 shadow-sm'
const ACTIVITY_TOP_N = 8
const DASHBOARD_POLL_MS = 60_000

const dteLabelMap: Record<DteFilter, string> = {
  all: '全部',
  '0dte': '0DTE',
  '1_3': '1-3天',
  '4_10': '4-10天',
  '11_30': '11-30天',
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
    snapshotTime: raw.snapshot_time ? String(raw.snapshot_time) : '-',
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
    previousOptionsVolume: raw.previous_options_volume == null ? null : toNumber(raw.previous_options_volume),
    previousOpenInterest: raw.previous_open_interest == null ? null : toNumber(raw.previous_open_interest),
    deltaVolume: raw.delta_volume == null ? null : toNumber(raw.delta_volume),
    deltaOpenInterest: raw.delta_open_interest == null ? null : toNumber(raw.delta_open_interest),
    estimatedPremium: toNumber(raw.estimated_premium),
    status: String(raw.status ?? (raw.is_new ? 'new' : raw.is_refreshed ? 'continued' : 'active')),
  }
}

function normalizeInactiveRow(raw: RawInactiveContract, idx: number): InactiveRow {
  const optionType = String(raw.option_type ?? '').toLowerCase().startsWith('p') ? 'Put' : 'Call'
  return {
    id: String(raw.contract_signature ?? raw.contract_symbol ?? `inactive-${idx}`),
    ticker: String(raw.ticker ?? '-').toUpperCase(),
    contractSymbol: String(raw.contract_symbol ?? '-'),
    contractDisplayName: String(raw.contract_display_name ?? raw.contract_symbol ?? '-'),
    optionType,
    expirationDate: raw.expiration_date ? String(raw.expiration_date) : '-',
    strike: toNumber(raw.strike),
    previousOptionsVolume: toNumber(raw.previous_options_volume),
    previousOpenInterest: toNumber(raw.previous_open_interest),
  }
}

function normalizeChangeEventRow(raw: Record<string, unknown>, idx: number): ChangeEventRow {
  const optionType = String(raw.option_type ?? '').toLowerCase().startsWith('p') ? 'Put' : 'Call'
  const eventType = String(raw.event_type ?? 'UPDATE').toUpperCase() as ChangeEventRow['eventType']
  return {
    id: String(raw.contract_signature ?? raw.contract_symbol ?? `event-${idx}`) + `-${idx}`,
    eventTime: raw.event_time ? String(raw.event_time) : '-',
    eventType,
    ticker: String(raw.ticker ?? '-').toUpperCase(),
    contractSignature: String(raw.contract_signature ?? '-'),
    contractSymbol: String(raw.contract_symbol ?? '-'),
    contractDisplayName: String(raw.contract_display_name ?? raw.contract_symbol ?? '-'),
    optionType,
    expirationDate: raw.expiration_date ? String(raw.expiration_date) : '-',
    strike: toNumber(raw.strike),
    dte: toNumber(raw.dte),
    previousOptionsVolume: raw.previous_options_volume == null ? null : toNumber(raw.previous_options_volume),
    currentOptionsVolume: raw.current_options_volume == null ? null : toNumber(raw.current_options_volume),
    deltaVolume: raw.delta_volume == null ? null : toNumber(raw.delta_volume),
    previousOpenInterest: raw.previous_open_interest == null ? null : toNumber(raw.previous_open_interest),
    currentOpenInterest: raw.current_open_interest == null ? null : toNumber(raw.current_open_interest),
    deltaOpenInterest: raw.delta_open_interest == null ? null : toNumber(raw.delta_open_interest),
    estimatedPremium: raw.estimated_premium == null ? null : toNumber(raw.estimated_premium),
  }
}

function normalizeSummaryTickerRow(raw: Record<string, unknown>): SummaryTickerRow {
  return {
    ticker: String(raw.ticker ?? '-').toUpperCase(),
    windowStartTime: raw.window_start_time ? String(raw.window_start_time) : '-',
    windowEndTime: raw.window_end_time ? String(raw.window_end_time) : '-',
    totalNewCount: toNumber(raw.total_new_count),
    totalUpdateCount: toNumber(raw.total_update_count),
    totalInactiveCount: toNumber(raw.total_inactive_count),
    cumulativeDeltaVolume: toNumber(raw.cumulative_delta_volume),
    cumulativeDeltaOpenInterest: toNumber(raw.cumulative_delta_open_interest),
    cumulativeEstimatedPremium: toNumber(raw.cumulative_estimated_premium),
    putRatio: toNumber(raw.put_ratio),
    callRatio: toNumber(raw.call_ratio),
    eventCount: toNumber(raw.event_count),
    lastEventTime: raw.last_event_time ? String(raw.last_event_time) : '-',
  }
}

function formatCompact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return `${Math.round(n)}`
}

function formatShortDate(dateStr: string): string {
  if (!dateStr || dateStr === '-') return '-'
  const parts = dateStr.split('-')
  if (parts.length >= 3) return `${parts[1]}-${parts[2]}`
  return dateStr
}

function formatShortTime(ts: string): string {
  if (!ts || ts === '-') return '-'
  const match = ts.match(/(\d{2}:\d{2})/)
  return match?.[1] ?? ts
}

function sortLabel(active: boolean, dir: 'asc' | 'desc'): string {
  if (!active) return ' ↕'
  return dir === 'asc' ? ' ↑' : ' ↓'
}

function dteBucket(dte: number): DteFilter {
  if (dte <= 0) return '0dte'
  if (dte <= 3) return '1_3'
  if (dte <= 10) return '4_10'
  if (dte <= 30) return '11_30'
  return 'gt30'
}

function getBias(callShare: number, putShare: number): { text: string; cls: string } {
  if (putShare > 0.6) return { text: 'Put鍋忛噸', cls: 'text-rose-700 bg-rose-50 border-rose-200' }
  if (callShare > 0.6) return { text: 'Call鍋忛噸', cls: 'text-emerald-700 bg-emerald-50 border-emerald-200' }
  return { text: '娴佸悜娣峰悎', cls: 'text-amber-700 bg-amber-50 border-amber-200' }
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

function normalizeOverview(raw: DashboardResponse | null): CurrentOverview {
  const overview = raw?.current_overview
  const dteDistribution = Array.isArray(overview?.dte_distribution)
    ? overview!.dte_distribution.map((row) => ({
        label: String((row.dte_bucket as string | undefined) ?? '-'),
        value: toNumber(row.total_volume),
      }))
    : []
  const tickerVolumeTop = Array.isArray(overview?.ticker_volume_top)
    ? overview!.ticker_volume_top.map((row) => ({
        ticker: String((row.ticker as string | undefined) ?? '-').toUpperCase(),
        totalVolume: toNumber(row.total_volume),
        rows: toNumber(row.rows),
      }))
    : []
  const topActiveContracts = Array.isArray(overview?.top_active_contracts)
    ? overview!.top_active_contracts.map((row, idx) => normalizeRow(row as RawContract, idx))
    : []
  return {
    activeContractCount: toNumber(overview?.active_contract_count),
    activeTickerCount: toNumber(overview?.active_ticker_count),
    topTicker: String(overview?.top_ticker ?? '-'),
    putRatio: toNumber(overview?.put_ratio),
    callRatio: toNumber(overview?.call_ratio),
    dteDistribution,
    tickerVolumeTop,
    topActiveContracts,
  }
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
  const [selectionSource, setSelectionSource] = useState<string | null>(null)
  const [tableMode, setTableMode] = useState<TableMode>('current')
  const [eventTypeFilter, setEventTypeFilter] = useState<'all' | 'NEW' | 'UPDATE' | 'INACTIVE'>('all')
  const [newExpanded, setNewExpanded] = useState(false)
  const [inactiveExpanded, setInactiveExpanded] = useState(false)
  const [newSort, setNewSort] = useState<'volume' | 'vol_oi' | 'time'>('volume')

  const [tableSort, setTableSort] = useState<{ key: keyof ContractRow; dir: 'asc' | 'desc' }>({
    key: 'optionsVolume',
    dir: 'desc',
  })
  const [summaryWindowMode, setSummaryWindowMode] = useState<SummaryWindowMode>('three_day')
  const [summarySort, setSummarySort] = useState<{ key: keyof SummaryTickerRow; dir: 'asc' | 'desc' }>({
    key: 'eventCount',
    dir: 'desc',
  })
  const [page, setPage] = useState(1)
  const fetchInFlightRef = useRef(false)

  const loadDashboard = useCallback(
    async (mode: 'initial' | 'background' = 'initial') => {
      if (fetchInFlightRef.current) return
      fetchInFlightRef.current = true
      if (mode === 'initial') {
        setLoading(true)
      }
      setError(null)
      try {
        const next = await fetchDashboard(source)
        setData(next)
      } catch (e) {
        setError(String(e))
      } finally {
        fetchInFlightRef.current = false
        if (mode === 'initial') {
          setLoading(false)
        }
      }
    },
    [source],
  )

  useEffect(() => {
    void loadDashboard('initial')

    const intervalId = window.setInterval(() => {
      if (document.visibilityState === 'visible') {
        void loadDashboard('background')
      }
    }, DASHBOARD_POLL_MS)

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void loadDashboard('background')
      }
    }

    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      window.clearInterval(intervalId)
      document.removeEventListener('visibilitychange', handleVisibilityChange)
      fetchInFlightRef.current = false
    }
  }, [loadDashboard])

  const rows = useMemo(() => {
    const raw = (data?.sections?.overall?.contracts ?? []) as RawContract[]
    return raw.map(normalizeRow)
  }, [data])

  const currentSnapshotDataRows = useMemo(() => {
    const raw = (data?.current_snapshot_rows ?? []) as RawContract[]
    return raw.map(normalizeRow)
  }, [data])

  const continuedDataRows = useMemo(() => {
    const raw = (data?.continued_rows ?? []) as RawContract[]
    return raw.map(normalizeRow)
  }, [data])

  const inactiveRows = useMemo(() => {
    const raw = (data?.sections?.inactive_helper?.contracts ?? []) as RawInactiveContract[]
    return raw.map(normalizeInactiveRow)
  }, [data])

  const changeFeedRows = useMemo(() => {
    const raw = (data?.change_feed ?? []) as Array<Record<string, unknown>>
    return raw.map(normalizeChangeEventRow)
  }, [data])

  const dailySummaryRows = useMemo(() => {
    const raw = (data?.daily_summary ?? []) as Array<Record<string, unknown>>
    return raw.map(normalizeSummaryTickerRow)
  }, [data])

  const threeDaySummaryRows = useMemo(() => {
    const raw = (data?.three_day_summary ?? []) as Array<Record<string, unknown>>
    return raw.map(normalizeSummaryTickerRow)
  }, [data])

  const currentOverview = useMemo(() => normalizeOverview(data), [data])

  const filteredRows = useMemo(() => {
    return rows.filter((row) => {
      if (selectedTicker && row.ticker !== selectedTicker) return false
      if (optionFilter === 'call' && row.optionType !== 'Call') return false
      if (optionFilter === 'put' && row.optionType !== 'Put') return false
      if (dteFilter !== 'all' && dteBucket(row.dte) !== dteFilter) return false
      if (statusFilter === 'new' && !row.isNew) return false
      if (statusFilter === 'continuing' && !row.isRefreshed) return false
      if (selectedStrikeRange && (row.strike < selectedStrikeRange.min || row.strike > selectedStrikeRange.max)) return false
      return true
    })
  }, [rows, selectedTicker, optionFilter, dteFilter, statusFilter, selectedStrikeRange])

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
      continuingContracts: filteredRows.filter((r) => r.isRefreshed).length,
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

  const behaviorSummary = useMemo(() => {
    const apiSummary = data?.summary
    if (!apiSummary) {
      return {
        currentTotal: summary.totalContracts,
        newCount: summary.newContracts,
        continuedCount: summary.continuingContracts,
        inactiveCount: inactiveRows.length,
        putRatio: summary.putShare,
        callRatio: summary.callShare,
        dominantTicker: selectedTicker ?? '-',
        dominantDteBucket: dteLabelMap[summary.topDte],
        dominantStrikeBucket: summary.topStrike ? String(summary.topStrike) : '-',
      }
    }
    return {
      currentTotal: toNumber(apiSummary.current_total),
      newCount: toNumber(apiSummary.new_count),
      continuedCount: toNumber(apiSummary.continued_count),
      inactiveCount: toNumber(apiSummary.inactive_count),
      putRatio: toNumber(apiSummary.put_ratio),
      callRatio: toNumber(apiSummary.call_ratio),
      dominantTicker: String(apiSummary.dominant_ticker ?? '-'),
      dominantDteBucket: String(apiSummary.dominant_dte_bucket ?? '-'),
      dominantStrikeBucket: String(apiSummary.dominant_strike_bucket ?? '-'),
    }
  }, [data, inactiveRows.length, selectedTicker, summary])

  const newRowsAll = useMemo(() => {
    const sorted = filteredRows.filter((r) => r.isNew)
    if (newSort === 'volume') return sorted.sort((a, b) => b.optionsVolume - a.optionsVolume)
    if (newSort === 'vol_oi') return sorted.sort((a, b) => b.volOi - a.volOi)
    return sorted.sort((a, b) => String(b.recordedAt).localeCompare(String(a.recordedAt)))
  }, [filteredRows, newSort])
  const newRows = useMemo(() => (newExpanded ? newRowsAll : newRowsAll.slice(0, ACTIVITY_TOP_N)), [newExpanded, newRowsAll])
  const continuingRows = useMemo(
    () =>
      continuedDataRows
        .filter((r) => {
          if (selectedTicker && r.ticker !== selectedTicker) return false
          if (optionFilter === 'call' && r.optionType !== 'Call') return false
          if (optionFilter === 'put' && r.optionType !== 'Put') return false
          if (dteFilter !== 'all' && dteBucket(r.dte) !== dteFilter) return false
          if (selectedStrikeRange && (r.strike < selectedStrikeRange.min || r.strike > selectedStrikeRange.max)) return false
          return true
        })
        .sort((a, b) => {
          const deltaA = Math.abs(a.deltaVolume ?? 0) + Math.abs(a.deltaOpenInterest ?? 0)
          const deltaB = Math.abs(b.deltaVolume ?? 0) + Math.abs(b.deltaOpenInterest ?? 0)
          return deltaB - deltaA
        })
        .slice(0, ACTIVITY_TOP_N),
    [continuedDataRows, selectedTicker, optionFilter, dteFilter, selectedStrikeRange],
  )

  const currentSnapshotRows = useMemo(
    () => (currentOverview.topActiveContracts.length ? currentOverview.topActiveContracts : currentSnapshotDataRows).slice(0, ACTIVITY_TOP_N),
    [currentOverview, currentSnapshotDataRows],
  )

  const filteredChangeFeed = useMemo(() => {
    return changeFeedRows.filter((row) => {
      if (selectedTicker && row.ticker !== selectedTicker) return false
      if (selectedContract && row.contractSymbol !== selectedContract) return false
      if (eventTypeFilter === 'all' && row.eventType === 'INACTIVE') return false
      if (eventTypeFilter !== 'all' && row.eventType !== eventTypeFilter) return false
      if (optionFilter === 'call' && row.optionType !== 'Call') return false
      if (optionFilter === 'put' && row.optionType !== 'Put') return false
      if (dteFilter !== 'all' && dteBucket(row.dte) !== dteFilter) return false
      if (selectedStrikeRange && (row.strike < selectedStrikeRange.min || row.strike > selectedStrikeRange.max)) return false
      return true
    })
  }, [changeFeedRows, selectedTicker, selectedContract, eventTypeFilter, optionFilter, dteFilter, selectedStrikeRange])

  const hiddenInactiveEventCount = useMemo(() => {
    if (eventTypeFilter !== 'all') return 0
    return changeFeedRows.filter((row) => {
      if (row.eventType !== 'INACTIVE') return false
      if (selectedTicker && row.ticker !== selectedTicker) return false
      if (selectedContract && row.contractSymbol !== selectedContract) return false
      if (optionFilter === 'call' && row.optionType !== 'Call') return false
      if (optionFilter === 'put' && row.optionType !== 'Put') return false
      if (dteFilter !== 'all' && dteBucket(row.dte) !== dteFilter) return false
      if (selectedStrikeRange && (row.strike < selectedStrikeRange.min || row.strike > selectedStrikeRange.max)) return false
      return true
    }).length
  }, [changeFeedRows, eventTypeFilter, selectedTicker, selectedContract, optionFilter, dteFilter, selectedStrikeRange])

  const comparisonText = useMemo(() => {
    const latest = data?.snapshot_meta?.latest_snapshot_time
    const previous = data?.snapshot_meta?.previous_snapshot_time
    if (!latest) return '当前基准：最近一次有效变化快照'
    if (!previous) return `当前基准：最新快照 ${formatShortTime(String(latest))}，此前暂无有效变化快照`
    return `当前基准：${formatShortTime(String(previous))} -> ${formatShortTime(String(latest))}（最近一次有效变化快照）`
  }, [data])

  const dashboardMetaText = useMemo(() => {
    const generatedAt = data?.metadata?.dashboard_generated_at ?? data?.dashboard_generated_at
    const snapshotTime = data?.metadata?.snapshot_time ?? data?.snapshot_meta?.snapshot_time
    return `快照时点：${snapshotTime ? formatShortTime(String(snapshotTime)) : '-'} · 生成时间：${generatedAt ? formatShortTime(String(generatedAt)) : '-'}`
  }, [data])

  const cacheStatusSummary = useMemo(() => {
    const status = String(data?.metadata?.cache_refresh_status ?? data?.snapshot_meta?.cache_refresh_status ?? 'unknown')
    const artifactKey = String(data?.metadata?.artifact_key ?? data?.artifact_key ?? '-')
    const rawRefreshId = String(data?.metadata?.raw_refresh_id ?? data?.snapshot_meta?.raw_refresh_id ?? '-')
    const currentRefreshId = String(data?.metadata?.current_refresh_id ?? data?.snapshot_meta?.current_refresh_id ?? '-')
    const cacheGeneratedAt = data?.metadata?.cache_generated_at ?? data?.snapshot_meta?.cache_generated_at ?? data?.dashboard_generated_at ?? null
    const lastError = data?.metadata?.cache_last_error ?? data?.snapshot_meta?.cache_last_error ?? null
    return {
      status,
      artifactKey,
      rawRefreshId,
      currentRefreshId,
      cacheGeneratedAt: cacheGeneratedAt ? String(cacheGeneratedAt) : '-',
      lastError: lastError ? String(lastError) : null,
    }
  }, [data])

  const dteChartData = useMemo(() => {
    const labels: DteFilter[] = ['0dte', '1_3', '4_10', '11_30', 'gt30']
    const totalBy = new Map<DteFilter, number>()
    labels.forEach((k) => totalBy.set(k, 0))
    filteredRows.forEach((r) => totalBy.set(dteBucket(r.dte), (totalBy.get(dteBucket(r.dte)) ?? 0) + r.optionsVolume))
    const max = Math.max(...labels.map((k) => totalBy.get(k) ?? 0), 1)
    return labels.map((k) => ({ key: k, label: dteLabelMap[k], value: totalBy.get(k) ?? 0, max }))
  }, [filteredRows])

  const strikeChartData = useMemo(() => {
    if (selectedTicker) {
      const strikeMap = new Map<number, number>()
      filteredRows.forEach((r) => strikeMap.set(r.strike, (strikeMap.get(r.strike) ?? 0) + r.optionsVolume))
      return Array.from(strikeMap.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([strike, volume]) => ({ label: String(strike), min: strike, max: strike, volume }))
        .sort((a, b) => a.min - b.min)
    }
    return strikeBuckets(filteredRows)
  }, [filteredRows, selectedTicker])

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
    if (optionFilter !== 'all') list.push({ key: 'option', label: `方向：${optionFilter === 'call' ? '仅 Call' : '仅 Put'}`, clear: () => setOptionFilter('all') })
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

  const summarySourceRows = useMemo(
    () => (summaryWindowMode === 'daily' ? dailySummaryRows : threeDaySummaryRows),
    [summaryWindowMode, dailySummaryRows, threeDaySummaryRows],
  )

  const filteredSummaryRows = useMemo(() => {
    return summarySourceRows.filter((row) => !selectedTicker || row.ticker === selectedTicker)
  }, [summarySourceRows, selectedTicker])

  const sortedSummaryRows = useMemo(() => {
    const copy = [...filteredSummaryRows]
    copy.sort((a, b) => {
      const va = a[summarySort.key]
      const vb = b[summarySort.key]
      if (typeof va === 'number' && typeof vb === 'number') {
        return summarySort.dir === 'asc' ? va - vb : vb - va
      }
      return summarySort.dir === 'asc' ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va))
    })
    return copy
  }, [filteredSummaryRows, summarySort])

  const eventTableRows = useMemo(() => filteredChangeFeed.slice(0, 200), [filteredChangeFeed])

  const pageSize = 20
  const totalTableRows =
    tableMode === 'current'
      ? sortedTableRows.length
      : tableMode === 'events'
        ? eventTableRows.length
        : sortedSummaryRows.length
  const totalPages = Math.max(1, Math.ceil(totalTableRows / pageSize))
  const pagedRows =
    tableMode === 'current'
      ? sortedTableRows.slice((page - 1) * pageSize, page * pageSize)
      : tableMode === 'events'
        ? eventTableRows.slice((page - 1) * pageSize, page * pageSize)
        : sortedSummaryRows.slice((page - 1) * pageSize, page * pageSize)

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
    setSelectionSource(null)
    setTableMode('current')
    setEventTypeFilter('all')
    setNewExpanded(false)
    setInactiveExpanded(false)
    setNewSort('volume')
    setSummaryWindowMode('three_day')
  }

  const onSortHeader = (key: keyof ContractRow) => {
    setTableSort((prev) => ({ key, dir: prev.key === key && prev.dir === 'desc' ? 'asc' : 'desc' }))
  }

  const onSummarySortHeader = (key: keyof SummaryTickerRow) => {
    setSummarySort((prev) => ({ key, dir: prev.key === key && prev.dir === 'desc' ? 'asc' : 'desc' }))
  }

  return (
    <div className="mx-auto max-w-[1500px] space-y-4 bg-white p-4 text-slate-900">
      <section className={CARD_CLASS}>
        <h1 className="text-2xl font-bold">异常期权成交监控（异常行为阅读器）</h1>
        <div className="mt-2 flex flex-wrap gap-2 text-xs">
          <span className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1">
            comparison mode: {data?.comparison_mode ?? 'effective_change_previous'}
          </span>
          <span className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1">
            auto refresh: {Math.round(DASHBOARD_POLL_MS / 1000)}s
          </span>
          <span
            className={`rounded-full border px-3 py-1 ${
              cacheStatusSummary.status === 'success'
                ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                : cacheStatusSummary.status === 'failed'
                  ? 'border-rose-200 bg-rose-50 text-rose-700'
                  : 'border-slate-300 bg-slate-50 text-slate-600'
            }`}
          >
            cache: {cacheStatusSummary.status}
          </span>
        </div>
        <div className="mt-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-600">
          <div className="flex flex-wrap gap-x-4 gap-y-1">
            <span>artifact: {cacheStatusSummary.artifactKey}</span>
            <span>raw refresh: {cacheStatusSummary.rawRefreshId}</span>
            <span>current refresh: {cacheStatusSummary.currentRefreshId}</span>
            <span>cache generated: {formatShortTime(cacheStatusSummary.cacheGeneratedAt)}</span>
          </div>
          {cacheStatusSummary.lastError ? (
            <div className="mt-2 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-rose-700">
              cache refresh error: {cacheStatusSummary.lastError}
            </div>
          ) : null}
        </div>
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
              <option value="1_3">1-3天</option>
              <option value="4_10">4-10天</option>
              <option value="11_30">11-30天</option>
              <option value="gt30">30天以上</option>
            </select>
          </label>
          <div className="lg:col-span-1">
            <div className="mb-1 text-sm text-slate-500">状态筛选</div>
            <div className="flex gap-2">
              <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value as StatusFilter)} className="w-full rounded-xl border border-slate-300 px-3 py-2 text-sm">
                <option value="all">全部</option>
                <option value="new">仅新增</option>
                <option value="continuing">仅延续</option>
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
        <section className={CARD_CLASS}>当前数据源暂时没有匹配结果。</section>
      )}

      {!loading && !error && rows.length > 0 && (
        <>
          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">全局概览</h2>
            <p className="mb-3 text-xs text-slate-500">{comparisonText}</p>
            <p className="mb-3 text-xs text-slate-400">{dashboardMetaText}</p>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
              <MetricCard label="当前异常合约总数" value={behaviorSummary.currentTotal} />
              <MetricCard label="本轮新增数" value={behaviorSummary.newCount} />
              <MetricCard label="本轮延续数" value={behaviorSummary.continuedCount} />
              <MetricCard label="本轮不再异常数" value={behaviorSummary.inactiveCount} />
              <MetricCard label="Put 占比" value={`${(behaviorSummary.putRatio * 100).toFixed(1)}%`} />
              <MetricCard label="Call 占比" value={`${(behaviorSummary.callRatio * 100).toFixed(1)}%`} />
            </div>
            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-3">
              <Fact label="当前主导 ticker" value={behaviorSummary.dominantTicker} />
              <Fact label="当前主导 DTE 区间" value={behaviorSummary.dominantDteBucket} />
              <Fact label="当前最活跃 strike 区间" value={behaviorSummary.dominantStrikeBucket} />
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
                    <div className="text-xs">新增 {formatCompact(item.newVolume)} / 总量 {formatCompact(item.total)}</div>
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
              <div className="grid grid-cols-2 gap-3 text-sm md:grid-cols-3 lg:grid-cols-6">
                <Fact label="当前标的" value={selectedTicker ?? '全局视图'} />
                <Fact label="平均 Vol/OI" value={summary.avgVolOi.toFixed(2)} />
                <Fact label="最高 Vol/OI" value={summary.maxVolOi.toFixed(2)} />
                <Fact label="最活跃到期日" value={summary.topExp} />
                <Fact label="最活跃 Strike" value={summary.topStrike ? String(summary.topStrike) : '-'} />
                <Fact label="最集中的 DTE" value={dteLabelMap[summary.topDte]} />
              </div>
            )}
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 className="text-lg font-semibold">最近变化流</h2>
                <p className="mt-1 text-xs text-slate-500">按时间倒序查看今日事件，默认聚焦 NEW / UPDATE；INACTIVE 仅作为辅助筛选。</p>
              </div>
              <div className="flex items-center gap-2 text-xs text-slate-500">
                <label>
                  事件类型：
                  <select value={eventTypeFilter} onChange={(e) => setEventTypeFilter(e.target.value as 'all' | 'NEW' | 'UPDATE' | 'INACTIVE')} className="ml-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-700">
                    <option value="all">全部</option>
                    <option value="NEW">NEW</option>
                    <option value="UPDATE">UPDATE</option>
                    <option value="INACTIVE">INACTIVE</option>
                  </select>
                </label>
              </div>
            </div>
            <ChangeFeedPanel
              rows={filteredChangeFeed.slice(0, 12)}
              hiddenInactiveCount={hiddenInactiveEventCount}
              onSelect={(row) => {
                setSelectedTicker(row.ticker || null)
                setSelectedContract(row.contractSymbol || null)
                setSelectionSource('最近变化流')
                setTableMode('events')
              }}
            />
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold">当前盘面概览</h2>
                <p className="mt-1 text-xs text-slate-500">当前 active contracts 的概览视图，强调盘面结构、主要标的和少量最强合约。</p>
              </div>
            </div>
            <CurrentOverviewPanel
              overview={currentOverview}
              rows={currentSnapshotRows}
              selected={selectedContract}
              onSelect={(contract) => {
                setSelectedContract(contract)
                setSelectionSource(contract ? '当前盘面概览' : null)
                setTableMode('current')
              }}
            />
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <div>
                <h2 className="text-lg font-semibold">新增活动</h2>
                <p className="mt-1 text-xs text-slate-500">相对于最近一次有效变化快照新增，默认显示前 {ACTIVITY_TOP_N} 条。</p>
              </div>
              <label className="text-xs text-slate-500">
                排序：
                <select value={newSort} onChange={(e) => setNewSort(e.target.value as 'volume' | 'vol_oi' | 'time')} className="ml-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-700">
                  <option value="volume">按成交量</option>
                  <option value="vol_oi">按 Vol/OI</option>
                  <option value="time">按时间</option>
                </select>
              </label>
            </div>
            <NewActivityPanel
              rows={newRows}
              onSelect={(contract) => {
                setSelectedContract(contract)
                setSelectionSource(contract ? '新增活动' : null)
              }}
              selected={selectedContract}
            />
            <div className="mt-3">
              <button
                onClick={() => setNewExpanded((v) => !v)}
                className="rounded-lg border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50"
              >
                {newExpanded ? '收起新增合约' : `查看全部新增合约（${newRowsAll.length} 条）`}
              </button>
            </div>
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">结构分布图</h2>
            <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
              <div>
                <div className="mb-2 font-medium">Call / Put 构成</div>
                <div className="h-8 overflow-hidden rounded-full border border-slate-200">
                  <button
                    style={{ width: `${summary.callShare * 100}%` }}
                    className="h-full bg-emerald-500 text-xs text-white"
                    onClick={() => {
                      setOptionFilter('call')
                      setSelectionSource('Call/Put 构成图')
                    }}
                  >
                    Call {(summary.callShare * 100).toFixed(1)}%
                  </button>
                  <button
                    style={{ width: `${summary.putShare * 100}%` }}
                    className="h-full bg-rose-500 text-xs text-white"
                    onClick={() => {
                      setOptionFilter('put')
                      setSelectionSource('Call/Put 构成图')
                    }}
                  >
                    Put {(summary.putShare * 100).toFixed(1)}%
                  </button>
                </div>
                <button onClick={() => setOptionFilter('all')} className="mt-2 text-xs text-slate-500 underline">清除方向筛选</button>
              </div>

              <div>
                <div className="mb-2 font-medium">DTE 分布</div>
                <div className="space-y-2">
                  {dteChartData.map((d) => (
                    <button
                      key={d.key}
                      onClick={() => {
                        setDteFilter(d.key)
                        setSelectionSource('DTE 分布')
                      }}
                      className="block w-full text-left text-xs"
                    >
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
                      <button
                        key={b.label}
                        onClick={() => {
                          setSelectedStrikeRange({ min: b.min, max: b.max })
                          setSelectionSource('Strike 分布')
                        }}
                        className="block w-full text-left text-xs"
                      >
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
            <h2 className="mb-3 text-lg font-semibold">日内 Summary</h2>
            <p className="mb-3 text-xs text-slate-500">帮助判断今天窗口内的情绪主线，图形默认覆盖主要标的，而不是仅依赖 focus tickers。</p>
            <SummaryVisualPanel
              rows={dailySummaryRows.slice(0, 8)}
              titlePrefix="日内"
              emptyText="今日暂无变化事件汇总。"
              onSelectTicker={(ticker) => {
                setSelectedTicker(ticker)
                setSelectionSource('日内 Summary')
                setSummaryWindowMode('daily')
                setTableMode('summary')
              }}
            />
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">近三日 Summary</h2>
            <p className="mb-3 text-xs text-slate-500">用于区分一次性脉冲和持续主题，图形直接展示近三日的累计事件与累计变化主线。</p>
            <SummaryVisualPanel
              rows={threeDaySummaryRows.slice(0, 8)}
              titlePrefix="近三日"
              emptyText="近三日暂无变化事件汇总。"
              onSelectTicker={(ticker) => {
                setSelectedTicker(ticker)
                setSelectionSource('近三日 Summary')
                setSummaryWindowMode('three_day')
                setTableMode('summary')
              }}
            />
          </section>

          <section className={CARD_CLASS}>
            <h2 className="mb-3 text-lg font-semibold">延续活动</h2>
            <p className="mb-3 text-xs text-slate-500">相对于最近一次有效变化快照仍在持续异常出现，并突出展示 Vol / OI 的前后变化与 delta。</p>
            <ContinuingActivityPanel
              rows={continuingRows}
              onSelect={(contract) => {
                setSelectedContract(contract)
                setSelectionSource(contract ? '延续活动' : null)
              }}
              selected={selectedContract}
            />
          </section>

          <section className="rounded-2xl border border-slate-200 bg-slate-50 p-4 shadow-sm">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <h2 className="text-lg font-semibold text-slate-700">不再异常</h2>
                <p className="mt-1 text-xs text-slate-500">
                  这些合约相对于最近一次有效变化快照未继续被异常捕捉，不代表仓位已关闭。
                </p>
              </div>
              <button
                onClick={() => setInactiveExpanded((v) => !v)}
                className="rounded-lg border border-slate-300 px-3 py-1 text-sm text-slate-600 hover:bg-white"
              >
                {inactiveExpanded ? '收起' : `展开查看（${inactiveRows.length}）`}
              </button>
            </div>
            <InactiveActivityPanel rows={inactiveExpanded ? inactiveRows : inactiveRows.slice(0, 6)} />
          </section>

          <section className={CARD_CLASS}>
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-lg font-semibold">明细验证表格</h2>
              <div className="text-right text-sm text-slate-500">
                <div>共 {totalTableRows} 条</div>
                {selectionSource ? <div>来源：{selectionSource}</div> : null}
                {tableMode === 'summary' ? <div>排序：{String(summarySort.key)} {summarySort.dir === 'asc' ? '升序' : '降序'}</div> : null}
              </div>
            </div>
            <div className="mb-3 flex flex-wrap gap-2">
              <button onClick={() => setTableMode('current')} className={`rounded-lg border px-3 py-1 text-sm ${tableMode === 'current' ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-300'}`}>当前快照</button>
              <button onClick={() => setTableMode('events')} className={`rounded-lg border px-3 py-1 text-sm ${tableMode === 'events' ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-300'}`}>日内变化</button>
              <button onClick={() => setTableMode('summary')} className={`rounded-lg border px-3 py-1 text-sm ${tableMode === 'summary' ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-300'}`}>总结汇总</button>
              {tableMode === 'summary' ? (
                <div className="ml-2 inline-flex rounded-lg border border-slate-300 p-1">
                  <button onClick={() => setSummaryWindowMode('daily')} className={`rounded px-3 py-1 text-sm ${summaryWindowMode === 'daily' ? 'bg-slate-900 text-white' : 'text-slate-700'}`}>日内</button>
                  <button onClick={() => setSummaryWindowMode('three_day')} className={`rounded px-3 py-1 text-sm ${summaryWindowMode === 'three_day' ? 'bg-slate-900 text-white' : 'text-slate-700'}`}>近三日</button>
                </div>
              ) : null}
            </div>
            <div className="mb-3 flex flex-wrap gap-2">
              <span className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs">{source.toUpperCase()}</span>
              {tableMode === 'events' && data?.metadata?.window_start_time ? (
                <span className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs">
                  日内窗口：{data.metadata.window_start_time} {'->'} {data.metadata.window_end_time ?? '-'}
                </span>
              ) : null}
              {tableMode === 'summary' && filteredSummaryRows[0] ? (
                <span className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs">
                  汇总窗口：{filteredSummaryRows[0].windowStartTime} {'->'} {filteredSummaryRows[0].windowEndTime}
                </span>
              ) : null}
              {activeFilters.map((f) => (
                <span key={`table-${f.key}`} className="rounded-full border border-slate-300 bg-slate-50 px-3 py-1 text-xs">
                  {f.label}
                </span>
              ))}
            </div>
            {totalTableRows === 0 ? (
              <div className="text-sm text-slate-500">当前筛选下没有匹配数据。</div>
            ) : (
              <>
                <div className="max-h-[520px] overflow-auto rounded-xl border border-slate-200">
                  {tableMode === 'current' ? (
                    <CurrentTable rows={pagedRows as ContractRow[]} onSortHeader={onSortHeader} selectedContract={selectedContract} />
                  ) : tableMode === 'events' ? (
                    <EventTable rows={pagedRows as ChangeEventRow[]} selectedContract={selectedContract} />
                  ) : (
                    <SummaryTable rows={pagedRows as SummaryTickerRow[]} onSortHeader={onSummarySortHeader} sortState={summarySort} />
                  )}
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

function ChangeFeedPanel({
  rows,
  hiddenInactiveCount,
  onSelect,
}: {
  rows: ChangeEventRow[]
  hiddenInactiveCount: number
  onSelect: (row: ChangeEventRow) => void
}) {
  if (rows.length === 0) {
    return (
      <div className="text-sm text-slate-500">
        最近没有可展示的变化事件。
        {hiddenInactiveCount > 0 ? ` 当前有 ${hiddenInactiveCount} 条 INACTIVE 事件被默认隐藏，可切换筛选查看。` : ''}
      </div>
    )
  }
  return (
    <div className="space-y-2">
      {rows.map((row) => {
        const badgeCls =
          row.eventType === 'NEW'
            ? 'bg-rose-100 text-rose-700'
            : row.eventType === 'UPDATE'
              ? 'bg-amber-100 text-amber-700'
              : 'bg-slate-200 text-slate-700'
        return (
          <button key={row.id} onClick={() => onSelect(row)} className="block w-full rounded-xl border border-slate-200 p-3 text-left hover:bg-slate-50">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${badgeCls}`}>{row.eventType}</span>
                <span className="font-semibold">{row.contractDisplayName}</span>
                <span className="text-xs text-slate-500">{row.ticker}</span>
              </div>
              <span className="text-xs text-slate-500">{formatShortTime(row.eventTime)}</span>
            </div>
            <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-500">
              <span>{row.optionType}</span>
              <span>Exp {formatShortDate(row.expirationDate)}</span>
              <span>Strike {row.strike}</span>
              {row.currentOptionsVolume != null ? <span>当前 Vol {formatCompact(row.currentOptionsVolume)}</span> : null}
              {row.deltaVolume != null ? <span>ΔVol {row.deltaVolume >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.deltaVolume))}</span> : null}
              {row.deltaOpenInterest != null ? <span>ΔOI {row.deltaOpenInterest >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.deltaOpenInterest))}</span> : null}
            </div>
          </button>
        )
      })}
    </div>
  )
}

function CurrentOverviewPanel({
  overview,
  rows,
  onSelect,
  selected,
}: {
  overview: CurrentOverview
  rows: ContractRow[]
  onSelect: (contract: string | null) => void
  selected: string | null
}) {
  if (overview.activeContractCount === 0) return <div className="text-sm text-slate-500">当前没有 active contracts。</div>
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <MetricCard label="当前 active contracts" value={overview.activeContractCount} />
        <MetricCard label="当前 active tickers" value={overview.activeTickerCount} />
        <MetricCard label="当前 Top ticker" value={overview.topTicker} />
        <MetricCard label="Put / Call" value={`${(overview.putRatio * 100).toFixed(0)}% / ${(overview.callRatio * 100).toFixed(0)}%`} />
      </div>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <MiniBarList
          title="当前 Top Ticker"
          rows={overview.tickerVolumeTop.map((row) => ({
            label: row.ticker,
            value: row.totalVolume,
            sublabel: `${row.rows} contracts`,
          }))}
        />
        <MiniBarList
          title="当前 DTE 分布"
          rows={overview.dteDistribution.map((row) => ({
            label: row.label,
            value: row.value,
          }))}
        />
      </div>
      <div className="space-y-2">
        <div className="text-sm font-medium text-slate-700">最强 active contracts</div>
        {rows.map((r) => (
          <button
            key={r.id}
            onClick={() => onSelect(selected === r.contractSymbol ? null : r.contractSymbol)}
            className={`block w-full rounded-xl border p-3 text-left ${selected === r.contractSymbol ? 'border-slate-900 bg-slate-50' : 'border-slate-200'}`}
          >
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <span className="font-semibold">{r.contractDisplayName}</span>
                <span className="text-xs text-slate-500">{r.ticker}</span>
              </div>
              <span className="text-sm font-semibold text-slate-700">{formatCompact(r.optionsVolume)}</span>
            </div>
            <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-500">
              <span>{r.optionType}</span>
              <span>Exp {formatShortDate(r.expirationDate)}</span>
              <span>Strike {r.strike}</span>
              <span>OI {formatCompact(r.openInterest)}</span>
              <span>Vol/OI {r.volOi.toFixed(2)}</span>
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}

function SummaryVisualPanel({
  rows,
  titlePrefix,
  emptyText,
  onSelectTicker,
}: {
  rows: SummaryTickerRow[]
  titlePrefix: string
  emptyText: string
  onSelectTicker: (ticker: string) => void
}) {
  if (rows.length === 0) return <div className="text-sm text-slate-500">{emptyText}</div>
  const totalNew = rows.reduce((sum, row) => sum + row.totalNewCount, 0)
  const totalUpdate = rows.reduce((sum, row) => sum + row.totalUpdateCount, 0)
  const totalInactive = rows.reduce((sum, row) => sum + row.totalInactiveCount, 0)
  const weightedPut = rows.reduce((sum, row) => sum + row.putRatio * Math.max(row.eventCount, 1), 0)
  const weightedCall = rows.reduce((sum, row) => sum + row.callRatio * Math.max(row.eventCount, 1), 0)
  const totalWeight = rows.reduce((sum, row) => sum + Math.max(row.eventCount, 1), 0) || 1
  const summaryPutRatio = weightedPut / totalWeight
  const summaryCallRatio = weightedCall / totalWeight
  const signedDeltaRows = rows
    .map((row) => ({
      label: row.ticker,
      deltaVolume: row.cumulativeDeltaVolume,
      deltaOpenInterest: row.cumulativeDeltaOpenInterest,
    }))
    .sort((a, b) => Math.abs(b.deltaVolume) + Math.abs(b.deltaOpenInterest) - (Math.abs(a.deltaVolume) + Math.abs(a.deltaOpenInterest)))
    .slice(0, 8)
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <MetricCard label={`${titlePrefix} NEW 总数`} value={totalNew} />
        <MetricCard label={`${titlePrefix} UPDATE 总数`} value={totalUpdate} />
        <MetricCard label={`${titlePrefix} INACTIVE 总数`} value={totalInactive} />
        <MetricCard label={`${titlePrefix} Put / Call`} value={`${(summaryPutRatio * 100).toFixed(0)}% / ${(summaryCallRatio * 100).toFixed(0)}%`} />
      </div>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <MiniBarList
          title={`${titlePrefix} Top N by event_count`}
          rows={rows.map((row) => ({
            label: row.ticker,
            value: row.eventCount,
            sublabel: `NEW ${row.totalNewCount} / UPDATE ${row.totalUpdateCount}`,
          }))}
          onSelect={onSelectTicker}
        />
        <MiniBarList
          title={`${titlePrefix} Top N by cumulative_delta_volume`}
          rows={rows.map((row) => ({
            label: row.ticker,
            value: Math.abs(row.cumulativeDeltaVolume),
            sublabel: `${row.cumulativeDeltaVolume >= 0 ? '+' : '-'}${formatCompact(Math.abs(row.cumulativeDeltaVolume))}`,
          }))}
          onSelect={onSelectTicker}
        />
        <MiniBarList
          title={`${titlePrefix} Top N by cumulative_delta_open_interest`}
          rows={rows.map((row) => ({
            label: row.ticker,
            value: Math.abs(row.cumulativeDeltaOpenInterest),
            sublabel: `${row.cumulativeDeltaOpenInterest >= 0 ? '+' : '-'}${formatCompact(Math.abs(row.cumulativeDeltaOpenInterest))}`,
          }))}
          onSelect={onSelectTicker}
        />
      </div>
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <SummaryMixCard
          title={`${titlePrefix} 事件结构`}
          rows={[
            { label: 'NEW', value: totalNew, color: 'bg-rose-500' },
            { label: 'UPDATE', value: totalUpdate, color: 'bg-amber-500' },
            { label: 'INACTIVE', value: totalInactive, color: 'bg-slate-400' },
          ]}
        />
        <SummaryMixCard
          title={`${titlePrefix} Put / Call 构成`}
          rows={[
            { label: 'Put', value: summaryPutRatio * 100, color: 'bg-rose-500' },
            { label: 'Call', value: summaryCallRatio * 100, color: 'bg-emerald-500' },
          ]}
          suffix="%"
        />
      </div>
      <SummaryDeltaPanel rows={signedDeltaRows} onSelectTicker={onSelectTicker} />
      <div className="space-y-2">
        {rows.map((row) => (
          <button key={row.ticker} onClick={() => onSelectTicker(row.ticker)} className="flex w-full items-center justify-between rounded-xl border border-slate-200 px-3 py-3 text-left hover:bg-slate-50">
            <div>
              <div className="font-semibold">{row.ticker}</div>
              <div className="mt-1 flex flex-wrap gap-3 text-xs text-slate-500">
                <span>NEW {row.totalNewCount}</span>
                <span>UPDATE {row.totalUpdateCount}</span>
                <span>INACTIVE {row.totalInactiveCount}</span>
                <span>Put/Call {(row.putRatio * 100).toFixed(0)}% / {(row.callRatio * 100).toFixed(0)}%</span>
              </div>
            </div>
            <div className="text-right text-xs text-slate-500">
              <div>ΔVol {row.cumulativeDeltaVolume >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.cumulativeDeltaVolume))}</div>
              <div>ΔOI {row.cumulativeDeltaOpenInterest >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.cumulativeDeltaOpenInterest))}</div>
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}

function MiniBarList({
  title,
  rows,
  onSelect,
}: {
  title: string
  rows: Array<{ label: string; value: number; sublabel?: string }>
  onSelect?: (label: string) => void
}) {
  const topRows = rows.slice(0, 8)
  const maxValue = Math.max(...topRows.map((row) => row.value), 1)
  return (
    <div className="rounded-xl border border-slate-200 p-3">
      <div className="mb-3 text-sm font-medium text-slate-700">{title}</div>
      <div className="space-y-2">
        {topRows.map((row) => {
          const content = (
            <>
              <div className="mb-1 flex justify-between text-xs text-slate-600">
                <span>{row.label}</span>
                <span>{formatCompact(row.value)}</span>
              </div>
              <div className="h-2 rounded bg-slate-100">
                <div className="h-full rounded bg-slate-900" style={{ width: `${(row.value / maxValue) * 100}%` }} />
              </div>
              {row.sublabel ? <div className="mt-1 text-[11px] text-slate-500">{row.sublabel}</div> : null}
            </>
          )
          return onSelect ? (
            <button key={row.label} onClick={() => onSelect(row.label)} className="block w-full text-left">
              {content}
            </button>
          ) : (
            <div key={row.label}>{content}</div>
          )
        })}
      </div>
    </div>
  )
}

function SummaryMixCard({
  title,
  rows,
  suffix = '',
}: {
  title: string
  rows: Array<{ label: string; value: number; color: string }>
  suffix?: string
}) {
  const maxValue = Math.max(...rows.map((row) => row.value), 1)
  return (
    <div className="rounded-xl border border-slate-200 p-3">
      <div className="mb-3 text-sm font-medium text-slate-700">{title}</div>
      <div className="space-y-3">
        {rows.map((row) => (
          <div key={row.label}>
            <div className="mb-1 flex justify-between text-xs text-slate-600">
              <span>{row.label}</span>
              <span>{row.value.toFixed(0)}{suffix}</span>
            </div>
            <div className="h-2 rounded bg-slate-100">
              <div className={`h-full rounded ${row.color}`} style={{ width: `${(row.value / maxValue) * 100}%` }} />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

function SummaryDeltaPanel({
  rows,
  onSelectTicker,
}: {
  rows: Array<{ label: string; deltaVolume: number; deltaOpenInterest: number }>
  onSelectTicker: (ticker: string) => void
}) {
  if (rows.length === 0) return null
  const maxAbs = Math.max(
    ...rows.flatMap((row) => [Math.abs(row.deltaVolume), Math.abs(row.deltaOpenInterest)]),
    1,
  )
  return (
    <div className="rounded-xl border border-slate-200 p-3">
      <div className="mb-3 text-sm font-medium text-slate-700">累计变化强度（点击联动表格）</div>
      <div className="space-y-3">
        {rows.map((row) => (
          <button key={row.label} onClick={() => onSelectTicker(row.label)} className="block w-full text-left">
            <div className="mb-1 flex items-center justify-between text-xs text-slate-600">
              <span>{row.label}</span>
              <span>
                ΔVol {row.deltaVolume >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.deltaVolume))} · ΔOI {row.deltaOpenInterest >= 0 ? '+' : '-'}{formatCompact(Math.abs(row.deltaOpenInterest))}
              </span>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <div className="mb-1 text-[11px] text-slate-500">Volume</div>
                <div className="h-2 rounded bg-slate-100">
                  <div
                    className={`h-full rounded ${row.deltaVolume >= 0 ? 'bg-emerald-500' : 'bg-rose-500'}`}
                    style={{ width: `${(Math.abs(row.deltaVolume) / maxAbs) * 100}%` }}
                  />
                </div>
              </div>
              <div>
                <div className="mb-1 text-[11px] text-slate-500">Open Interest</div>
                <div className="h-2 rounded bg-slate-100">
                  <div
                    className={`h-full rounded ${row.deltaOpenInterest >= 0 ? 'bg-sky-500' : 'bg-amber-500'}`}
                    style={{ width: `${(Math.abs(row.deltaOpenInterest) / maxAbs) * 100}%` }}
                  />
                </div>
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  )
}

function CurrentTable({ rows, onSortHeader, selectedContract }: { rows: ContractRow[]; onSortHeader: (key: keyof ContractRow) => void; selectedContract: string | null }) {
  return (
    <table className="min-w-full text-xs">
      <thead className="sticky top-0 bg-slate-100">
        <tr>
          {[
            ['snapshotTime', 'snapshot_time'],
            ['ticker', 'ticker'],
            ['contractDisplayName', 'contract_display_name'],
            ['optionType', 'option_type'],
            ['expirationDate', 'expiration_date'],
            ['strike', 'strike'],
            ['dte', 'dte'],
            ['previousOptionsVolume', 'previous_volume'],
            ['optionsVolume', 'current_volume'],
            ['deltaVolume', 'delta_volume'],
            ['previousOpenInterest', 'previous_oi'],
            ['openInterest', 'current_oi'],
            ['deltaOpenInterest', 'delta_oi'],
            ['estimatedPremium', 'estimated_premium'],
            ['volOi', 'vol_oi_ratio'],
            ['status', 'status'],
          ].map(([key, label]) => (
            <th key={key} onClick={() => onSortHeader(key as keyof ContractRow)} className="cursor-pointer border-b border-slate-200 px-2 py-2 text-left font-semibold">{label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.id} className={`border-b border-slate-100 hover:bg-sky-50 ${selectedContract === r.contractSymbol ? 'bg-amber-50' : ''}`}>
            <td className="px-2 py-1">{r.snapshotTime}</td>
            <td className="px-2 py-1">{r.ticker}</td>
            <td className="px-2 py-1">{r.contractDisplayName}</td>
            <td className="px-2 py-1">{r.optionType}</td>
            <td className="px-2 py-1">{r.expirationDate}</td>
            <td className="px-2 py-1">{r.strike}</td>
            <td className="px-2 py-1">{r.dte}</td>
            <td className="px-2 py-1">{r.previousOptionsVolume == null ? '—' : formatCompact(r.previousOptionsVolume)}</td>
            <td className="px-2 py-1">{formatCompact(r.optionsVolume)}</td>
            <td className="px-2 py-1">{r.deltaVolume == null ? 'N/A' : `${r.deltaVolume >= 0 ? '+' : '-'}${formatCompact(Math.abs(r.deltaVolume))}`}</td>
            <td className="px-2 py-1">{r.previousOpenInterest == null ? '—' : formatCompact(r.previousOpenInterest)}</td>
            <td className="px-2 py-1">{formatCompact(r.openInterest)}</td>
            <td className="px-2 py-1">{r.deltaOpenInterest == null ? 'N/A' : `${r.deltaOpenInterest >= 0 ? '+' : '-'}${formatCompact(Math.abs(r.deltaOpenInterest))}`}</td>
            <td className="px-2 py-1">{formatCompact(r.estimatedPremium)}</td>
            <td className="px-2 py-1">{r.volOi.toFixed(2)}</td>
            <td className="px-2 py-1">{r.status}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function EventTable({ rows, selectedContract }: { rows: ChangeEventRow[]; selectedContract: string | null }) {
  return (
    <table className="min-w-full text-xs">
      <thead className="sticky top-0 bg-slate-100">
        <tr>
          {['event_time', 'event_type', 'ticker', 'contract_display_name', 'option_type', 'expiration_date', 'strike', 'previous_vol', 'current_vol', 'delta_vol', 'previous_oi', 'current_oi', 'delta_oi'].map((label) => (
            <th key={label} className="border-b border-slate-200 px-2 py-2 text-left font-semibold">{label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.id} className={`border-b border-slate-100 ${selectedContract === r.contractSymbol ? 'bg-amber-50' : ''}`}>
            <td className="px-2 py-1">{r.eventTime}</td>
            <td className="px-2 py-1">{r.eventType}</td>
            <td className="px-2 py-1">{r.ticker}</td>
            <td className="px-2 py-1">{r.contractDisplayName}</td>
            <td className="px-2 py-1">{r.optionType}</td>
            <td className="px-2 py-1">{r.expirationDate}</td>
            <td className="px-2 py-1">{r.strike}</td>
            <td className="px-2 py-1">{r.previousOptionsVolume == null ? '--' : formatCompact(r.previousOptionsVolume)}</td>
            <td className="px-2 py-1">{r.currentOptionsVolume == null ? '--' : formatCompact(r.currentOptionsVolume)}</td>
            <td className="px-2 py-1">{r.deltaVolume == null ? '--' : `${r.deltaVolume >= 0 ? '+' : '-'}${formatCompact(Math.abs(r.deltaVolume))}`}</td>
            <td className="px-2 py-1">{r.previousOpenInterest == null ? '--' : formatCompact(r.previousOpenInterest)}</td>
            <td className="px-2 py-1">{r.currentOpenInterest == null ? '--' : formatCompact(r.currentOpenInterest)}</td>
            <td className="px-2 py-1">{r.deltaOpenInterest == null ? '--' : `${r.deltaOpenInterest >= 0 ? '+' : '-'}${formatCompact(Math.abs(r.deltaOpenInterest))}`}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function SummaryTable({
  rows,
  onSortHeader,
  sortState,
}: {
  rows: SummaryTickerRow[]
  onSortHeader: (key: keyof SummaryTickerRow) => void
  sortState: { key: keyof SummaryTickerRow; dir: 'asc' | 'desc' }
}) {
  return (
    <table className="min-w-full text-xs">
      <thead className="sticky top-0 bg-slate-100">
        <tr>
          {[
            ['ticker', 'ticker'],
            ['windowStartTime', 'window_start_time'],
            ['windowEndTime', 'window_end_time'],
            ['lastEventTime', 'last_event_time'],
            ['totalNewCount', 'total_new_count'],
            ['totalUpdateCount', 'total_update_count'],
            ['totalInactiveCount', 'total_inactive_count'],
            ['cumulativeDeltaVolume', 'cumulative_delta_volume'],
            ['cumulativeDeltaOpenInterest', 'cumulative_delta_open_interest'],
            ['putRatio', 'put_ratio'],
            ['callRatio', 'call_ratio'],
            ['eventCount', 'event_count'],
          ].map(([key, label]) => (
            <th key={label} onClick={() => onSortHeader(key as keyof SummaryTickerRow)} className="cursor-pointer border-b border-slate-200 px-2 py-2 text-left font-semibold">
              {label}{sortLabel(sortState.key === key, sortState.dir)}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={`${r.ticker}-${r.lastEventTime}`} className="border-b border-slate-100">
            <td className="px-2 py-1">{r.ticker}</td>
            <td className="px-2 py-1">{r.windowStartTime}</td>
            <td className="px-2 py-1">{r.windowEndTime}</td>
            <td className="px-2 py-1">{r.lastEventTime}</td>
            <td className="px-2 py-1">{r.totalNewCount}</td>
            <td className="px-2 py-1">{r.totalUpdateCount}</td>
            <td className="px-2 py-1">{r.totalInactiveCount}</td>
            <td className="px-2 py-1">{r.cumulativeDeltaVolume >= 0 ? '+' : '-'}{formatCompact(Math.abs(r.cumulativeDeltaVolume))}</td>
            <td className="px-2 py-1">{r.cumulativeDeltaOpenInterest >= 0 ? '+' : '-'}{formatCompact(Math.abs(r.cumulativeDeltaOpenInterest))}</td>
            <td className="px-2 py-1">{(r.putRatio * 100).toFixed(0)}%</td>
            <td className="px-2 py-1">{(r.callRatio * 100).toFixed(0)}%</td>
            <td className="px-2 py-1">{r.eventCount}</td>
          </tr>
        ))}
      </tbody>
    </table>
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

function NewActivityPanel({ rows, onSelect, selected }: { rows: ContractRow[]; onSelect: (s: string | null) => void; selected: string | null }) {
  const maxVolume = Math.max(...rows.map((r) => r.optionsVolume), 1)
  return (
    rows.length === 0 ? (
      <div className="text-sm text-slate-500">暂无匹配合约</div>
    ) : (
      <div className="space-y-2">
        {rows.map((r) => (
          <button
            key={r.id}
            title={`OI ${r.openInterest} / Vol-OI ${r.volOi.toFixed(2)}`}
            onClick={() => onSelect(selected === r.contractSymbol ? null : r.contractSymbol)}
            className={`block w-full rounded-xl border p-3 text-left ${selected === r.contractSymbol ? 'border-slate-900 bg-slate-50' : 'border-slate-200'}`}
          >
            <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
              <div className="flex items-center gap-2">
                <span className="rounded bg-rose-100 px-1.5 py-0.5 text-[10px] font-semibold text-rose-700">NEW</span>
                <span className="font-semibold">{r.contractDisplayName}</span>
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${r.optionType === 'Put' ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}`}>
                  {r.optionType}
                </span>
                <span className="text-xs text-slate-500">{formatShortDate(r.expirationDate)}</span>
              </div>
              <div className="text-sm font-semibold text-slate-700">{formatCompact(r.optionsVolume)}</div>
            </div>
            <div className="mt-1 flex flex-wrap gap-3 text-xs text-slate-500">
              <span>Strike {r.strike}</span>
              <span>DTE {r.dte}</span>
              <span>Vol/OI {r.volOi.toFixed(2)}</span>
              <span>时间 {formatShortTime(r.recordedAt)}</span>
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
    )
  )
}

function ContinuingActivityPanel({ rows, onSelect, selected }: { rows: ContractRow[]; onSelect: (s: string | null) => void; selected: string | null }) {
  return (
    rows.length === 0 ? (
      <div className="text-sm text-slate-500">暂无匹配合约</div>
    ) : (
      <div className="space-y-2">
        {rows.map((r) => {
          const prevVol = r.previousOptionsVolume
          const prevOi = r.previousOpenInterest
          const deltaVol = r.deltaVolume ?? (prevVol == null ? null : r.optionsVolume - prevVol)
          const deltaOi = r.deltaOpenInterest ?? (prevOi == null ? null : r.openInterest - prevOi)
          const deltaCls = (value: number | null) => (value == null ? 'text-slate-400' : value >= 0 ? 'text-emerald-600' : 'text-rose-600')
          const previousLabel = (value: number | null) => (value == null ? '首次出现' : formatCompact(value))
          const deltaLabel = (value: number | null) => (value == null ? 'N/A' : `${value >= 0 ? '+' : '-'}${formatCompact(Math.abs(value))}`)

          return (
            <button
              key={r.id}
              onClick={() => onSelect(selected === r.contractSymbol ? null : r.contractSymbol)}
              className={`block w-full rounded-xl border p-3 text-left ${selected === r.contractSymbol ? 'border-slate-900 bg-slate-50' : 'border-slate-200'}`}
            >
              <div className="flex items-center justify-between gap-2">
                <div className="font-semibold">{r.contractDisplayName}</div>
                <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${r.optionType === 'Put' ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}`}>
                  {r.optionType}
                </span>
              </div>
              <div className="mt-2 grid grid-cols-1 gap-1 text-xs sm:grid-cols-2">
                <div className="text-slate-600">
                  Vol: {previousLabel(prevVol)} {'->'} {formatCompact(r.optionsVolume)}
                </div>
                <div className={deltaCls(deltaVol)}>
                  ΔVol: {deltaLabel(deltaVol)}
                </div>
                <div className="text-slate-600">
                  OI: {previousLabel(prevOi)} {'->'} {formatCompact(r.openInterest)}
                </div>
                <div className={deltaCls(deltaOi)}>
                  ΔOI: {deltaLabel(deltaOi)}
                </div>
              </div>
              <div className="mt-2 text-xs text-slate-500">
                当前 Vol/OI {r.volOi.toFixed(2)} · 到期 {formatShortDate(r.expirationDate)} · Strike {r.strike}
              </div>
            </button>
          )
        })}
      </div>
    )
  )
}

function InactiveActivityPanel({ rows }: { rows: InactiveRow[] }) {
  return rows.length === 0 ? (
    <div className="text-sm text-slate-500">当前没有“不再异常”的合约。</div>
  ) : (
    <div className="space-y-2">
      {rows.map((r) => (
        <div key={r.id} className="rounded-xl border border-slate-200 bg-white px-3 py-3 text-left">
          <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
            <div className="flex items-center gap-2">
              <span className="font-semibold text-slate-700">{r.contractDisplayName}</span>
              <span className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${r.optionType === 'Put' ? 'bg-rose-100 text-rose-700' : 'bg-emerald-100 text-emerald-700'}`}>
                {r.optionType}
              </span>
              <span className="text-xs text-slate-500">{formatShortDate(r.expirationDate)}</span>
            </div>
            <div className="text-xs text-slate-500">{r.ticker}</div>
          </div>
          <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-500">
            <span>Strike {r.strike}</span>
            <span>此前 Vol {formatCompact(r.previousOptionsVolume)}</span>
            <span>此前 OI {formatCompact(r.previousOpenInterest)}</span>
          </div>
        </div>
      ))}
    </div>
  )
}


