import { DashboardResponse } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://127.0.0.1:8000'

function normalizePayload(raw: any): DashboardResponse {
  const fallbackSection = { summary: [], bubble: [], dte_profile: [], strike_profile: [], focus_blocks: [], contracts: [], best_ticker: '-', key: 'overall' }
  const sections = raw?.sections ?? {}
  return {
    table: raw?.table ?? 'stock',
    db_path: raw?.db_path ?? '-',
    cards: raw?.cards ?? {},
    metric_explanations: raw?.metric_explanations ?? [],
    refresh_delta: {
      snapshot_time: raw?.refresh_delta?.snapshot_time ?? null,
      previous_snapshot_time: raw?.refresh_delta?.previous_snapshot_time ?? null,
      contract_count_delta: Number(raw?.refresh_delta?.contract_count_delta ?? 0),
      options_volume_delta: Number(raw?.refresh_delta?.options_volume_delta ?? 0),
      estimated_premium_delta: Number(raw?.refresh_delta?.estimated_premium_delta ?? 0),
      open_interest_delta: Number(raw?.refresh_delta?.open_interest_delta ?? 0),
      new_contract_count: Number(raw?.refresh_delta?.new_contract_count ?? 0),
      disappeared_contract_count: Number(raw?.refresh_delta?.disappeared_contract_count ?? 0),
      persistent_contract_count: Number(raw?.refresh_delta?.persistent_contract_count ?? 0),
      ticker_rank: raw?.refresh_delta?.ticker_rank ?? [],
      contract_changes: raw?.refresh_delta?.contract_changes ?? [],
    },
    sections: {
      refreshed: { ...fallbackSection, ...(sections.refreshed ?? {}), key: 'refreshed' },
      today_new: { ...fallbackSection, ...(sections.today_new ?? {}), key: 'today_new' },
      overall: { ...fallbackSection, ...(sections.overall ?? {}), key: 'overall' },
    },
  }
}

export async function fetchDashboard(table: 'stock' | 'etf'): Promise<DashboardResponse> {
  const res = await fetch(`${API_BASE}/api/dashboard?table=${table}&_ts=${Date.now()}`, { cache: 'no-store' })
  if (!res.ok) {
    throw new Error(`API 请求失败: ${res.status}`)
  }
  const raw = await res.json()
  return normalizePayload(raw)
}
