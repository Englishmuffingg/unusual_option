import { DashboardResponse } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://127.0.0.1:8000'

function normalizePayload(raw: any): DashboardResponse {
  const fallbackSection = { summary: [], bubble: [], dte_profile: [], strike_profile: [], focus_blocks: [], contracts: [], best_ticker: '-', key: 'overall' as const }
  const sections = raw?.sections ?? {}
  return {
    table: raw?.table ?? 'stock',
    db_path: raw?.db_path ?? '-',
    comparison_mode: raw?.comparison_mode ?? raw?.snapshot_meta?.comparison_mode ?? null,
    artifact_key: raw?.artifact_key ?? raw?.metadata?.artifact_key ?? raw?.snapshot_meta?.artifact_key ?? null,
    dashboard_generated_at: raw?.dashboard_generated_at ?? raw?.metadata?.dashboard_generated_at ?? null,
    cache_meta: raw?.cache_meta ?? {
      artifact_key: raw?.metadata?.artifact_key ?? raw?.snapshot_meta?.artifact_key ?? null,
      generated_at: raw?.metadata?.cache_generated_at ?? raw?.snapshot_meta?.cache_generated_at ?? raw?.dashboard_generated_at ?? null,
      refresh_status: raw?.metadata?.cache_refresh_status ?? raw?.snapshot_meta?.cache_refresh_status ?? 'unknown',
      last_attempt_at: raw?.metadata?.cache_last_attempt_at ?? raw?.snapshot_meta?.cache_last_attempt_at ?? null,
      last_success_at: raw?.metadata?.cache_last_success_at ?? raw?.snapshot_meta?.cache_last_success_at ?? null,
      last_error: raw?.metadata?.cache_last_error ?? raw?.snapshot_meta?.cache_last_error ?? null,
    },
    metadata: raw?.metadata ?? {
      comparison_mode: raw?.comparison_mode ?? raw?.snapshot_meta?.comparison_mode ?? 'effective_change_previous',
      dashboard_generated_at: raw?.dashboard_generated_at ?? null,
      artifact_key: raw?.artifact_key ?? raw?.snapshot_meta?.artifact_key ?? null,
      cache_generated_at: raw?.snapshot_meta?.cache_generated_at ?? raw?.dashboard_generated_at ?? null,
      cache_refresh_status: raw?.snapshot_meta?.cache_refresh_status ?? 'unknown',
      cache_last_attempt_at: raw?.snapshot_meta?.cache_last_attempt_at ?? null,
      cache_last_success_at: raw?.snapshot_meta?.cache_last_success_at ?? null,
      cache_last_error: raw?.snapshot_meta?.cache_last_error ?? null,
      snapshot_time: raw?.snapshot_meta?.snapshot_time ?? raw?.refresh_delta?.snapshot_time ?? null,
      latest_snapshot_time: raw?.snapshot_meta?.latest_snapshot_time ?? raw?.refresh_delta?.snapshot_time ?? null,
      previous_snapshot_time: raw?.snapshot_meta?.previous_snapshot_time ?? raw?.refresh_delta?.previous_snapshot_time ?? null,
      window_start_time: raw?.snapshot_meta?.window_start_time ?? null,
      window_end_time: raw?.snapshot_meta?.window_end_time ?? null,
      raw_signature: raw?.snapshot_meta?.raw_signature ?? null,
      current_signature: raw?.snapshot_meta?.current_signature ?? null,
      raw_refresh_id: raw?.snapshot_meta?.raw_refresh_id ?? null,
      current_refresh_id: raw?.snapshot_meta?.current_refresh_id ?? null,
      data_source: raw?.table ?? 'stock',
      active_filter_summary: {},
    },
    current_overview: raw?.current_overview ?? {
      active_contract_count: 0,
      active_ticker_count: 0,
      top_ticker: '-',
      put_ratio: 0,
      call_ratio: 0,
      dte_distribution: [],
      ticker_volume_top: [],
      top_active_contracts: [],
    },
    summary: raw?.summary ?? {
      current_total: Number(raw?.cards?.overall_ticker_count ?? 0),
      new_count: Number(raw?.cards?.today_new_contract_count ?? 0),
      continued_count: Number(raw?.cards?.refreshed_contract_count ?? 0),
      put_ratio: 0,
      call_ratio: 0,
      dominant_ticker: String(raw?.cards?.strongest_focus_ticker ?? '-'),
      dominant_dte_bucket: '-',
      dominant_strike_bucket: '-',
    },
    cards: raw?.cards ?? {},
    metric_explanations: raw?.metric_explanations ?? [],
    change_feed: raw?.change_feed ?? [],
    change_feed_preview: raw?.change_feed_preview ?? raw?.change_feed ?? [],
    intraday_event_rows: raw?.intraday_event_rows ?? [],
    current_snapshot_rows: raw?.current_snapshot_rows ?? raw?.sections?.overall?.contracts ?? [],
    continued_rows: raw?.continued_rows ?? raw?.sections?.refreshed?.contracts ?? [],
    daily_summary: raw?.daily_summary ?? [],
    three_day_summary: raw?.three_day_summary ?? [],
    refresh_delta: {
      snapshot_time: raw?.refresh_delta?.snapshot_time ?? null,
      previous_snapshot_time: raw?.refresh_delta?.previous_snapshot_time ?? null,
      comparison_mode: raw?.refresh_delta?.comparison_mode ?? raw?.comparison_mode ?? null,
      contract_count_delta: Number(raw?.refresh_delta?.contract_count_delta ?? 0),
      options_volume_delta: Number(raw?.refresh_delta?.options_volume_delta ?? 0),
      estimated_premium_delta: Number(raw?.refresh_delta?.estimated_premium_delta ?? 0),
      open_interest_delta: Number(raw?.refresh_delta?.open_interest_delta ?? 0),
      new_contract_count: Number(raw?.refresh_delta?.new_contract_count ?? 0),
      persistent_contract_count: Number(raw?.refresh_delta?.persistent_contract_count ?? 0),
      ticker_rank: raw?.refresh_delta?.ticker_rank ?? [],
      contract_changes: raw?.refresh_delta?.contract_changes ?? [],
    },
    sections: {
      refreshed: { ...fallbackSection, ...(sections.refreshed ?? {}), key: 'refreshed' },
      today_new: { ...fallbackSection, ...(sections.today_new ?? {}), key: 'today_new' },
      overall: { ...fallbackSection, ...(sections.overall ?? {}), key: 'overall' },
    },
    snapshot_meta: {
      dashboard_generated_at: raw?.snapshot_meta?.dashboard_generated_at ?? raw?.dashboard_generated_at ?? null,
      artifact_key: raw?.snapshot_meta?.artifact_key ?? raw?.metadata?.artifact_key ?? null,
      cache_generated_at: raw?.snapshot_meta?.cache_generated_at ?? raw?.metadata?.cache_generated_at ?? raw?.dashboard_generated_at ?? null,
      cache_refresh_status: raw?.snapshot_meta?.cache_refresh_status ?? raw?.metadata?.cache_refresh_status ?? 'unknown',
      cache_last_attempt_at: raw?.snapshot_meta?.cache_last_attempt_at ?? raw?.metadata?.cache_last_attempt_at ?? null,
      cache_last_success_at: raw?.snapshot_meta?.cache_last_success_at ?? raw?.metadata?.cache_last_success_at ?? null,
      cache_last_error: raw?.snapshot_meta?.cache_last_error ?? raw?.metadata?.cache_last_error ?? null,
      snapshot_time: raw?.snapshot_meta?.snapshot_time ?? raw?.refresh_delta?.snapshot_time ?? null,
      latest_snapshot_time: raw?.snapshot_meta?.latest_snapshot_time ?? raw?.refresh_delta?.snapshot_time ?? null,
      previous_snapshot_time: raw?.snapshot_meta?.previous_snapshot_time ?? raw?.refresh_delta?.previous_snapshot_time ?? null,
      comparison_mode: raw?.snapshot_meta?.comparison_mode ?? raw?.comparison_mode ?? null,
      window_start_time: raw?.snapshot_meta?.window_start_time ?? null,
      window_end_time: raw?.snapshot_meta?.window_end_time ?? null,
      raw_signature: raw?.snapshot_meta?.raw_signature ?? raw?.metadata?.raw_signature ?? null,
      current_signature: raw?.snapshot_meta?.current_signature ?? raw?.metadata?.current_signature ?? null,
      raw_refresh_id: raw?.snapshot_meta?.raw_refresh_id ?? raw?.metadata?.raw_refresh_id ?? null,
      current_refresh_id: raw?.snapshot_meta?.current_refresh_id ?? raw?.metadata?.current_refresh_id ?? null,
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
