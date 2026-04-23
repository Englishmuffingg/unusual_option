export type SummaryRow = {
  ticker: string
  total_est_premium: number
  call_premium_pct: number
  put_premium_pct: number
  refreshed_rows: number
  refreshed_row_pct: number
  new_rows: number
  new_row_pct: number
  median_dte: number
  avg_ratio: number
  max_ratio: number
  bullish_score: number
  flow_desc: string
}

export type SectionPayload = {
  key: 'refreshed' | 'today_new' | 'overall' | 'inactive_helper'
  summary: SummaryRow[]
  bubble: Array<{
    ticker: string
    median_dte: number
    bullish_score: number
    total_est_premium: number
    avg_ratio: number
    flow_desc: string
  }>
  dte_profile: Array<{ ticker: string; dte_bucket: string; total_est_premium: number; total_volume: number }>
  strike_profile: Array<{ ticker: string; option_type: string; strike: number; total_est_premium: number; rows: number }>
  contracts: Array<Record<string, string | number>>
  focus_blocks: FocusBlock[]
  best_ticker: string
}

export type FocusBlock = {
  ticker: string
  flow_desc: string
  call_premium_pct: number
  put_premium_pct: number
  median_dte: number
  avg_ratio: number
  max_ratio: number
  refreshed_rows: number
  new_rows: number
  top_strikes: Array<{ option_type: string; strike: number; total_est_premium: number }>
  top_contracts: Array<Record<string, string | number>>
}

export type DashboardResponse = {
  table: 'stock' | 'etf'
  db_path: string
  comparison_mode?: string
  metadata?: {
    comparison_mode: string
    latest_snapshot_time: string | null
    previous_snapshot_time: string | null
    data_source: 'stock' | 'etf'
    active_filter_summary?: Record<string, unknown>
  }
  summary?: {
    current_total: number
    new_count: number
    continued_count: number
    inactive_count: number
    put_ratio: number
    call_ratio: number
    dominant_ticker: string
    dominant_dte_bucket: string
    dominant_strike_bucket: string
  }
  cards: Record<string, number | string>
  metric_explanations: string[]
  change_feed?: Array<Record<string, string | number | null>>
  current_snapshot_rows?: Array<Record<string, string | number | null>>
  continued_rows?: Array<Record<string, string | number | null>>
  inactive_rows?: Array<Record<string, string | number | null>>
  daily_summary?: Array<Record<string, string | number | null>>
  three_day_summary?: Array<Record<string, string | number | null>>
  refresh_delta: {
    snapshot_time: string | null
    previous_snapshot_time: string | null
    comparison_mode?: string | null
    contract_count_delta: number
    options_volume_delta: number
    estimated_premium_delta: number
    open_interest_delta: number
    new_contract_count: number
    inactive_contract_count?: number
    disappeared_contract_count: number
    persistent_contract_count: number
    ticker_rank: Array<{ ticker: string; premium_delta: number; volume_delta: number; open_interest_delta: number; contract_count_delta: number }>
    contract_changes: Array<Record<string, string | number>>
  }
  sections: {
    refreshed: SectionPayload
    today_new: SectionPayload
    overall: SectionPayload
    inactive_helper: SectionPayload
  }
  snapshot_meta?: {
    latest_snapshot_time: string | null
    previous_snapshot_time: string | null
    comparison_mode?: string | null
  }
}
