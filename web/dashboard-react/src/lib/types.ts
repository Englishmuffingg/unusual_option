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
  key: 'refreshed' | 'today_new' | 'overall'
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
  cards: Record<string, number | string>
  metric_explanations: string[]
  refresh_delta: {
    snapshot_time: string | null
    previous_snapshot_time: string | null
    contract_count_delta: number
    options_volume_delta: number
    estimated_premium_delta: number
    open_interest_delta: number
    new_contract_count: number
    disappeared_contract_count: number
    persistent_contract_count: number
    ticker_rank: Array<{ ticker: string; premium_delta: number; volume_delta: number; contract_count_delta: number }>
    contract_changes: Array<Record<string, string | number>>
  }
  sections: {
    refreshed: SectionPayload
    today_new: SectionPayload
    overall: SectionPayload
  }
}
