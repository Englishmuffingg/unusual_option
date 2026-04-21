import { DashboardResponse } from './types'

const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://127.0.0.1:8000'

export async function fetchDashboard(table: 'stock' | 'etf'): Promise<DashboardResponse> {
  const res = await fetch(`${API_BASE}/api/dashboard?table=${table}`)
  if (!res.ok) {
    throw new Error(`API 请求失败: ${res.status}`)
  }
  return res.json()
}
