export function formatCompact(value: number): string {
  const n = Number(value || 0)
  const abs = Math.abs(n)
  if (abs >= 1e8) return `${(n / 1e8).toFixed(1)}亿`
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}万`
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return n.toLocaleString('zh-CN')
}

export function dteBucket(dte: number): string {
  const v = Number(dte || 0)
  if (v <= 2) return '0-2天'
  if (v <= 7) return '3-7天'
  if (v <= 14) return '8-14天'
  if (v <= 30) return '15-30天'
  if (v <= 60) return '31-60天'
  if (v <= 120) return '61-120天'
  return '120天以上'
}

export function flowFromOptionType(optionType: string): string {
  const t = (optionType || '').toUpperCase()
  if (t === 'CALL') return '偏多'
  if (t === 'PUT') return '偏防守'
  return '双向博弈'
}

export function dateDay(v: string): string {
  if (!v) return ''
  return String(v).slice(0, 10)
}
