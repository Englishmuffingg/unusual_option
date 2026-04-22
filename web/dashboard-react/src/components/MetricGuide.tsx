type Props = { items: string[] }

export function MetricGuide({ items }: Props) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <h3 className="mb-3 text-sm font-semibold text-slate-800">指标解释</h3>
      <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {items.map((text) => (
          <div key={text} className="rounded-lg bg-slate-50 p-2 text-xs text-slate-700">
            {text}
          </div>
        ))}
      </div>
    </div>
  )
}
