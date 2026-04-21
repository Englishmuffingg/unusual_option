type Props = { items: string[] }

export function MetricGuide({ items }: Props) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
      <h3 className="mb-3 text-sm font-semibold text-slate-200">指标解释</h3>
      <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {items.map((text) => (
          <div key={text} className="rounded-lg bg-slate-800/80 p-2 text-xs text-slate-300">
            {text}
          </div>
        ))}
      </div>
    </div>
  )
}
