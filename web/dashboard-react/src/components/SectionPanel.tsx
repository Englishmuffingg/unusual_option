import { SectionPayload } from '@/lib/types'
import { SectionCharts } from './SectionCharts'
import { FocusTickerBlocks } from './FocusTickerBlocks'
import { SummaryTable } from './SummaryTable'

type Props = {
  title: string
  subtitle: string
  section: SectionPayload
  focusTitle: string
}

export function SectionPanel({ title, subtitle, section, focusTitle }: Props) {
  return (
    <section className="space-y-4 rounded-2xl border border-slate-800 bg-slate-950/50 p-4">
      <div>
        <h2 className="text-lg font-semibold text-cyan-300">{title}</h2>
        <p className="text-xs text-slate-400">{subtitle}</p>
      </div>
      <SummaryTable title={`${title} ticker 排行`} data={section.summary} />
      <SectionCharts section={section} />
      <FocusTickerBlocks blocks={section.focus_blocks} title={focusTitle} />
    </section>
  )
}
