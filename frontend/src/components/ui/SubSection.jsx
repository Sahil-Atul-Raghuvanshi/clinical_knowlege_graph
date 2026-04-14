/**
 * Sub-section heading + content block within a SectionCard.
 */
export default function SubSection({ title, children }) {
  return (
    <div className="mb-5 last:mb-0">
      <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 pb-1 border-b border-slate-100">
        {title}
      </h3>
      <div>{children}</div>
    </div>
  )
}
