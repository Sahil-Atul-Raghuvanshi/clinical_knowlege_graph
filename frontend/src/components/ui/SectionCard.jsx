/**
 * Clinical section card with colored header bar.
 * Used for displaying comparison and summary sections.
 */
export default function SectionCard({ title, icon, children, color = 'clinical', className = '' }) {
  const colors = {
    clinical: 'bg-clinical-700',
    teal:     'bg-teal-700',
    purple:   'bg-purple-700',
    amber:    'bg-amber-600',
    rose:     'bg-rose-700',
    slate:    'bg-slate-600',
    green:    'bg-green-700',
    indigo:   'bg-indigo-700',
    cyan:     'bg-cyan-700',
    orange:   'bg-orange-600',
  }
  const bar = colors[color] || colors.clinical

  return (
    <div className={`bg-white rounded-xl shadow-sm border border-slate-100 overflow-hidden ${className}`}>
      <div className={`${bar} px-5 py-3 flex items-center gap-2`}>
        {icon && <span className="text-white text-lg">{icon}</span>}
        <h2 className="text-white font-semibold text-base">{title}</h2>
      </div>
      <div className="p-5">{children}</div>
    </div>
  )
}
