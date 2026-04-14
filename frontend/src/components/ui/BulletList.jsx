export default function BulletList({ items = [], emptyText = 'None' }) {
  if (!items || items.length === 0)
    return <p className="text-sm text-slate-400 italic">{emptyText}</p>

  return (
    <ul className="space-y-1.5">
      {items.filter(Boolean).map((item, i) => (
        <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
          <span className="text-clinical-500 font-bold mt-0.5 shrink-0">•</span>
          <span>{item}</span>
        </li>
      ))}
    </ul>
  )
}
