/** Three-column stats bar for similarity results. */
export default function StatBar({ stats }) {
  return (
    <div className="grid grid-cols-3 gap-3">
      {stats.map(({ label, value }) => (
        <div key={label} className="bg-clinical-50 border border-clinical-100 rounded-lg px-4 py-3 text-center">
          <p className="text-xs text-slate-500 mb-1">{label}</p>
          <p className="text-lg font-semibold text-clinical-800">{value}</p>
        </div>
      ))}
    </div>
  )
}
