export default function Spinner({ text = 'Loading…', size = 'md' }) {
  const sz = size === 'sm' ? 'w-4 h-4 border-2' : 'w-8 h-8 border-4'
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-10">
      <div className={`${sz} border-clinical-200 border-t-clinical-600 rounded-full animate-spin`} />
      {text && <p className="text-sm text-slate-500">{text}</p>}
    </div>
  )
}
