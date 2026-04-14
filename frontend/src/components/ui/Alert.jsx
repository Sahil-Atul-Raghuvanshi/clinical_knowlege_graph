export default function Alert({ type = 'info', children }) {
  const styles = {
    info:    'bg-clinical-50 border-clinical-200 text-clinical-800',
    success: 'bg-green-50 border-green-200 text-green-800',
    warning: 'bg-amber-50 border-amber-200 text-amber-800',
    error:   'bg-red-50 border-red-200 text-red-800',
  }
  return (
    <div className={`p-3 rounded-lg border text-sm ${styles[type]}`}>
      {children}
    </div>
  )
}
