import { useState } from 'react'
import { findByDiagnosis } from '../../api/client'
import Spinner from '../ui/Spinner'
import Alert from '../ui/Alert'
import StatBar from '../ui/StatBar'

export default function DiagnosisSimilarity({ data, onDataChange, onClear }) {
  const [diagText, setDiagText] = useState('')
  const [topK, setTopK]         = useState(20)
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)

  const handleSearch = async () => {
    if (!diagText.trim()) { setError('Please enter at least one diagnosis'); return }
    setError(null)
    setLoading(true)
    try {
      const result = await findByDiagnosis(diagText.trim(), topK)
      onDataChange(result)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Search failed')
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setDiagText('')
    setTopK(20)
    setError(null)
    onClear()
  }

  const scores = data?.results?.map(r => r.similarity_score) ?? []

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Find Patients by Diagnosis</h2>
          <p className="text-sm text-slate-500 mt-1">
            Diagnosis embedding similarity search — find patients with similar clinical diagnoses
          </p>
        </div>
        {data && (
          <button onClick={handleClear} className="btn-danger">Clear</button>
        )}
      </div>

      {/* Search form */}
      <div className="card mb-6">
        <div className="mb-4">
          <label className="block text-xs font-medium text-slate-600 mb-1">
            Diagnosis Text
          </label>
          <textarea
            className="textarea h-32"
            placeholder={`Enter diagnoses (one per line, or comma/semicolon separated)\n\ne.g.:\nDiabetes mellitus type 2\nHypertension\nChronic kidney disease`}
            value={diagText}
            onChange={e => setDiagText(e.target.value)}
          />
          <p className="mt-1 text-xs text-slate-400">
            Separate multiple diagnoses with commas, semicolons, or new lines.
          </p>
        </div>

        <div className="mb-4">
          <label className="block text-xs font-medium text-slate-600 mb-1">
            Results: <span className="text-clinical-700 font-semibold">{topK}</span>
          </label>
          <input
            type="range"
            min={5} max={50} step={5}
            value={topK}
            onChange={e => setTopK(Number(e.target.value))}
            className="w-full accent-clinical-600"
          />
          <div className="flex justify-between text-xs text-slate-400 mt-0.5">
            <span>5</span><span>50</span>
          </div>
        </div>

        <button onClick={handleSearch} disabled={loading} className="btn-teal w-full">
          {loading && <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
          {loading ? 'Searching…' : 'Search by Diagnosis'}
        </button>
      </div>

      {error && <div className="mb-4"><Alert type="error"><span>{error}</span></Alert></div>}
      {loading && <Spinner text="Generating diagnosis embedding and searching for similar patients…" />}

      {/* Results */}
      {data?.results && !loading && (
        <div className="space-y-4">
          {scores.length > 0 && (
            <StatBar stats={[
              { label: 'Total Results',      value: data.results.length },
              { label: 'Highest Similarity', value: Math.max(...scores).toFixed(4) },
              { label: 'Lowest Similarity',  value: Math.min(...scores).toFixed(4) },
            ]} />
          )}

          <div className="card overflow-hidden p-0">
            <div className="px-5 py-3 bg-teal-700 flex items-center gap-2">
              <h3 className="text-white font-semibold">Patients with Similar Diagnoses</h3>
            </div>

            {data.results.length === 0 ? (
              <div className="p-6">
                <Alert type="warning"><span>No patients found with similar diagnoses.</span></Alert>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-slate-50 border-b border-slate-200">
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">#</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Patient ID</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Similarity Score</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Diagnoses (preview)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.results.map((row, i) => {
                      const pct = (row.similarity_score * 100).toFixed(2)
                      const diags = row.all_diagnoses || []
                      const preview = diags.slice(0, 4).join(', ') + (diags.length > 4 ? ` … +${diags.length - 4} more` : '')
                      return (
                        <tr key={i} className="border-b border-slate-100 hover:bg-teal-50 transition-colors">
                          <td className="px-5 py-3 text-slate-400">{i + 1}</td>
                          <td className="px-5 py-3">
                            <span className="font-mono bg-slate-100 px-2 py-0.5 rounded text-slate-700">
                              {row.patient_id}
                            </span>
                          </td>
                          <td className="px-5 py-3">
                            <div className="flex items-center gap-2">
                              <span className="font-semibold text-teal-700">{Number(row.similarity_score).toFixed(4)}</span>
                              <div className="flex-1 bg-slate-200 rounded-full h-2 max-w-24">
                                <div
                                  className="bg-teal-600 h-2 rounded-full"
                                  style={{ width: `${pct}%` }}
                                />
                              </div>
                            </div>
                          </td>
                          <td className="px-5 py-3 text-slate-500 max-w-xs truncate">{preview || 'N/A'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}

      {!data && !loading && (
        <div className="text-center py-4 text-slate-400">
          <p className="text-sm">Enter diagnosis text and click Search</p>
        </div>
      )}
    </div>
  )
}
