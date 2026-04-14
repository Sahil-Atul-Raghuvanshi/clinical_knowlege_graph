import { useState } from 'react'
import { findSimilarPatients } from '../../api/client'
import Spinner from '../ui/Spinner'
import Alert from '../ui/Alert'
import StatBar from '../ui/StatBar'

export default function SimilarPatients({ data, onDataChange, onClear }) {
  const [patientId, setPatientId] = useState('')
  const [topK, setTopK]           = useState(20)
  const [loading, setLoading]     = useState(false)
  const [error, setError]         = useState(null)

  const handleSearch = async () => {
    if (!patientId.trim()) { setError('Please enter a patient ID'); return }
    if (!/^\d+$/.test(patientId.trim())) { setError('Patient ID must be numeric'); return }
    setError(null)
    setLoading(true)
    try {
      const result = await findSimilarPatients(patientId.trim(), topK)
      onDataChange(result)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Search failed')
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setPatientId('')
    setTopK(20)
    setError(null)
    onClear()
  }

  const scores = data?.results?.map(r => r.similarity_score) ?? []

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Find Similar Patients</h2>
          <p className="text-sm text-slate-500 mt-1">
            Vector similarity search using patient text embeddings from Neo4j
          </p>
        </div>
        {data && (
          <button onClick={handleClear} className="btn-danger">
            Clear
          </button>
        )}
      </div>

      {/* Search controls */}
      <div className="card mb-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
          <div className="md:col-span-2">
            <label className="block text-xs font-medium text-slate-600 mb-1">Patient ID</label>
            <input
              className="input"
              type="text"
              placeholder="e.g., 10000032"
              value={patientId}
              onChange={e => setPatientId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
            />
          </div>
          <div>
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
        </div>

        <button
          onClick={handleSearch}
          disabled={loading}
          className="btn-primary mt-4 w-full"
        >
          {loading && <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
          {loading ? 'Searching…' : 'Search Similar Patients'}
        </button>
      </div>

      {error && <Alert type="error"><span>{error}</span></Alert>}
      {loading && <Spinner text="Searching for similar patients…" />}

      {/* Results */}
      {data?.results && !loading && (
        <div className="space-y-4">
          {/* Stats */}
          {scores.length > 0 && (
            <StatBar stats={[
              { label: 'Total Results',      value: data.results.length },
              { label: 'Highest Similarity', value: Math.max(...scores).toFixed(4) },
              { label: 'Lowest Similarity',  value: Math.min(...scores).toFixed(4) },
            ]} />
          )}

          {/* Table */}
          <div className="card overflow-hidden p-0">
            <div className="px-5 py-3 bg-clinical-700 flex items-center gap-2">
              <h3 className="text-white font-semibold">
                Similar Patients to {data.patient_id}
              </h3>
            </div>
            {data.results.length === 0 ? (
              <div className="p-6">
                <Alert type="warning"><span>No similar patients found.</span></Alert>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-slate-50 border-b border-slate-200">
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">#</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Patient ID</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Similarity Score</th>
                      <th className="text-left px-5 py-3 font-semibold text-slate-600">Similarity</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.results.map((row, i) => {
                      const pct = (row.similarity_score * 100).toFixed(2)
                      return (
                        <tr
                          key={i}
                          className="border-b border-slate-100 hover:bg-clinical-50 transition-colors"
                        >
                          <td className="px-5 py-3 text-slate-400">{i + 1}</td>
                          <td className="px-5 py-3">
                            <span className="font-mono bg-slate-100 px-2 py-0.5 rounded text-slate-700">
                              {row.patient_id}
                            </span>
                          </td>
                          <td className="px-5 py-3 font-semibold text-clinical-700">
                            {Number(row.similarity_score).toFixed(4)}
                          </td>
                          <td className="px-5 py-3">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 bg-slate-200 rounded-full h-2 max-w-32">
                                <div
                                  className="bg-clinical-600 h-2 rounded-full transition-all"
                                  style={{ width: `${pct}%` }}
                                />
                              </div>
                              <span className="text-xs text-slate-500">{pct}%</span>
                            </div>
                          </td>
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
    </div>
  )
}
