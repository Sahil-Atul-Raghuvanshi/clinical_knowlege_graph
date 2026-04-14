import { useState } from 'react'

export default function Sidebar({ apiKeys, onApiKeysChange }) {
  const [inputValue, setInputValue] = useState(apiKeys.join('\n'))
  const [saved, setSaved]           = useState(false)

  const handleSave = () => {
    const keys = inputValue
      .split(/[\n,]+/)
      .map(k => k.trim())
      .filter(Boolean)
    onApiKeysChange(keys)
    localStorage.setItem('gemini_api_keys', JSON.stringify(keys))
    setSaved(true)
    setTimeout(() => setSaved(false), 2000)
  }

  return (
    <aside className="w-72 shrink-0 bg-white border-r border-slate-200 flex flex-col h-full overflow-y-auto">
      {/* API Keys */}
      <div className="p-5 border-b border-slate-100">
        <h2 className="text-sm font-semibold text-slate-700 mb-1">
          Gemini API Keys
        </h2>
        <p className="text-xs text-slate-500 mb-3">
          Auto-loaded from <code className="bg-slate-100 px-1 rounded">.env</code>. Override below.
        </p>
        <textarea
          className="textarea h-28 font-mono text-xs"
          placeholder="Paste API keys (one per line or comma-separated)"
          value={inputValue}
          onChange={e => setInputValue(e.target.value)}
        />
        <button
          onClick={handleSave}
          className={`mt-2 w-full btn-primary justify-center ${saved ? 'bg-green-600 hover:bg-green-600' : ''}`}
        >
          {saved ? 'Saved!' : 'Save Keys'}
        </button>
        {apiKeys.length > 0 && (
          <p className="mt-2 text-xs text-green-700 bg-green-50 border border-green-200 rounded px-2 py-1">
            {apiKeys.length} key{apiKeys.length !== 1 ? 's' : ''} configured
          </p>
        )}
      </div>

      {/* About */}
      <div className="p-5 flex-1">
        <h2 className="text-sm font-semibold text-slate-700 mb-3">
          Features
        </h2>
        <div className="space-y-3">
          {[
            { title: 'Find Similar Patients', desc: 'Vector similarity search on text embeddings' },
            { title: 'Summarize Patient',     desc: 'AI-generated clinical summary from KG' },
            { title: 'Compare Patients',      desc: 'Side-by-side AI comparison of two patients' },
            { title: 'Patient Journey',       desc: 'Chronological clinical event timeline' },
            { title: 'Find by Diagnosis',     desc: 'Diagnosis embedding similarity search' },
          ].map(f => (
            <div key={f.title}>
              <p className="text-xs font-semibold text-slate-700">{f.title}</p>
              <p className="text-xs text-slate-500">{f.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Footer */}
      <div className="p-4 border-t border-slate-100">
        <p className="text-xs text-slate-400 text-center">
          Powered by Neo4j &amp; Gemini AI
        </p>
      </div>
    </aside>
  )
}
