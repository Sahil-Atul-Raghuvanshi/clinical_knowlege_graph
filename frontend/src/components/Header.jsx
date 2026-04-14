import { useState, useEffect } from 'react'
import { fetchHealth } from '../api/client'

export default function Header() {
  const [neo4jStatus, setNeo4jStatus] = useState('checking')
  const [errorDetail, setErrorDetail] = useState('')

  useEffect(() => {
    const check = async () => {
      try {
        const data = await fetchHealth()
        if (data.neo4j === 'connected') {
          setNeo4jStatus('connected')
          setErrorDetail('')
        } else {
          setNeo4jStatus('disconnected')
          setErrorDetail(data.neo4j || '')
        }
      } catch {
        setNeo4jStatus('disconnected')
        setErrorDetail('Backend unreachable')
      }
    }

    check()
    const interval = setInterval(check, 30000)
    return () => clearInterval(interval)
  }, [])

  const statusConfig = {
    checking: {
      wrapper: 'bg-slate-700/40 border-slate-500/40',
      dot: 'bg-slate-400 animate-pulse',
      text: 'text-slate-300',
      label: 'Checking Neo4j…',
    },
    connected: {
      wrapper: 'bg-green-900/40 border-green-700/40',
      dot: 'bg-green-400 animate-pulse',
      text: 'text-green-300',
      label: 'Neo4j Connected',
    },
    disconnected: {
      wrapper: 'bg-red-900/40 border-red-700/40',
      dot: 'bg-red-400',
      text: 'text-red-300',
      label: 'Neo4j Disconnected',
    },
  }

  const cfg = statusConfig[neo4jStatus]

  return (
    <header className="bg-clinical-800 shadow-md">
      <div className="flex items-center gap-4 px-6 py-4">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-white rounded-lg flex items-center justify-center shadow-sm">
            <span className="text-2xl">🩺</span>
          </div>
          <div>
            <h1 className="text-white font-bold text-xl leading-tight">Patient Analysis Dashboard</h1>
            <p className="text-clinical-300 text-xs">Clinical Knowledge Graph · AI-Powered Insights</p>
          </div>
        </div>

        <div className="ml-auto flex items-center gap-3">
          <span
            title={errorDetail || undefined}
            className={`flex items-center gap-1.5 ${cfg.wrapper} ${cfg.text} text-xs font-medium px-3 py-1.5 rounded-full border cursor-default`}
          >
            <span className={`w-1.5 h-1.5 rounded-full ${cfg.dot}`} />
            {cfg.label}
          </span>
          <span className="flex items-center gap-1.5 bg-clinical-900/40 text-clinical-300 text-xs font-medium px-3 py-1.5 rounded-full border border-clinical-600/40">
            <span className="text-clinical-400">✦</span>
            Gemini 2.5 Pro
          </span>
        </div>
      </div>
    </header>
  )
}
