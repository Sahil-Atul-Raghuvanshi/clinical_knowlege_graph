import { useState } from 'react'
import Header from './components/Header'
import SimilarPatients    from './components/tabs/SimilarPatients'
import PatientSummary     from './components/tabs/PatientSummary'
import PatientComparison  from './components/tabs/PatientComparison'
import PatientJourney     from './components/tabs/PatientJourney'
import DiagnosisSimilarity from './components/tabs/DiagnosisSimilarity'

const TABS = [
  { id: 'similarity',  label: 'Find Similar Patients' },
  { id: 'summary',     label: 'Summarize Patient'     },
  { id: 'comparison',  label: 'Compare Patients'      },
  { id: 'journey',     label: 'Patient Journey'       },
  { id: 'diagnosis',   label: 'Find by Diagnosis'     },
]

export default function App() {
  const [activeTab, setActiveTab] = useState('similarity')

  // Persist data per tab so it survives tab switches
  const [tabData, setTabData] = useState({
    similarity: null,
    summary:    {},   // keyed by patient_id
    comparison: {},   // keyed by "p1_p2"
    journey:    {},   // keyed by patient_id
    diagnosis:  null,
  })

  const updateTabData = (tab, key, value) =>
    setTabData(prev => ({
      ...prev,
      [tab]: key ? { ...prev[tab], [key]: value } : value,
    }))

  const clearTabData = (tab) =>
    setTabData(prev => ({
      ...prev,
      [tab]: tab === 'similarity' || tab === 'diagnosis' ? null : {},
    }))

  return (
      <div className="flex flex-col h-screen overflow-hidden">
      <Header />

      <div className="flex flex-1 overflow-hidden">
        <main className="flex-1 flex flex-col overflow-hidden">
          {/* Tab bar */}
          <div className="bg-white border-b border-slate-200 overflow-x-auto">
            <div className="flex px-4">
              {TABS.map(tab => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`tab-btn ${activeTab === tab.id ? 'active' : 'inactive'}`}
                >
                  <span>{tab.label}</span>
                </button>
              ))}
            </div>
          </div>

          {/* Tab content – all tabs rendered but only active one visible */}
          <div className="flex-1 overflow-y-auto">
            <div style={{ display: activeTab === 'similarity' ? 'block' : 'none' }}>
              <SimilarPatients
                data={tabData.similarity}
                onDataChange={v => updateTabData('similarity', null, v)}
                onClear={() => clearTabData('similarity')}
              />
            </div>
            <div style={{ display: activeTab === 'summary' ? 'block' : 'none' }}>
              <PatientSummary
                data={tabData.summary}
                onDataChange={(key, v) => updateTabData('summary', key, v)}
                onClear={() => clearTabData('summary')}
              />
            </div>
            <div style={{ display: activeTab === 'comparison' ? 'block' : 'none' }}>
              <PatientComparison
                data={tabData.comparison}
                onDataChange={(key, v) => updateTabData('comparison', key, v)}
                onClear={() => clearTabData('comparison')}
              />
            </div>
            <div style={{ display: activeTab === 'journey' ? 'block' : 'none' }}>
              <PatientJourney
                data={tabData.journey}
                onDataChange={(key, v) => updateTabData('journey', key, v)}
                onClear={() => clearTabData('journey')}
              />
            </div>
            <div style={{ display: activeTab === 'diagnosis' ? 'block' : 'none' }}>
              <DiagnosisSimilarity
                data={tabData.diagnosis}
                onDataChange={v => updateTabData('diagnosis', null, v)}
                onClear={() => clearTabData('diagnosis')}
              />
            </div>
          </div>
        </main>
      </div>
    </div>
  )
}
