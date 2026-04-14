import { useState } from 'react'
import { generateSummary, downloadSummaryPdf } from '../../api/client'
import Spinner from '../ui/Spinner'
import Alert from '../ui/Alert'
import SectionCard from '../ui/SectionCard'
import SubSection from '../ui/SubSection'
import BulletList from '../ui/BulletList'

const SECTION_CONFIG = [
  { key: 'executive_summary',                color: 'clinical', title: 'Executive Summary' },
  { key: 'chief_complaints_and_presentation',color: 'teal',     title: 'Chief Complaints & Presentation' },
  { key: 'clinical_course',                  color: 'indigo',   title: 'Clinical Course' },
  { key: 'key_diagnoses',                    color: 'rose',     title: 'Key Diagnoses' },
  { key: 'significant_procedures',           color: 'purple',   title: 'Significant Procedures' },
  { key: 'medications',                      color: 'amber',    title: 'Medications' },
  { key: 'key_lab_findings',                 color: 'cyan',     title: 'Key Laboratory Findings' },
  { key: 'microbiology_findings',            color: 'green',    title: 'Microbiology Findings' },
  { key: 'discharge_summary',                color: 'slate',    title: 'Discharge Summary' },
  { key: 'clinical_significance',            color: 'orange',   title: 'Clinical Significance' },
]

function SummaryDisplay({ summary, patientId, onDownload, downloading }) {
  const demo = summary.patient_demographics || {}

  return (
    <div className="space-y-5">
      {/* Download */}
      <div className="flex justify-end">
        <button
          onClick={onDownload}
          disabled={downloading}
          className="btn-secondary"
        >
          {downloading && <span className="w-4 h-4 border-2 border-slate-400 border-t-transparent rounded-full animate-spin" />}
          {downloading ? 'Generating PDF…' : 'Download PDF'}
        </button>
      </div>

      {/* Demographics card */}
      <SectionCard title="Patient Demographics" color="clinical">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[
            { label: 'Age',              value: demo.age },
            { label: 'Gender',           value: demo.gender },
            { label: 'Race',             value: demo.race },
            { label: 'Total Admissions', value: demo.total_admissions },
          ].map(({ label, value }) => (
            <div key={label} className="bg-clinical-50 border border-clinical-100 rounded-lg px-4 py-3">
              <p className="text-xs text-slate-500 mb-1">{label}</p>
              <p className="font-semibold text-slate-800">{value || 'N/A'}</p>
            </div>
          ))}
        </div>
      </SectionCard>

      {/* Dynamic sections */}
      {SECTION_CONFIG.map(({ key, color, title }) => {
        const value = summary[key]
        if (!value || (Array.isArray(value) && value.length === 0)) return null
        if (key === 'medications') {
          const meds = value
          const hasData = meds.started?.length || meds.stopped?.length || meds.to_avoid?.length
          if (!hasData) return null
          return (
            <SectionCard key={key} title={title} color={color}>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {meds.started?.length > 0 && (
                  <SubSection title="Started">
                    <BulletList items={meds.started} />
                  </SubSection>
                )}
                {meds.stopped?.length > 0 && (
                  <SubSection title="Stopped">
                    <BulletList items={meds.stopped} />
                  </SubSection>
                )}
                {meds.to_avoid?.length > 0 && (
                  <SubSection title="To Avoid">
                    <BulletList items={meds.to_avoid} />
                  </SubSection>
                )}
              </div>
            </SectionCard>
          )
        }
        if (key === 'discharge_summary') {
          const d = value
          return (
            <SectionCard key={key} title={title} color={color}>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
                {[
                  { label: 'Disposition',     value: d.disposition },
                  { label: 'Condition',        value: d.condition },
                  { label: 'Activity Status', value: d.activity_status },
                ].map(({ label, value: v }) => v ? (
                  <div key={label} className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
                    <p className="text-xs text-slate-500 mb-1">{label}</p>
                    <p className="font-medium text-slate-800 text-sm">{v}</p>
                  </div>
                ) : null)}
              </div>
              {d.follow_up_instructions && (
                <SubSection title="Follow-up Instructions">
                  <p className="text-sm text-slate-700 leading-relaxed">{d.follow_up_instructions}</p>
                </SubSection>
              )}
            </SectionCard>
          )
        }
        if (Array.isArray(value)) {
          return (
            <SectionCard key={key} title={title} color={color}>
              <BulletList items={value} />
            </SectionCard>
          )
        }
        return (
          <SectionCard key={key} title={title} color={color}>
            <p className="text-sm text-slate-700 leading-relaxed">{value}</p>
          </SectionCard>
        )
      })}
    </div>
  )
}

export default function PatientSummary({ data, onDataChange, onClear }) {
  const [patientId, setPatientId] = useState('')
  const [loading, setLoading]     = useState(false)
  const [error, setError]         = useState(null)
  const [downloading, setDownloading] = useState(false)

  // data is a dict keyed by patient_id
  const currentSummary = patientId ? data[patientId.trim()] : null

  const handleGenerate = async () => {
    if (!patientId.trim()) { setError('Please enter a patient ID'); return }
    if (!/^\d+$/.test(patientId.trim())) { setError('Patient ID must be numeric'); return }
    setError(null)
    setLoading(true)
    try {
      const result = await generateSummary(patientId.trim())
      onDataChange(patientId.trim(), result)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Failed to generate summary')
    } finally {
      setLoading(false)
    }
  }

  const handleDownload = async () => {
    if (!currentSummary) return
    setDownloading(true)
    try {
      await downloadSummaryPdf(patientId.trim(), currentSummary)
    } catch (e) {
      setError('Failed to generate PDF')
    } finally {
      setDownloading(false)
    }
  }

  const handleClear = () => {
    setPatientId('')
    setError(null)
    onClear()
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Summarize Patient</h2>
          <p className="text-sm text-slate-500 mt-1">
            AI-powered clinical summary generated from the patient knowledge graph
          </p>
        </div>
        {Object.keys(data).length > 0 && (
          <button onClick={handleClear} className="btn-danger">Clear All</button>
        )}
      </div>

      {/* Controls */}
      <div className="card mb-6">
        <div className="flex gap-3">
          <div className="flex-1">
            <label className="block text-xs font-medium text-slate-600 mb-1">Patient ID</label>
            <input
              className="input"
              type="text"
              placeholder="e.g., 10000032"
              value={patientId}
              onChange={e => setPatientId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleGenerate()}
            />
          </div>
          <div className="flex items-end">
            <button
              onClick={handleGenerate}
              disabled={loading}
              className="btn-primary"
            >
              {loading && <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              {loading ? 'Generating…' : 'Generate Summary'}
            </button>
          </div>
        </div>

      </div>

      {error && <div className="mb-4"><Alert type="error"><span>{error}</span></Alert></div>}
      {loading && <Spinner text="Extracting knowledge graph and generating AI summary… this may take a minute." />}

      {currentSummary && !loading && (
        <div>
          <div className="mb-4">
            <Alert type="success">
              <span>Summary for Patient <strong>{patientId.trim()}</strong> — ready.</span>
            </Alert>
          </div>
          <SummaryDisplay
            summary={currentSummary}
            patientId={patientId.trim()}
            onDownload={handleDownload}
            downloading={downloading}
          />
        </div>
      )}

      {!currentSummary && !loading && patientId && data[patientId.trim()] === undefined && (
        <div className="text-center py-4 text-slate-400">
          <p className="text-sm">Enter a patient ID and click Generate Summary</p>
        </div>
      )}

      {!patientId && Object.keys(data).length === 0 && !loading && (
        <div className="text-center py-4 text-slate-400">
          <p className="text-sm">Enter a patient ID and click Generate Summary</p>
        </div>
      )}
    </div>
  )
}
