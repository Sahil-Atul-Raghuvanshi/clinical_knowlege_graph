import { useState } from 'react'
import { generateComparison, downloadComparisonPdf } from '../../api/client'
import Spinner from '../ui/Spinner'
import Alert from '../ui/Alert'
import SectionCard from '../ui/SectionCard'
import SubSection from '../ui/SubSection'
import BulletList from '../ui/BulletList'

/** Renders a two-column comparison layout for lists unique to each patient. */
function TwoColumn({ p1Id, p2Id, p1Items, p2Items }) {
  return (
    <div className="grid grid-cols-2 gap-4">
      <div>
        <p className="text-xs font-semibold text-clinical-700 bg-clinical-50 border border-clinical-200 rounded px-2 py-1 mb-2">
          Patient {p1Id}
        </p>
        <BulletList items={p1Items} emptyText="None" />
      </div>
      <div>
        <p className="text-xs font-semibold text-teal-700 bg-teal-50 border border-teal-200 rounded px-2 py-1 mb-2">
          Patient {p2Id}
        </p>
        <BulletList items={p2Items} emptyText="None" />
      </div>
    </div>
  )
}

function ComparisonDisplay({ comparison, p1Id, p2Id, onDownload, downloading }) {
  const sections = [
    {
      key: 'demographics_comparison', title: 'Demographics Comparison', color: 'clinical',
      render: (d) => (
        <>
          {d.similarities?.length > 0 && <SubSection title="Similarities"><BulletList items={d.similarities} /></SubSection>}
          {d.differences?.length  > 0 && <SubSection title="Differences"><BulletList items={d.differences} /></SubSection>}
        </>
      ),
    },
    {
      key: 'presentation_comparison', title: 'Presentation Comparison', color: 'teal',
      render: (d) => (
        <>
          {d.similarities?.length > 0 && <SubSection title="Similarities"><BulletList items={d.similarities} /></SubSection>}
          {d.differences?.length  > 0 && <SubSection title="Differences"><BulletList items={d.differences} /></SubSection>}
          {d.temporal_differences   && <SubSection title="Temporal Differences"><p className="text-sm text-slate-700 leading-relaxed">{d.temporal_differences}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'diagnoses_comparison', title: 'Diagnoses Comparison', color: 'rose',
      render: (d) => (
        <>
          {d.common_diagnoses?.length > 0 && <SubSection title="Common Diagnoses"><BulletList items={d.common_diagnoses} /></SubSection>}
          <SubSection title="Unique Diagnoses">
            <TwoColumn p1Id={p1Id} p2Id={p2Id} p1Items={d.unique_to_patient1} p2Items={d.unique_to_patient2} />
          </SubSection>
          {d.severity_comparison && <SubSection title="Severity Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.severity_comparison}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'clinical_course_comparison', title: 'Clinical Course Comparison', color: 'indigo',
      render: (d) => (
        <>
          {d.similarities?.length > 0 && <SubSection title="Similarities"><BulletList items={d.similarities} /></SubSection>}
          {d.differences?.length  > 0 && <SubSection title="Differences"><BulletList items={d.differences} /></SubSection>}
          {d.temporal_sequence_comparison && <SubSection title="Temporal Sequence Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.temporal_sequence_comparison}</p></SubSection>}
          {d.length_of_stay_comparison    && <SubSection title="Length of Stay"><p className="text-sm text-slate-700 leading-relaxed">{d.length_of_stay_comparison}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'procedures_comparison', title: 'Procedures Comparison', color: 'purple',
      render: (d) => (
        <>
          {d.common_procedures?.length > 0 && <SubSection title="Common Procedures"><BulletList items={d.common_procedures} /></SubSection>}
          <SubSection title="Unique Procedures">
            <TwoColumn p1Id={p1Id} p2Id={p2Id} p1Items={d.unique_to_patient1} p2Items={d.unique_to_patient2} />
          </SubSection>
          {d.timing_comparison && <SubSection title="Timing Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.timing_comparison}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'medications_comparison', title: 'Medications Comparison', color: 'amber',
      render: (d) => (
        <>
          {d.common_medications?.length > 0 && <SubSection title="Common Medications"><BulletList items={d.common_medications} /></SubSection>}
          <SubSection title="Unique Medications">
            <TwoColumn p1Id={p1Id} p2Id={p2Id} p1Items={d.unique_to_patient1} p2Items={d.unique_to_patient2} />
          </SubSection>
          {d.timing_comparison && <SubSection title="Timing Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.timing_comparison}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'lab_findings_comparison', title: 'Laboratory Findings Comparison', color: 'cyan',
      render: (d) => (
        <>
          {d.similar_abnormalities?.length > 0 && <SubSection title="Similar Abnormalities"><BulletList items={d.similar_abnormalities} /></SubSection>}
          <SubSection title="Unique Abnormalities">
            <TwoColumn p1Id={p1Id} p2Id={p2Id} p1Items={d.unique_abnormalities_patient1} p2Items={d.unique_abnormalities_patient2} />
          </SubSection>
          {d.temporal_patterns && <SubSection title="Temporal Patterns"><p className="text-sm text-slate-700 leading-relaxed">{d.temporal_patterns}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'microbiology_comparison', title: 'Microbiology Comparison', color: 'green',
      render: (d) => (
        <>
          {d.common_findings?.length > 0 && <SubSection title="Common Findings"><BulletList items={d.common_findings} /></SubSection>}
          <SubSection title="Unique Findings">
            <TwoColumn p1Id={p1Id} p2Id={p2Id} p1Items={d.unique_to_patient1} p2Items={d.unique_to_patient2} />
          </SubSection>
        </>
      ),
    },
    {
      key: 'outcomes_comparison', title: 'Outcomes Comparison', color: 'slate',
      render: (d) => (
        <>
          {d.discharge_comparison  && <SubSection title="Discharge Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.discharge_comparison}</p></SubSection>}
          {d.recovery_trajectory   && <SubSection title="Recovery Trajectory"><p className="text-sm text-slate-700 leading-relaxed">{d.recovery_trajectory}</p></SubSection>}
          {d.key_differences?.length > 0 && <SubSection title="Key Differences"><BulletList items={d.key_differences} /></SubSection>}
        </>
      ),
    },
    {
      key: 'temporal_analysis', title: 'Temporal Analysis', color: 'orange',
      render: (d) => (
        <>
          {d.event_sequence_comparison && <SubSection title="Event Sequence Comparison"><p className="text-sm text-slate-700 leading-relaxed">{d.event_sequence_comparison}</p></SubSection>}
          {d.critical_timepoints       && <SubSection title="Critical Timepoints"><p className="text-sm text-slate-700 leading-relaxed">{d.critical_timepoints}</p></SubSection>}
          {d.timing_patterns           && <SubSection title="Timing Patterns"><p className="text-sm text-slate-700 leading-relaxed">{d.timing_patterns}</p></SubSection>}
        </>
      ),
    },
    {
      key: 'clinical_insights', title: 'Clinical Insights', color: 'teal',
      render: (d) => (
        <>
          {d.why_similar     && <SubSection title="Why These Patients Are Similar"><p className="text-sm text-slate-700 leading-relaxed">{d.why_similar}</p></SubSection>}
          {d.why_different   && <SubSection title="Why These Patients Differ"><p className="text-sm text-slate-700 leading-relaxed">{d.why_different}</p></SubSection>}
          {d.lessons_learned && <SubSection title="Lessons Learned"><p className="text-sm text-slate-700 leading-relaxed">{d.lessons_learned}</p></SubSection>}
        </>
      ),
    },
  ]

  return (
    <div className="space-y-5">
      {/* Download */}
      <div className="flex justify-end">
        <button onClick={onDownload} disabled={downloading} className="btn-secondary">
          {downloading && <span className="w-4 h-4 border-2 border-slate-400 border-t-transparent rounded-full animate-spin" />}
          {downloading ? 'Generating PDF…' : 'Download PDF'}
        </button>
      </div>

      {/* Summary overview */}
      {comparison.comparison_summary && (
        <SectionCard title="Comparison Overview" color="clinical">
          <div className="flex items-center gap-6 mb-4">
            <div className="flex-1 text-center bg-clinical-50 border border-clinical-200 rounded-lg p-3">
              <p className="text-xs text-slate-500 mb-1">Patient 1</p>
              <p className="font-mono font-bold text-clinical-800 text-lg">{p1Id}</p>
            </div>
            <div className="text-slate-300 font-bold text-2xl">vs</div>
            <div className="flex-1 text-center bg-teal-50 border border-teal-200 rounded-lg p-3">
              <p className="text-xs text-slate-500 mb-1">Patient 2</p>
              <p className="font-mono font-bold text-teal-800 text-lg">{p2Id}</p>
            </div>
          </div>
          <p className="text-sm text-slate-700 leading-relaxed">{comparison.comparison_summary}</p>
        </SectionCard>
      )}

      {/* Dynamic sections */}
      {sections.map(({ key, title, color, render }) => {
        const sectionData = comparison[key]
        if (!sectionData) return null
        return (
          <SectionCard key={key} title={title} color={color}>
            {render(sectionData)}
          </SectionCard>
        )
      })}
    </div>
  )
}

export default function PatientComparison({ data, onDataChange, onClear }) {
  const [p1Id, setP1Id]         = useState('')
  const [p2Id, setP2Id]         = useState('')
  const [loading, setLoading]   = useState(false)
  const [error, setError]       = useState(null)
  const [downloading, setDownloading] = useState(false)

  const cacheKey = p1Id.trim() && p2Id.trim() ? `${p1Id.trim()}_${p2Id.trim()}` : null
  const currentComparison = cacheKey ? data[cacheKey] : null

  const handleGenerate = async () => {
    if (!p1Id.trim() || !p2Id.trim()) { setError('Please enter both patient IDs'); return }
    if (!/^\d+$/.test(p1Id.trim()) || !/^\d+$/.test(p2Id.trim())) { setError('Patient IDs must be numeric'); return }
    if (p1Id.trim() === p2Id.trim()) { setError('Please enter two different patient IDs'); return }
    setError(null)
    setLoading(true)
    try {
      const result = await generateComparison(p1Id.trim(), p2Id.trim())
      onDataChange(cacheKey, result)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Failed to generate comparison')
    } finally {
      setLoading(false)
    }
  }

  const handleDownload = async () => {
    if (!currentComparison) return
    setDownloading(true)
    try {
      await downloadComparisonPdf(p1Id.trim(), p2Id.trim(), currentComparison)
    } catch (e) {
      setError('Failed to generate PDF')
    } finally {
      setDownloading(false)
    }
  }

  return (
    <div className="p-6 max-w-5xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Compare Patients</h2>
          <p className="text-sm text-slate-500 mt-1">
            AI-powered side-by-side comparison of two patients' clinical journeys
          </p>
        </div>
        {Object.keys(data).length > 0 && (
          <button onClick={() => { setP1Id(''); setP2Id(''); setError(null); onClear() }} className="btn-danger">
            Clear All
          </button>
        )}
      </div>

      {/* Controls */}
      <div className="card mb-6">
        <div className="grid grid-cols-2 gap-4 mb-4">
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Patient 1 ID</label>
            <input
              className="input"
              placeholder="e.g., 10000032"
              value={p1Id}
              onChange={e => setP1Id(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-slate-600 mb-1">Patient 2 ID</label>
            <input
              className="input"
              placeholder="e.g., 10000033"
              value={p2Id}
              onChange={e => setP2Id(e.target.value)}
            />
          </div>
        </div>
        <button onClick={handleGenerate} disabled={loading} className="btn-primary w-full">
          {loading && <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
          {loading ? 'Generating Comparison…' : 'Generate Comparison'}
        </button>
      </div>

      {error && <div className="mb-4"><Alert type="error"><span>{error}</span></Alert></div>}
      {loading && <Spinner text="Extracting knowledge graphs and generating AI comparison… this may take 1-2 minutes." />}

      {currentComparison && !loading && (
        <div>
          <div className="mb-4">
            <Alert type="success">
              <span>Comparison of Patient <strong>{p1Id.trim()}</strong> vs <strong>{p2Id.trim()}</strong> — ready.</span>
            </Alert>
          </div>
          <ComparisonDisplay
            comparison={currentComparison}
            p1Id={p1Id.trim()}
            p2Id={p2Id.trim()}
            onDownload={handleDownload}
            downloading={downloading}
          />
        </div>
      )}

      {!currentComparison && !loading && (
        <div className="text-center py-4 text-slate-400">
          <p className="text-sm">Enter two patient IDs and click Generate Comparison</p>
        </div>
      )}
    </div>
  )
}
