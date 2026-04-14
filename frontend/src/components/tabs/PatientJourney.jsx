import { useState } from 'react'
import { getPatientJourney, downloadJourneyPdf } from '../../api/client'
import Spinner from '../ui/Spinner'
import Alert from '../ui/Alert'

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmt(ts) {
  if (!ts) return 'N/A'
  try {
    const d = new Date(ts)
    return d.toLocaleString('en-US', {
      month: 'long', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    })
  } catch { return ts }
}

function timeDiff(a, b) {
  if (!a || !b) return null
  const ms = new Date(b) - new Date(a)
  if (ms <= 0) return null
  const mins  = Math.floor(ms / 60000)
  const hrs   = Math.floor(mins / 60)
  const days  = Math.floor(hrs  / 24)
  const parts = []
  if (days)        parts.push(`${days} day${days !== 1 ? 's' : ''}`)
  if (hrs  % 24)   parts.push(`${hrs % 24} hour${hrs % 24 !== 1 ? 's' : ''}`)
  if (mins % 60)   parts.push(`${mins % 60} minute${mins % 60 !== 1 ? 's' : ''}`)
  return parts.length ? parts.join(', ') : 'less than a minute'
}

function ordinal(n) {
  const s = ['th','st','nd','rd'], v = n % 100
  return n + (s[(v-20)%10] || s[v] || s[0])
}

// ── Event type config ─────────────────────────────────────────────────────────
const EVENT_COLORS = {
  EmergencyDepartment: { bg: 'bg-red-600',    dot: 'bg-red-500',    label: 'ED Visit' },
  HospitalAdmission:  { bg: 'bg-clinical-700',dot: 'bg-clinical-500',label: 'Hospital Admission' },
  ICUStay:            { bg: 'bg-purple-700',  dot: 'bg-purple-500', label: 'ICU Stay' },
  UnitAdmission:      { bg: 'bg-teal-700',    dot: 'bg-teal-500',   label: 'Unit Admission' },
  LabEvent:           { bg: 'bg-cyan-700',    dot: 'bg-cyan-500',   label: 'Lab Tests' },
  MicrobiologyEvent:  { bg: 'bg-green-700',   dot: 'bg-green-500',  label: 'Microbiology' },
  Procedures:         { bg: 'bg-indigo-700',  dot: 'bg-indigo-500', label: 'Procedure' },
  Prescription:       { bg: 'bg-amber-600',   dot: 'bg-amber-500',  label: 'Prescription' },
  AdministeredMeds:   { bg: 'bg-orange-600',  dot: 'bg-orange-500', label: 'Medications Given' },
  Discharge:          { bg: 'bg-slate-700',   dot: 'bg-slate-500',  label: 'Discharge' },
  Transfer:           { bg: 'bg-pink-700',    dot: 'bg-pink-500',   label: 'Transfer' },
  PreviousPrescriptionMeds: { bg: 'bg-yellow-600', dot: 'bg-yellow-500', label: 'Previous Meds' },
  ChartEvent:         { bg: 'bg-slate-500',   dot: 'bg-slate-400',  label: 'Chart Event' },
}

function getEventConfig(label) {
  return EVENT_COLORS[label] || { bg: 'bg-slate-600', dot: 'bg-slate-400', label }
}

// ── Reusable table ────────────────────────────────────────────────────────────
function MatrixGrid({ items, cols = 3 }) {
  if (!items || !items.length) return null
  const colClass = cols === 1 ? 'grid-cols-1' : cols === 2 ? 'grid-cols-2' : 'grid-cols-3'
  return (
    <div className={`grid ${colClass} gap-2`}>
      {items.map((item, i) => (
        <div key={i} className="bg-slate-50 border border-slate-200 rounded px-3 py-2 text-xs text-slate-700">
          {item}
        </div>
      ))}
    </div>
  )
}

function LabTable({ results }) {
  if (!results || !results.length) return null

  const parsed = results.map(r => {
    const parts = r.split('=', 2)
    if (parts.length < 2) return { name: r, value: '', ref: '', abnormal: false }
    const name = parts[0].trim()
    let rest = parts[1].trim()
    const abnormal = rest.includes('[abnormal]')
    rest = rest.replace('[abnormal]', '').trim()
    let value = rest, ref = ''
    if (rest.includes('(ref:')) {
      const ri = rest.indexOf('(ref:')
      value = rest.slice(0, ri).trim()
      ref = rest.slice(ri + 5, rest.indexOf(')', ri)).trim()
    }
    // strip trailing category/specimen
    const commaIdx = value.lastIndexOf(',')
    if (commaIdx > 0) value = value.slice(0, commaIdx).trim()
    return { name, value, ref, abnormal }
  })

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200">
      <table className="w-full text-xs">
        <thead>
          <tr className="bg-slate-100">
            <th className="text-left px-3 py-2 font-semibold text-slate-600">Test</th>
            <th className="text-left px-3 py-2 font-semibold text-slate-600">Value</th>
            <th className="text-left px-3 py-2 font-semibold text-slate-600">Reference</th>
          </tr>
        </thead>
        <tbody>
          {parsed.map((r, i) => (
            <tr key={i} className={`border-t border-slate-100 ${r.abnormal ? 'bg-red-50' : ''}`}>
              <td className="px-3 py-1.5 text-slate-700">{r.name}</td>
              <td className={`px-3 py-1.5 font-medium ${r.abnormal ? 'text-red-600' : 'text-slate-800'}`}>
                {r.value}
              </td>
              <td className="px-3 py-1.5 text-slate-500">{r.ref || 'N/A'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function VitalsTable({ vitals }) {
  const rows = vitals.filter(v => v.value !== undefined && v.value !== null && v.value !== 'N/A')
  if (!rows.length) return null
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200">
      <table className="w-full text-xs">
        <thead>
          <tr className="bg-slate-100">
            <th className="text-left px-3 py-2 font-semibold text-slate-600">Vital Sign</th>
            <th className="text-left px-3 py-2 font-semibold text-slate-600">Value</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-t border-slate-100">
              <td className="px-3 py-1.5 text-slate-600">{r.label}</td>
              <td className="px-3 py-1.5 font-medium text-slate-800">{r.value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Event renderers ───────────────────────────────────────────────────────────
function EventCard({ event, gap }) {
  const label = event.labels?.[0] || 'Unknown'
  const props = event.properties || {}
  const children = event.children || {}
  const { bg, label: evtLabel } = getEventConfig(label)

  return (
    <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      {/* Header bar */}
      <div className={`${bg} px-4 py-2.5 flex items-center justify-between`}>
        <div className="flex items-center gap-2">
          <span className="text-white font-semibold text-sm">{evtLabel}</span>
        </div>
        <span className="text-white/80 text-xs">{fmt(event.timestamp)}</span>
      </div>

      {gap && (
        <div className="px-4 py-1.5 bg-slate-50 border-b border-slate-100">
          <p className="text-xs text-slate-500 italic">
            <span className="font-medium text-slate-600">{gap}</span> since last event
          </p>
        </div>
      )}

      <div className="p-4 space-y-3">
        {/* Render by event type */}
        {label === 'EmergencyDepartment' && <EDContent props={props} children={children} />}
        {label === 'HospitalAdmission'   && <HospAdmContent props={props} children={children} />}
        {label === 'ICUStay'             && <ICUContent props={props} />}
        {label === 'UnitAdmission'       && <UnitContent props={props} />}
        {label === 'LabEvent'            && <LabContent props={props} />}
        {label === 'MicrobiologyEvent'   && <MicroContent props={props} />}
        {label === 'Procedures'          && <ProcContent props={props} />}
        {label === 'Prescription'        && <RxContent props={props} />}
        {label === 'AdministeredMeds'    && <AdminMedsContent props={props} />}
        {label === 'PreviousPrescriptionMeds' && <PrevMedsContent props={props} />}
        {label === 'Transfer'            && <TransferContent props={props} />}
        {label === 'Discharge'           && <DischargeContent props={props} children={children} />}
        {label === 'ChartEvent'          && <p className="text-sm text-slate-500">Chart event recorded.</p>}
        {!EVENT_COLORS[label] && <GenericContent props={props} />}
      </div>
    </div>
  )
}

function Field({ label, value }) {
  if (!value || value === 'N/A') return null
  return (
    <div className="flex gap-2 text-sm">
      <span className="text-slate-500 shrink-0">{label}:</span>
      <span className="text-slate-800 font-medium">{value}</span>
    </div>
  )
}

function SectionLabel({ children }) {
  return <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mt-3 mb-1">{children}</p>
}

function EDContent({ props, children }) {
  const diag = children.Diagnosis?.[0]?.props
  const ia   = children.InitialAssessment?.[0]?.props
  const vitals = ia ? [
    { label: 'Blood Pressure',     value: ia.sbp && ia.dbp ? `${ia.sbp}/${ia.dbp} mmHg` : null },
    { label: 'Heart Rate',         value: ia.heartrate ? `${ia.heartrate} bpm` : null },
    { label: 'Respiratory Rate',   value: ia.resprate   ? `${ia.resprate} breaths/min` : null },
    { label: 'O₂ Saturation',      value: ia.o2sat      ? `${ia.o2sat}%` : null },
    { label: 'Temperature',        value: ia.temperature ? `${ia.temperature}°F` : null },
    { label: 'Pain Score',         value: ia.pain },
    { label: 'Acuity Level',       value: ia.acuity },
  ] : []

  return (
    <>
      <Field label="ED Visit #"      value={props.ed_seq_num} />
      <Field label="Arrival via"     value={props.arrival_transport} />
      <Field label="Departure"       value={props.outtime} />
      <Field label="Duration"        value={props.period} />
      <Field label="Disposition"     value={props.disposition} />

      {ia?.chiefcomplaint && (
        <>
          <SectionLabel>Chief Complaint</SectionLabel>
          <p className="text-sm text-slate-700">{ia.chiefcomplaint}</p>
        </>
      )}
      {vitals.some(v => v.value) && (
        <>
          <SectionLabel>Triage Vitals</SectionLabel>
          <VitalsTable vitals={vitals} />
        </>
      )}
      {diag?.complete_diagnosis?.length > 0 && (
        <>
          <SectionLabel>Initial Diagnosis</SectionLabel>
          <MatrixGrid items={diag.complete_diagnosis} />
        </>
      )}
    </>
  )
}

function HospAdmContent({ props, children }) {
  const ph   = children.PatientPastHistory?.[0]?.props
  const hpi  = children.HPISummary?.[0]?.props
  const av   = children.AdmissionVitals?.[0]?.props
  const labs = children.AdmissionLabs?.[0]?.props
  const meds = children.AdmissionMedications?.[0]?.props

  const vitals = av ? [
    { label: 'Blood Pressure',   value: av.Blood_Pressure ? `${av.Blood_Pressure} mmHg` : null },
    { label: 'Heart Rate',       value: av.Heart_Rate ? `${av.Heart_Rate} bpm` : null },
    { label: 'Respiratory Rate', value: av.Respiratory_Rate ? `${av.Respiratory_Rate} breaths/min` : null },
    { label: 'Temperature',      value: av.Temperature ? `${av.Temperature}°F` : null },
    { label: 'O₂ Saturation',    value: av.SpO2 },
    { label: 'General',          value: av.General },
  ] : []

  return (
    <>
      {props.hospital_admission_sequence_number && (
        <Field label="Admission #" value={ordinal(props.hospital_admission_sequence_number)} />
      )}
      <Field label="From"           value={props.admission_location} />
      <Field label="Type"           value={props.admission_type} />
      <Field label="Service"        value={props.service} />
      <Field label="Insurance"      value={props.insurance} />
      <Field label="Chief Complaint" value={props.chief_complaint} />
      <Field label="Race"           value={props.race} />
      <Field label="Marital Status" value={props.marital_status} />

      {hpi?.summary && (
        <>
          <SectionLabel>History of Present Illness</SectionLabel>
          <p className="text-sm text-slate-700 leading-relaxed">{hpi.summary}</p>
        </>
      )}
      {ph && (
        <>
          <SectionLabel>Past History</SectionLabel>
          <Field label="Medical"  value={ph.past_medical_history} />
          <Field label="Family"   value={ph.family_history} />
          <Field label="Social"   value={ph.social_history} />
        </>
      )}
      {vitals.some(v => v.value) && (
        <>
          <SectionLabel>Admission Vitals</SectionLabel>
          <VitalsTable vitals={vitals} />
        </>
      )}
      {labs?.lab_tests?.length > 0 && (
        <>
          <SectionLabel>Admission Lab Results</SectionLabel>
          <MatrixGrid items={labs.lab_tests} />
        </>
      )}
      {meds?.medications?.length > 0 && (
        <>
          <SectionLabel>Admission Medications ({meds.medications.length})</SectionLabel>
          <MatrixGrid items={meds.medications} />
        </>
      )}
    </>
  )
}

function ICUContent({ props }) {
  return (
    <>
      <Field label="Care Unit"   value={props.careunit} />
      <Field label="Departure"   value={props.outtime} />
      <Field label="Duration"    value={props.period} />
      <Field label="LOS (days)"  value={props.los} />
      <Field label="Service"     value={props.service_given} />
      {props.first_careunit !== props.last_careunit && props.first_careunit && (
        <Field label="Transfer" value={`${props.first_careunit} → ${props.last_careunit}`} />
      )}
    </>
  )
}

function UnitContent({ props }) {
  return (
    <>
      <Field label="Ward"       value={props.careunit} />
      <Field label="Departure"  value={props.outtime} />
      <Field label="Duration"   value={props.period} />
      <Field label="Service"    value={props.service_given} />
    </>
  )
}

function LabContent({ props }) {
  return (
    <>
      <div className="flex gap-4 text-sm mb-2">
        <span className="text-slate-600">Total tests: <strong className="text-slate-800">{props.lab_count}</strong></span>
        {props.abnormal_count > 0 && (
          <span className="text-red-600">{props.abnormal_count} abnormal</span>
        )}
      </div>
      {props.lab_results?.length > 0 && <LabTable results={props.lab_results} />}
    </>
  )
}

function MicroContent({ props }) {
  return (
    <>
      <p className="text-sm text-slate-600 mb-2">
        <strong className="text-slate-800">{props.micro_count}</strong> microbiology result(s)
      </p>
      {props.micro_results?.map((r, i) => (
        <div key={i} className="text-xs bg-green-50 border border-green-100 rounded px-3 py-1.5 mb-1 text-slate-700">
          {r}
        </div>
      ))}
    </>
  )
}

function ProcContent({ props }) {
  return (
    <>
      <p className="text-sm text-slate-600 mb-2">
        <strong className="text-slate-800">{props.procedure_count}</strong> procedure(s) · Source: {props.source}
      </p>
      <MatrixGrid items={props.procedures} />
    </>
  )
}

function RxContent({ props }) {
  return (
    <>
      <p className="text-sm text-slate-600 mb-2">
        <strong className="text-slate-800">{props.medicine_count}</strong> medication(s) prescribed
      </p>
      <MatrixGrid items={props.medicines} />
    </>
  )
}

function AdminMedsContent({ props }) {
  return (
    <>
      <p className="text-sm text-slate-600 mb-2">
        <strong className="text-slate-800">{props.medication_count || props.medications?.length}</strong> medication(s) administered
      </p>
      <MatrixGrid items={props.medications} />
    </>
  )
}

function PrevMedsContent({ props }) {
  return (
    <>
      <p className="text-sm text-slate-600 mb-2">
        <strong className="text-slate-800">{props.medication_count || props.medications?.length}</strong> previous medication(s) on record
      </p>
      <MatrixGrid items={props.medications} />
    </>
  )
}

function TransferContent({ props }) {
  return <Field label="Transferred to" value={props.careunit} />
}

function DischargeContent({ props, children }) {
  const note    = children.DischargeClinicalNote?.[0]?.props
  const diag    = children.Diagnosis?.[0]?.props
  const started = children.MedicationStarted?.[0]?.props?.medications || []
  const stopped = children.MedicationStopped?.[0]?.props?.medications || []
  const avoid   = children.MedicationToAvoid?.[0]?.props?.medications  || []
  const allergies = children.AllergyIdentified?.map(a => a.props.allergy_name).filter(Boolean) || []

  return (
    <>
      <Field label="Discharged from" value={props.careunit} />
      <Field label="Disposition"     value={props.disposition} />
      {props.major_procedure && props.major_procedure !== 'None' && (
        <Field label="Major Procedure" value={props.major_procedure} />
      )}
      {props.allergies && (
        <div className="bg-amber-50 border border-amber-200 rounded px-3 py-2 text-sm text-amber-800">
          <strong>Allergies:</strong> {props.allergies}
        </div>
      )}
      {allergies.length > 0 && (
        <>
          <SectionLabel>Allergy List</SectionLabel>
          <MatrixGrid items={allergies} />
        </>
      )}
      {diag?.primary_diagnoses?.length > 0 && (
        <>
          <SectionLabel>Primary Diagnoses</SectionLabel>
          <MatrixGrid items={diag.primary_diagnoses} />
        </>
      )}
      {diag?.secondary_diagnoses?.length > 0 && (
        <>
          <SectionLabel>Secondary Diagnoses</SectionLabel>
          <MatrixGrid items={diag.secondary_diagnoses} />
        </>
      )}
      {note?.hospital_course && (
        <>
          <SectionLabel>Hospital Course</SectionLabel>
          <p className="text-sm text-slate-700 leading-relaxed">{note.hospital_course}</p>
        </>
      )}
      {note?.antibiotic_plan && (
        <>
          <SectionLabel>Antibiotic Plan</SectionLabel>
          <p className="text-sm text-slate-700">{note.antibiotic_plan}</p>
        </>
      )}
      {(note?.activity_status || note?.mental_status || note?.code_status) && (
        <>
          <SectionLabel>Discharge Status</SectionLabel>
          <div className="grid grid-cols-3 gap-2">
            {note.activity_status && <div className="bg-slate-50 border border-slate-200 rounded p-2 text-xs"><p className="text-slate-400">Activity</p><p className="font-medium">{note.activity_status}</p></div>}
            {note.mental_status   && <div className="bg-slate-50 border border-slate-200 rounded p-2 text-xs"><p className="text-slate-400">Mental Status</p><p className="font-medium">{note.mental_status}</p></div>}
            {note.code_status     && <div className="bg-slate-50 border border-slate-200 rounded p-2 text-xs"><p className="text-slate-400">Code Status</p><p className="font-medium">{note.code_status}</p></div>}
          </div>
        </>
      )}
      {note?.discharge_instructions && (
        <>
          <SectionLabel>Discharge Instructions</SectionLabel>
          <p className="text-sm text-slate-700 leading-relaxed">{note.discharge_instructions}</p>
        </>
      )}
      {(started.length || stopped.length || avoid.length) > 0 && (
        <>
          <SectionLabel>Medication Changes</SectionLabel>
          <div className="grid grid-cols-3 gap-2">
            {started.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-green-700 mb-1">Started</p>
                {started.map((m, i) => <p key={i} className="text-xs text-slate-700 mb-0.5">• {m}</p>)}
              </div>
            )}
            {stopped.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-red-700 mb-1">Stopped</p>
                {stopped.map((m, i) => <p key={i} className="text-xs text-slate-700 mb-0.5">• {m}</p>)}
              </div>
            )}
            {avoid.length > 0 && (
              <div>
                <p className="text-xs font-semibold text-amber-700 mb-1">To Avoid</p>
                {avoid.map((m, i) => <p key={i} className="text-xs text-slate-700 mb-0.5">• {m}</p>)}
              </div>
            )}
          </div>
        </>
      )}
    </>
  )
}

function GenericContent({ props }) {
  const keys = Object.keys(props).filter(k => !['event_id', 'period'].includes(k))
  return (
    <div className="space-y-1">
      {keys.slice(0, 6).map(k => (
        <Field key={k} label={k.replace(/_/g, ' ')} value={String(props[k])} />
      ))}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function PatientJourney({ data, onDataChange, onClear }) {
  const [patientId, setPatientId] = useState('')
  const [loading, setLoading]     = useState(false)
  const [error, setError]         = useState(null)
  const [downloading, setDownloading] = useState(false)

  const currentJourney = patientId ? data[patientId.trim()] : null

  const handleGenerate = async () => {
    if (!patientId.trim()) { setError('Please enter a patient ID'); return }
    if (!/^\d+$/.test(patientId.trim())) { setError('Patient ID must be numeric'); return }
    setError(null)
    setLoading(true)
    try {
      const result = await getPatientJourney(patientId.trim())
      onDataChange(patientId.trim(), result)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Failed to load journey')
    } finally {
      setLoading(false)
    }
  }

  const handleDownload = async () => {
    if (!currentJourney) return
    setDownloading(true)
    try {
      await downloadJourneyPdf(patientId.trim(), currentJourney)
    } catch (e) {
      setError('Failed to generate PDF: ' + (e.response?.data?.detail || e.message))
    } finally {
      setDownloading(false)
    }
  }

  const patient = currentJourney?.patient?.properties || {}
  const events  = currentJourney?.events || []

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Patient Journey</h2>
          <p className="text-sm text-slate-500 mt-1">
            Complete chronological clinical event timeline
          </p>
        </div>
        {Object.keys(data).length > 0 && (
          <button onClick={() => { setPatientId(''); setError(null); onClear() }} className="btn-danger">
            Clear All
          </button>
        )}
      </div>

      {/* Controls */}
      <div className="card mb-6">
        <div className="flex gap-3">
          <div className="flex-1">
            <label className="block text-xs font-medium text-slate-600 mb-1">Patient ID</label>
            <input
              className="input"
              placeholder="e.g., 10000032"
              value={patientId}
              onChange={e => setPatientId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleGenerate()}
            />
          </div>
          <div className="flex items-end">
            <button onClick={handleGenerate} disabled={loading} className="btn-primary">
              {loading && <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />}
              {loading ? 'Loading…' : 'Generate Journey'}
            </button>
          </div>
        </div>
      </div>

      {error && <div className="mb-4"><Alert type="error"><span>{error}</span></Alert></div>}
      {loading && <Spinner text="Extracting patient journey from knowledge graph…" />}

      {currentJourney && !loading && (
        <>
          <div className="mb-4 flex items-center justify-between">
            <Alert type="success">
              <span>
                Journey for Patient <strong>{patientId.trim()}</strong> — {events.length} events found.
              </span>
            </Alert>
            <button onClick={handleDownload} disabled={downloading} className="btn-secondary ml-4 shrink-0">
              {downloading && <span className="w-4 h-4 border-2 border-slate-400 border-t-transparent rounded-full animate-spin" />}
              {downloading ? 'Generating…' : 'Download PDF'}
            </button>
          </div>

          {/* Patient info */}
          <div className="card mb-6">
            <h3 className="section-heading">Patient Information</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              {[
                { label: 'Age',         value: patient.anchor_age },
                { label: 'Gender',      value: patient.gender },
                { label: 'Race',        value: patient.race },
                { label: 'Admissions',  value: patient.total_number_of_admissions },
              ].map(({ label, value }) => (
                <div key={label} className="bg-clinical-50 border border-clinical-100 rounded-lg px-4 py-3">
                  <p className="text-xs text-slate-500 mb-1">{label}</p>
                  <p className="font-semibold text-slate-800">{value ?? 'N/A'}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Event legend */}
          <div className="card mb-6">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">Event Legend</h3>
            <div className="flex flex-wrap gap-2">
              {Object.entries(EVENT_COLORS).map(([k, v]) => (
                <span key={k} className={`flex items-center gap-1.5 px-2 py-1 ${v.bg} bg-opacity-10 rounded text-xs`}>
                  <span className={`w-2 h-2 rounded-full ${v.dot}`} />
                  <span className="text-slate-700">{v.label}</span>
                </span>
              ))}
            </div>
          </div>

          {/* Timeline */}
          <div className="space-y-4">
            {events.map((event, i) => {
              const gap = i > 0 ? timeDiff(events[i - 1].timestamp, event.timestamp) : null
              return <EventCard key={i} event={event} gap={gap} />
            })}
          </div>
        </>
      )}

      {!currentJourney && !loading && (
        <div className="text-center py-4 text-slate-400">
          <p className="text-sm">Enter a patient ID and click Generate Journey</p>
        </div>
      )}
    </div>
  )
}
