import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 300000, // 5 minutes for LLM calls
})

// ── Health ────────────────────────────────────────────────────────────────────
export const fetchHealth = () =>
  api.get('/health').then(r => r.data).catch(() => ({ neo4j: 'disconnected' }))


// ── Patients ──────────────────────────────────────────────────────────────────
export const findSimilarPatients = (patientId, topK = 20) =>
  api.get(`/patients/similarity/${patientId}`, { params: { top_k: topK } }).then(r => r.data)

// ── Summary ───────────────────────────────────────────────────────────────────
export const generateSummary = (patientId) =>
  api.post('/summary/generate', { patient_id: patientId }).then(r => r.data)

// ── Comparison ────────────────────────────────────────────────────────────────
export const generateComparison = (patient1Id, patient2Id) =>
  api.post('/comparison/generate', {
    patient1_id: patient1Id,
    patient2_id: patient2Id,
  }).then(r => r.data)

// ── Journey ───────────────────────────────────────────────────────────────────
export const getPatientJourney = (patientId) =>
  api.get(`/journey/${patientId}`).then(r => r.data)

// ── Diagnosis similarity ──────────────────────────────────────────────────────
export const findByDiagnosis = (diagnosisText, topK = 20) =>
  api.post('/downloads/diagnosis-search', {
    diagnosis_text: diagnosisText,
    top_k: topK,
  }).then(r => r.data)

// ── Downloads ─────────────────────────────────────────────────────────────────
export const downloadSummaryPdf = async (patientId, summaryJson) => {
  const resp = await api.post(
    '/downloads/summary',
    { patient_id: patientId, summary_json: summaryJson },
    { responseType: 'blob' },
  )
  _triggerDownload(resp.data, `patient_${patientId}_Summary.pdf`)
}

export const downloadComparisonPdf = async (p1Id, p2Id, comparisonJson) => {
  const resp = await api.post(
    '/downloads/comparison',
    { patient1_id: p1Id, patient2_id: p2Id, comparison_json: comparisonJson },
    { responseType: 'blob' },
  )
  _triggerDownload(resp.data, `patient_${p1Id}_vs_${p2Id}_Comparison.pdf`)
}

export const downloadJourneyPdf = async (patientId, journeyData) => {
  const resp = await api.post(
    '/downloads/journey',
    { patient_id: patientId, journey_data: journeyData },
    { responseType: 'blob' },
  )
  _triggerDownload(resp.data, `patient_${patientId}_Journey.pdf`)
}

function _triggerDownload(blob, filename) {
  const url = window.URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}
