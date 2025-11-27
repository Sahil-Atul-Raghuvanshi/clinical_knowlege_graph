"""
Enhanced text extractor that uses important attributes from nodes
Extracts comprehensive text data for embedding generation
"""
import logging
import sys
import re
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add Scripts directory to path for utils imports
# This file is at: Scripts/Create_Embeddings/full_patient_embeddings/enhanced_text_extractor.py
# So parent.parent.parent is Scripts/
scripts_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(scripts_dir))

from utils.neo4j_connection import Neo4jConnection

logger = logging.getLogger(__name__)

# Initialize stop words (lazy loading)
_stop_words = None

def _get_stop_words():
    """Get English stop words, downloading if necessary"""
    global _stop_words
    if _stop_words is None:
        try:
            import nltk
            try:
                from nltk.corpus import stopwords
                _stop_words = set(stopwords.words('english'))
            except LookupError:
                # Download stopwords if not available
                logger.info("Downloading NLTK stopwords data...")
                nltk.download('stopwords', quiet=True)
                from nltk.corpus import stopwords
                _stop_words = set(stopwords.words('english'))
            logger.info(f"Loaded {len(_stop_words)} stop words")
        except ImportError:
            logger.warning("NLTK not installed. Install with: pip install nltk. Stop word removal will be skipped.")
            _stop_words = set()  # Empty set - no stop words removed
    return _stop_words

def remove_stop_words(text: str) -> str:
    """
    Remove stop words from text using NLP while preserving structure
    
    Args:
        text: Input text (may contain structured format with colons, pipes, etc.)
        
    Returns:
        Text with stop words removed, structure preserved
    """
    if not text or not text.strip():
        return text
    
    stop_words = _get_stop_words()
    if not stop_words:
        # NLTK not available, return original text
        return text
    
    # Tokenize text into words while preserving structure
    # Use word boundaries to find words, keep everything else
    def process_segment(segment):
        """Process a text segment, removing stop words"""
        # Split into words and non-words (preserving spaces and punctuation)
        tokens = []
        words = re.finditer(r'\b\w+\b', segment)
        last_end = 0
        
        for match in words:
            # Add text before this word
            if match.start() > last_end:
                before_text = segment[last_end:match.start()]
                tokens.append(before_text)
            
            # Check if word is a stop word
            word = match.group()
            if word.lower() not in stop_words:
                tokens.append(word)
            # Otherwise skip the stop word (the spaces/punctuation around it are preserved)
            last_end = match.end()
        
        # Add remaining text
        if last_end < len(segment):
            tokens.append(segment[last_end:])
        
        result = ''.join(tokens)
        # Clean up multiple spaces that might result from removed stop words
        result = re.sub(r'\s+', ' ', result)
        return result
    
    # Split by major separators to preserve structure
    # Split on | and : but keep them
    parts = re.split(r'([|:])', text)
    
    processed_parts = []
    for part in parts:
        if part in ['|', ':']:
            # Keep separators as-is
            processed_parts.append(part)
        else:
            # Process this segment to remove stop words
            processed_parts.append(process_segment(part))
    
    # Reconstruct full text
    result = ''.join(processed_parts)
    # Clean up multiple spaces
    result = re.sub(r'\s+', ' ', result)
    # Clean up spaces around separators
    result = re.sub(r'\s*([|:])\s*', r' \1 ', result)
    # Final cleanup
    result = re.sub(r'\s+', ' ', result)
    
    return result.strip()


class EnhancedTextExtractor:
    """Extracts text data using important node attributes"""
    
    def __init__(self, connection: Neo4jConnection):
        """
        Initialize enhanced text extractor
        
        Args:
            connection: Neo4j connection instance
        """
        self.connection = connection
    
    def extract_patient_text_data(self, patient_id: str) -> Dict[str, Any]:
        """
        Extract comprehensive text data for a patient using important attributes
        Uses multiple simpler queries for better performance
        
        Args:
            patient_id: Patient identifier
            
        Returns:
            Dictionary with categorized text data
        """
        text_data = {'patient_id': patient_id}
        
        # Patient attributes
        query_patient = """
        MATCH (p:Patient {subject_id: toInteger($patient_id)})
        RETURN {
            gender: p.gender,
            age: p.anchor_age,
            year_group: p.anchor_year_group,
            dod: p.dod,
            total_admissions: p.total_number_of_admissions
        } AS patient_attrs
        """
        result = self.connection.execute_query(query_patient, {'patient_id': patient_id})
        text_data['patient_attributes'] = result[0]['patient_attrs'] if result else {}
        
        # Emergency Department visits
        query_ed = """
        MATCH (p:Patient {subject_id: toInteger($patient_id)})-[:VISITED_ED]->(ed:EmergencyDepartment)
        RETURN collect(DISTINCT {
            period: ed.period,
            disposition: ed.disposition,
            arrival_transport: ed.arrival_transport
        }) AS ed_visits
        """
        result = self.connection.execute_query(query_ed, {'patient_id': patient_id})
        text_data['ed_visits'] = result[0]['ed_visits'] if result else []
        
        # Hospital Admissions
        query_admissions = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            admission_type: ha.admission_type,
            admission_location: ha.admission_location,
            discharge_location: ha.discharge_location,
            insurance: ha.insurance,
            language: ha.language,
            marital_status: ha.marital_status,
            race: ha.race,
            service: ha.service,
            chief_complaint: ha.chief_complaint,
            social_history: ha.social_history,
            family_history: ha.family_history,
            hospital_expire_flag: ha.hospital_expire_flag
        }) AS admissions
        """
        result = self.connection.execute_query(query_admissions, {'patient_id': patient_id})
        text_data['admissions'] = result[0]['admissions'] if result else []
        
        # Unit Admissions
        query_units = """
        MATCH (ua:UnitAdmission {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            careunit: ua.careunit,
            period: ua.period,
            service_given: ua.service_given
        }) AS unit_admissions
        """
        result = self.connection.execute_query(query_units, {'patient_id': patient_id})
        text_data['unit_admissions'] = result[0]['unit_admissions'] if result else []
        
        # ICU Stays
        query_icu = """
        MATCH (icu:ICUStay {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            careunit: icu.careunit,
            period: icu.period,
            service_given: icu.service_given,
            first_careunit: icu.first_careunit,
            last_careunit: icu.last_careunit,
            los: icu.los
        }) AS icu_stays
        """
        result = self.connection.execute_query(query_icu, {'patient_id': patient_id})
        text_data['icu_stays'] = result[0]['icu_stays'] if result else []
        
        # Discharges
        query_discharges = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            disposition: d.disposition,
            facility_name: d.facility_name,
            allergies: d.allergies,
            major_procedure: d.major_procedure
        }) AS discharges
        """
        result = self.connection.execute_query(query_discharges, {'patient_id': patient_id})
        text_data['discharges'] = result[0]['discharges'] if result else []
        
        # Initial Assessments
        query_assessments = """
        MATCH (ed:EmergencyDepartment {subject_id: toInteger($patient_id)})-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia:InitialAssessment)
        RETURN collect(DISTINCT {
            temperature: ia.temperature,
            heartrate: ia.heartrate,
            resprate: ia.resprate,
            o2sat: ia.o2sat,
            sbp: ia.sbp,
            dbp: ia.dbp,
            pain: ia.pain,
            acuity: ia.acuity,
            chiefcomplaint: ia.chiefcomplaint
        }) AS assessments
        """
        result = self.connection.execute_query(query_assessments, {'patient_id': patient_id})
        text_data['assessments'] = result[0]['assessments'] if result else []
        
        # Diagnoses
        query_diagnoses = """
        MATCH (diag:Diagnosis {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            ed_diagnosis: diag.ed_diagnosis,
            complete_diagnosis: diag.complete_diagnosis,
            primary_diagnoses: diag.primary_diagnoses,
            secondary_diagnoses: diag.secondary_diagnoses
        }) AS diagnoses
        """
        result = self.connection.execute_query(query_diagnoses, {'patient_id': patient_id})
        text_data['diagnoses'] = result[0]['diagnoses'] if result else []
        
        # Prescriptions (from ED, HospitalAdmission, UnitAdmission, and ICUStay)
        query_prescriptions = """
        MATCH (p:Patient {subject_id: toInteger($patient_id)})
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)
        OPTIONAL MATCH (p)-[:ADMITTED_TO_UNIT]->(ua:UnitAdmission)
        OPTIONAL MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})
        OPTIONAL MATCH (icu:ICUStay {subject_id: toInteger($patient_id)})
        OPTIONAL MATCH (ed)-[:ISSUED_PRESCRIPTIONS]->(pb1:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(presc1:Prescription)
        OPTIONAL MATCH (ha)-[:ISSUED_PRESCRIPTIONS]->(pb2:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(presc2:Prescription)
        OPTIONAL MATCH (ua)-[:ISSUED_PRESCRIPTIONS]->(pb3:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(presc3:Prescription)
        OPTIONAL MATCH (icu)-[:ISSUED_PRESCRIPTIONS]->(pb4:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(presc4:Prescription)
        RETURN collect(DISTINCT presc1.medicines) + 
               collect(DISTINCT presc2.medicines) + 
               collect(DISTINCT presc3.medicines) + 
               collect(DISTINCT presc4.medicines) AS prescriptions
        """
        result = self.connection.execute_query(query_prescriptions, {'patient_id': patient_id})
        text_data['prescriptions'] = [x for x in (result[0]['prescriptions'] if result else []) if x]
        
        # Previous and Administered Medications
        query_prev_meds = """
        MATCH (ed:EmergencyDepartment {subject_id: toInteger($patient_id)})-[:RECORDED_PREVIOUS_MEDICATIONS]->(prev:PreviousPrescriptionMeds)
        RETURN collect(DISTINCT prev.medications) AS previous_meds
        """
        result = self.connection.execute_query(query_prev_meds, {'patient_id': patient_id})
        text_data['previous_meds'] = result[0]['previous_meds'] if result else []
        
        query_admin_meds = """
        MATCH (ed:EmergencyDepartment {subject_id: toInteger($patient_id)})-[:ADMINISTERED_MEDICATIONS]->(admin:AdministeredMeds)
        RETURN collect(DISTINCT admin.medications) AS administered_meds
        """
        result = self.connection.execute_query(query_admin_meds, {'patient_id': patient_id})
        text_data['administered_meds'] = result[0]['administered_meds'] if result else []
        
        # Procedures
        query_procedures = """
        MATCH (proc:Procedures {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            procedures: proc.procedures,
            source: proc.source
        }) AS procedures
        """
        result = self.connection.execute_query(query_procedures, {'patient_id': patient_id})
        text_data['procedures'] = result[0]['procedures'] if result else []
        
        # Lab Events
        query_labs = """
        MATCH (le:LabEvent {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT {
            lab_results: le.lab_results,
            abnormal_results: le.abnormal_results
        }) AS lab_events
        """
        result = self.connection.execute_query(query_labs, {'patient_id': patient_id})
        text_data['lab_events'] = result[0]['lab_events'] if result else []
        
        # Microbiology Events
        query_micro = """
        MATCH (me:MicrobiologyEvent {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT me.micro_results) AS microbiology_events
        """
        result = self.connection.execute_query(query_micro, {'patient_id': patient_id})
        text_data['microbiology_events'] = result[0]['microbiology_events'] if result else []
        
        # Chart Events
        query_chart = """
        MATCH (ce:ChartEvent {subject_id: toInteger($patient_id)})
        RETURN collect(DISTINCT ce.chart_measurements) AS chart_events
        """
        result = self.connection.execute_query(query_chart, {'patient_id': patient_id})
        text_data['chart_events'] = result[0]['chart_events'] if result else []
        
        # DRG Codes
        query_drg = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:WAS_ASSIGNED_DRG_CODE]->(drg:DRG)
        RETURN collect(DISTINCT {
            drg_type: drg.drg_type,
            drg_code: drg.drg_code,
            description: drg.description,
            drg_severity: drg.drg_severity,
            drg_mortality: drg.drg_mortality
        }) AS drg_codes
        """
        result = self.connection.execute_query(query_drg, {'patient_id': patient_id})
        text_data['drg_codes'] = result[0]['drg_codes'] if result else []
        
        # Past History
        query_past = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:INCLUDED_PAST_HISTORY]->(pph:PatientPastHistory)
        RETURN collect(DISTINCT {
            past_medical_history: pph.past_medical_history,
            social_history: pph.social_history,
            family_history: pph.family_history
        }) AS past_history
        """
        result = self.connection.execute_query(query_past, {'patient_id': patient_id})
        text_data['past_history'] = result[0]['past_history'] if result else []
        
        # HPI Summary
        query_hpi = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:INCLUDED_HPI_SUMMARY]->(hpi:HPISummary)
        RETURN collect(DISTINCT hpi.summary) AS hpi_summaries
        """
        result = self.connection.execute_query(query_hpi, {'patient_id': patient_id})
        text_data['hpi_summaries'] = result[0]['hpi_summaries'] if result else []
        
        # Discharge Clinical Notes
        query_dcn = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})-[:DOCUMENTED_IN_NOTE]->(dcn:DischargeClinicalNote)
        RETURN collect(DISTINCT {
            mental_status: dcn.mental_status,
            level_of_consciousness: dcn.level_of_consciousness,
            activity_status: dcn.activity_status,
            discharge_instructions: dcn.discharge_instructions,
            disposition: dcn.disposition,
            hospital_course: dcn.hospital_course,
            imaging_studies: dcn.imaging_studies,
            major_procedure: dcn.major_procedure,
            microbiology_findings: dcn.microbiology_findings,
            antibiotic_plan: dcn.antibiotic_plan,
            code_status: dcn.code_status,
            facility_name: dcn.facility_name,
            primary_diagnoses: dcn.primary_diagnoses,
            secondary_diagnoses: dcn.secondary_diagnoses
        }) AS discharge_notes
        """
        result = self.connection.execute_query(query_dcn, {'patient_id': patient_id})
        text_data['discharge_notes'] = result[0]['discharge_notes'] if result else []
        
        # Medications (Admission, Started, Stopped, To Avoid)
        query_adm_meds = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:INCLUDED_MEDICATIONS]->(am:AdmissionMedications)
        RETURN collect(DISTINCT am.medications) AS admission_medications
        """
        result = self.connection.execute_query(query_adm_meds, {'patient_id': patient_id})
        text_data['admission_medications'] = result[0]['admission_medications'] if result else []
        
        query_started = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})-[:STARTED_MEDICATIONS]->(ms:MedicationStarted)
        RETURN collect(DISTINCT ms.medications) AS medications_started
        """
        result = self.connection.execute_query(query_started, {'patient_id': patient_id})
        text_data['medications_started'] = result[0]['medications_started'] if result else []
        
        query_stopped = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})-[:STOPPED_MEDICATIONS]->(mst:MedicationStopped)
        RETURN collect(DISTINCT mst.medications) AS medications_stopped
        """
        result = self.connection.execute_query(query_stopped, {'patient_id': patient_id})
        text_data['medications_stopped'] = result[0]['medications_stopped'] if result else []
        
        query_avoid = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})-[:LISTED_MEDICATIONS_TO_AVOID]->(mta:MedicationToAvoid)
        RETURN collect(DISTINCT mta.medications) AS medications_to_avoid
        """
        result = self.connection.execute_query(query_avoid, {'patient_id': patient_id})
        text_data['medications_to_avoid'] = result[0]['medications_to_avoid'] if result else []
        
        # Allergies
        query_allergies = """
        MATCH (d:Discharge {subject_id: toInteger($patient_id)})-[:HAS_ALLERGY]->(ai:AllergyIdentified)
        RETURN collect(DISTINCT ai.allergy_name) AS allergies
        """
        result = self.connection.execute_query(query_allergies, {'patient_id': patient_id})
        text_data['allergies'] = result[0]['allergies'] if result else []
        
        # Admission Labs
        query_adm_labs = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:INCLUDED_LAB_RESULTS]->(al:AdmissionLabs)
        RETURN collect(DISTINCT al.lab_tests) AS admission_labs
        """
        result = self.connection.execute_query(query_adm_labs, {'patient_id': patient_id})
        text_data['admission_labs'] = result[0]['admission_labs'] if result else []
        
        # Admission Vitals (dynamic properties)
        query_adm_vitals = """
        MATCH (ha:HospitalAdmission {subject_id: toInteger($patient_id)})-[:RECORDED_VITALS]->(av:AdmissionVitals)
        RETURN collect(DISTINCT properties(av)) AS admission_vitals
        """
        result = self.connection.execute_query(query_adm_vitals, {'patient_id': patient_id})
        text_data['admission_vitals'] = result[0]['admission_vitals'] if result else []
        
        # Discharge Labs
        query_disch_labs = """
        MATCH (dcn:DischargeClinicalNote)-[:RECORDED_LAB_RESULTS]->(dl:DischargeLabs)
        WHERE dcn.subject_id = toInteger($patient_id)
        RETURN collect(DISTINCT dl.lab_tests) AS discharge_labs
        """
        result = self.connection.execute_query(query_disch_labs, {'patient_id': patient_id})
        text_data['discharge_labs'] = result[0]['discharge_labs'] if result else []
        
        # Discharge Medications
        query_disch_meds = """
        MATCH (dcn:DischargeClinicalNote)-[:RECORDED_MEDICATIONS]->(dm:DischargeMedications)
        WHERE dcn.subject_id = toInteger($patient_id)
        RETURN collect(DISTINCT dm.medications) AS discharge_medications
        """
        result = self.connection.execute_query(query_disch_meds, {'patient_id': patient_id})
        text_data['discharge_medications'] = result[0]['discharge_medications'] if result else []
        
        # Discharge Vitals (dynamic properties)
        query_disch_vitals = """
        MATCH (dcn:DischargeClinicalNote)-[:RECORDED_VITALS]->(dv:DischargeVitals)
        WHERE dcn.subject_id = toInteger($patient_id)
        RETURN collect(DISTINCT properties(dv)) AS discharge_vitals
        """
        result = self.connection.execute_query(query_disch_vitals, {'patient_id': patient_id})
        text_data['discharge_vitals'] = result[0]['discharge_vitals'] if result else []
        
        return text_data
    
    def format_text_for_embedding(self, text_data: Dict[str, Any]) -> str:
        """
        Format extracted text data into a single string for embedding
        Complete text is preserved - no truncation
        
        Args:
            text_data: Dictionary of extracted text data
            
        Returns:
            Formatted text string (complete text, no truncation)
        """
        # Helper function to format arrays (no truncation)
        def format_array(arr, prefix=""):
            if not arr:
                return ""
            items = []
            for item in arr:
                if isinstance(item, dict):
                    # Format dictionary items, including dynamic properties
                    item_str = ", ".join([f"{k}: {v}" for k, v in item.items() if v is not None and k not in ['subject_id', 'hadm_id', 'event_id']])
                    if item_str:
                        items.append(item_str)
                elif isinstance(item, list):
                    items.extend([str(x) for x in item if x])
                else:
                    items.append(str(item))
            
            return prefix + "; ".join(items) if items else ""
        
        # Build text parts - all categories included, no truncation
        category_parts = {}
        
        # Patient attributes (always include)
        patient_attrs = text_data.get('patient_attributes', {})
        if patient_attrs:
            parts = []
            if patient_attrs.get('gender'):
                parts.append(f"Gender: {patient_attrs['gender']}")
            if patient_attrs.get('age'):
                parts.append(f"Age: {patient_attrs['age']}")
            if patient_attrs.get('year_group'):
                parts.append(f"Year Group: {patient_attrs['year_group']}")
            if patient_attrs.get('dod'):
                parts.append(f"Date of Death: {patient_attrs['dod']}")
            if patient_attrs.get('total_admissions'):
                parts.append(f"Total Admissions: {patient_attrs['total_admissions']}")
            if parts:
                category_parts['patient_attributes'] = "Patient: " + ", ".join(parts)
        
        # Build all category parts (complete text, no truncation)
        category_parts['diagnoses'] = format_array(text_data.get('diagnoses'), "Diagnoses: ") if text_data.get('diagnoses') else ""
        category_parts['allergies'] = format_array(text_data.get('allergies'), "Allergies: ") if text_data.get('allergies') else ""
        category_parts['discharge_notes'] = format_array(text_data.get('discharge_notes'), "Discharge Notes: ") if text_data.get('discharge_notes') else ""
        category_parts['medications_started'] = format_array(text_data.get('medications_started'), "Medications Started: ") if text_data.get('medications_started') else ""
        category_parts['medications_stopped'] = format_array(text_data.get('medications_stopped'), "Medications Stopped: ") if text_data.get('medications_stopped') else ""
        category_parts['medications_to_avoid'] = format_array(text_data.get('medications_to_avoid'), "Medications to Avoid: ") if text_data.get('medications_to_avoid') else ""
        category_parts['admission_medications'] = format_array(text_data.get('admission_medications'), "Admission Medications: ") if text_data.get('admission_medications') else ""
        category_parts['discharge_medications'] = format_array(text_data.get('discharge_medications'), "Discharge Medications: ") if text_data.get('discharge_medications') else ""
        category_parts['prescriptions'] = format_array(text_data.get('prescriptions'), "Prescriptions: ") if text_data.get('prescriptions') else ""
        category_parts['hpi_summaries'] = format_array(text_data.get('hpi_summaries'), "HPI Summaries: ") if text_data.get('hpi_summaries') else ""
        category_parts['procedures'] = format_array(text_data.get('procedures'), "Procedures: ") if text_data.get('procedures') else ""
        category_parts['admissions'] = format_array(text_data.get('admissions'), "Admissions: ") if text_data.get('admissions') else ""
        category_parts['discharges'] = format_array(text_data.get('discharges'), "Discharges: ") if text_data.get('discharges') else ""
        category_parts['admission_labs'] = format_array(text_data.get('admission_labs'), "Admission Labs: ") if text_data.get('admission_labs') else ""
        category_parts['discharge_labs'] = format_array(text_data.get('discharge_labs'), "Discharge Labs: ") if text_data.get('discharge_labs') else ""
        category_parts['admission_vitals'] = format_array(text_data.get('admission_vitals'), "Admission Vitals: ") if text_data.get('admission_vitals') else ""
        category_parts['discharge_vitals'] = format_array(text_data.get('discharge_vitals'), "Discharge Vitals: ") if text_data.get('discharge_vitals') else ""
        category_parts['assessments'] = format_array(text_data.get('assessments'), "Assessments: ") if text_data.get('assessments') else ""
        category_parts['lab_events'] = format_array(text_data.get('lab_events'), "Lab Events: ") if text_data.get('lab_events') else ""
        category_parts['microbiology_events'] = format_array(text_data.get('microbiology_events'), "Microbiology Events: ") if text_data.get('microbiology_events') else ""
        category_parts['previous_meds'] = format_array(text_data.get('previous_meds'), "Previous Medications: ") if text_data.get('previous_meds') else ""
        category_parts['administered_meds'] = format_array(text_data.get('administered_meds'), "Administered Medications: ") if text_data.get('administered_meds') else ""
        category_parts['past_history'] = format_array(text_data.get('past_history'), "Past History: ") if text_data.get('past_history') else ""
        category_parts['ed_visits'] = format_array(text_data.get('ed_visits'), "ED Visits: ") if text_data.get('ed_visits') else ""
        category_parts['unit_admissions'] = format_array(text_data.get('unit_admissions'), "Unit Admissions: ") if text_data.get('unit_admissions') else ""
        category_parts['icu_stays'] = format_array(text_data.get('icu_stays'), "ICU Stays: ") if text_data.get('icu_stays') else ""
        category_parts['chart_events'] = format_array(text_data.get('chart_events'), "Chart Events: ") if text_data.get('chart_events') else ""
        category_parts['drg_codes'] = format_array(text_data.get('drg_codes'), "DRG Codes: ") if text_data.get('drg_codes') else ""
        
        # Remove empty categories
        category_parts = {k: v for k, v in category_parts.items() if v}
        
        # Always include patient_id to ensure unique embeddings
        patient_id = text_data.get('patient_id', '')
        patient_id_prefix = f"Patient ID: {patient_id}" if patient_id else ""
        
        # Join all parts, with patient_id first
        text_parts = [text for _, text in category_parts.items()]
        if patient_id_prefix:
            text_parts.insert(0, patient_id_prefix)
        full_text = " | ".join(text_parts)
        
        # If text is still empty, add a fallback
        if not full_text or not full_text.strip():
            full_text = f"Patient ID: {patient_id}" if patient_id else "Empty patient data"
        
        # Remove stop words before returning (preserves structure and important terms)
        full_text = remove_stop_words(full_text)
        
        logger.debug(f"Full text length: {len(full_text)} characters (after stop word removal)")
        
        return full_text
    
    def batch_extract_patient_text_data(self, patient_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Extract text data for multiple patients
        
        Args:
            patient_ids: List of patient identifiers
            
        Returns:
            Dictionary mapping patient_id to text data
        """
        results = {}
        for patient_id in patient_ids:
            try:
                text_data = self.extract_patient_text_data(patient_id)
                if text_data:
                    results[patient_id] = text_data
            except Exception as e:
                logger.error(f"Error extracting text data for patient {patient_id}: {e}")
                continue
        
        return results

