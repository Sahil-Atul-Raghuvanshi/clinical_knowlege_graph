# add_hospitalization_data.py
import pandas as pd
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# Add Scripts directory to path for imports
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent.parent
sys.path.insert(0, str(scripts_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection
from utils.incremental_load_utils import IncrementalLoadChecker
from utils.etl_tracker import ETLTracker

# Configure logging - write only to file, not console (to keep progress bar clean)
project_root = script_dir.parent.parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

# Configure logger to only use file handler (no console output)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prevent propagation to root logger (which would print to console)
logger.propagate = False


def parse_vitals(vitals_string):
    """
    Parse vitals string into a dictionary of key-value pairs.
    Example: "Temperature: 100.8, Heart Rate: 106, Blood Pressure: 96/61"
    Returns: {"Temperature": "100.8", "Heart_Rate": "106", "Blood_Pressure": "96/61"}
    """
    if pd.isna(vitals_string) or not vitals_string.strip():
        return {}
    
    vitals_dict = {}
    # Split by comma to get individual vital measurements
    parts = vitals_string.split(',')
    
    for part in parts:
        part = part.strip()
        if ':' in part:
            key, value = part.split(':', 1)
            key = key.strip().replace(' ', '_').replace('/', '_')
            value = value.strip()
            vitals_dict[key] = value
    
    return vitals_dict

def parse_labs(labs_string):
    """
    Parse labs string into a list of lab test strings.
    Example: "WBC: 17.1, Hemoglobin: 13.6, Creatinine: 0.3"
    Returns: ["WBC: 17.1", "Hemoglobin: 13.6", "Creatinine: 0.3"]
    """
    if pd.isna(labs_string) or not labs_string.strip():
        return []
    
    # Split by comma and clean up each lab test
    labs_list = [lab.strip() for lab in labs_string.split(',') if lab.strip()]
    return labs_list

def parse_medications(medications_string):
    """
    Parse medications string into a list of medications.
    Example: "Aspirin 81 mg daily, Atorvastatin 80 mg QPM, Metoprolol Tartrate 6.25 mg Q6H"
    Returns: ["Aspirin 81 mg daily", "Atorvastatin 80 mg QPM", "Metoprolol Tartrate 6.25 mg Q6H"]
    """
    if pd.isna(medications_string) or not medications_string.strip():
        return []
    
    # Split by comma and clean up each medication
    medications = [med.strip() for med in medications_string.split(',') if med.strip()]
    return medications

def create_hospital_admission_node(session, row):
    """Create or update HospitalAdmission node with clinical note data"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    
    properties = {
        'subject_id': subject_id,
        'hadm_id': hadm_id,
        'note_id': str(row['note_id']) if pd.notna(row['note_id']) else None,
        'service': str(row['service']) if pd.notna(row['service']) else None,
        'chief_complaint': str(row['chief_complaint']) if pd.notna(row['chief_complaint']) else None,
        'social_history': str(row['social_history']) if pd.notna(row['social_history']) else None,
        'family_history': str(row['family_history']) if pd.notna(row['family_history']) else None
    }
    
    query = """
    MERGE (ha:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
    ON CREATE SET 
        ha.name = 'HospitalAdmission',
        ha.subject_id = $subject_id,
        ha.note_id = $note_id,
        ha.service = $service,
        ha.chief_complaint = $chief_complaint,
        ha.social_history = $social_history,
        ha.family_history = $family_history
    ON MATCH SET
        ha.subject_id = $subject_id,
        ha.note_id = $note_id,
        ha.service = $service,
        ha.chief_complaint = $chief_complaint,
        ha.social_history = $social_history,
        ha.family_history = $family_history
    RETURN ha IS NOT NULL as admission_exists
    """
    
    result = session.run(query, **properties)
    record = result.single()
    return record and record['admission_exists']

def create_admission_vitals_node(session, row):
    """Create AdmissionVitals node and link to HospitalAdmission - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    vitals_string = row['admission_vitals']
    
    vitals_dict = parse_vitals(vitals_string)
    
    if not vitals_dict:
        return False
    
    # Create unique ID for vitals node
    vitals_id = f"{hadm_id}_admission_vitals"
    
    query = """
    MATCH (ha:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH ha
    MERGE (av:AdmissionVitals {vitals_id: $vitals_id})
    SET av.name = 'AdmissionVitals',
        av.hadm_id = $hadm_id
    """
    
    # Dynamically add all vitals properties
    for key, value in vitals_dict.items():
        query += f", av.{key} = ${key}"
    
    query += """
    MERGE (ha)-[:RECORDED_VITALS]->(av)
    RETURN ha IS NOT NULL as admission_exists
    """
    
    params = {'hadm_id': hadm_id, 'subject_id': subject_id, 'vitals_id': vitals_id}
    params.update(vitals_dict)
    
    result = session.run(query, **params)
    record = result.single()
    return record and record['admission_exists']

def create_admission_labs_node(session, row):
    """Create AdmissionLabs node and link to HospitalAdmission - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    labs_string = row['admission_labs']
    
    labs_list = parse_labs(labs_string)
    
    if not labs_list:
        return False
    
    # Create unique ID for labs node
    labs_id = f"{hadm_id}_admission_labs"
    
    query = """
    MATCH (ha:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH ha
    MERGE (al:AdmissionLabs {labs_id: $labs_id})
    SET al.name = 'AdmissionLabs',
        al.hadm_id = $hadm_id,
        al.lab_tests = $lab_tests
    MERGE (ha)-[:INCLUDED_LAB_RESULTS]->(al)
    RETURN ha IS NOT NULL as admission_exists
    """
    
    result = session.run(query, hadm_id=hadm_id, subject_id=subject_id, labs_id=labs_id, lab_tests=labs_list)
    record = result.single()
    return record and record['admission_exists']

def create_admission_medications_node(session, row):
    """Create AdmissionMedications node and link to HospitalAdmission - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    medications_string = row['admission_medications']
    
    medications_list = parse_medications(medications_string)
    
    if not medications_list:
        return False
    
    # Create unique ID for medications node
    med_id = f"{hadm_id}_admission_medications"
    
    query = """
    MATCH (ha:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH ha
    MERGE (am:AdmissionMedications {medications_id: $med_id})
    SET am.name = 'AdmissionMedications',
        am.hadm_id = $hadm_id,
        am.medications = $medications
    MERGE (ha)-[:INCLUDED_MEDICATIONS]->(am)
    RETURN ha IS NOT NULL as admission_exists
    """
    
    result = session.run(query, hadm_id=hadm_id, subject_id=subject_id, med_id=med_id, medications=medications_list)
    record = result.single()
    return record and record['admission_exists']

def create_discharge_clinical_note_node(session, row):
    """Create DischargeClinicalNote node and link to Discharge (creates Discharge if missing)"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
    
    if not note_id:
        return False
    
    properties = {
        'subject_id': subject_id,
        'hadm_id': hadm_id,
        'note_id': note_id,
        'mental_status': str(row['mental_status']) if pd.notna(row['mental_status']) else None,
        'level_of_consciousness': str(row['level_of_consciousness']) if pd.notna(row['level_of_consciousness']) else None,
        'activity_status': str(row['activity_status']) if pd.notna(row['activity_status']) else None,
        'discharge_instructions': str(row['discharge_instructions']) if pd.notna(row['discharge_instructions']) else None,
        'disposition': str(row['disposition']) if pd.notna(row['disposition']) else None,
        'imaging_count': int(row['imaging_count']) if pd.notna(row['imaging_count']) else None,
        'imaging_studies': str(row['imaging_studies']) if pd.notna(row['imaging_studies']) else None,
        'microbiology_findings': str(row['microbiology_findings']) if pd.notna(row['microbiology_findings']) else None,
        'antibiotic_plan': str(row['antibiotic_plan']) if pd.notna(row['antibiotic_plan']) else None,
        'code_status': str(row['code_status']) if pd.notna(row['code_status']) else None,
        'facility_name': str(row['facility_name']) if pd.notna(row['facility_name']) else None,
        'primary_diagnoses': str(row['primary_diagnoses']) if pd.notna(row['primary_diagnoses']) else None,
        'secondary_diagnoses': str(row['secondary_diagnoses']) if pd.notna(row['secondary_diagnoses']) else None
    }
    
    query = """
    MERGE (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
    ON CREATE SET 
        d.name = 'Discharge',
        d.subject_id = $subject_id
    ON MATCH SET
        d.subject_id = $subject_id
    WITH d
    MERGE (dcn:DischargeClinicalNote {note_id: $note_id})
    ON CREATE SET 
        dcn.name = 'DischargeClinicalNote',
        dcn.subject_id = $subject_id,
        dcn.hadm_id = $hadm_id,
        dcn.mental_status = $mental_status,
        dcn.level_of_consciousness = $level_of_consciousness,
        dcn.activity_status = $activity_status,
        dcn.discharge_instructions = $discharge_instructions,
        dcn.disposition = $disposition,
        dcn.imaging_count = $imaging_count,
        dcn.imaging_studies = $imaging_studies,
        dcn.microbiology_findings = $microbiology_findings,
        dcn.antibiotic_plan = $antibiotic_plan,
        dcn.code_status = $code_status,
        dcn.facility_name = $facility_name,
        dcn.primary_diagnoses = $primary_diagnoses,
        dcn.secondary_diagnoses = $secondary_diagnoses
    ON MATCH SET
        dcn.name = 'DischargeClinicalNote',
        dcn.subject_id = $subject_id,
        dcn.hadm_id = $hadm_id,
        dcn.mental_status = $mental_status,
        dcn.level_of_consciousness = $level_of_consciousness,
        dcn.activity_status = $activity_status,
        dcn.discharge_instructions = $discharge_instructions,
        dcn.disposition = $disposition,
        dcn.imaging_count = $imaging_count,
        dcn.imaging_studies = $imaging_studies,
        dcn.microbiology_findings = $microbiology_findings,
        dcn.antibiotic_plan = $antibiotic_plan,
        dcn.code_status = $code_status,
        dcn.facility_name = $facility_name,
        dcn.primary_diagnoses = $primary_diagnoses,
        dcn.secondary_diagnoses = $secondary_diagnoses
    MERGE (d)-[:DOCUMENTED_IN_NOTE]->(dcn)
    RETURN d IS NOT NULL as discharge_exists
    """
    
    result = session.run(query, **properties)
    record = result.single()
    return record and record['discharge_exists']

def create_discharge_vitals_node(session, row):
    """Create DischargeVitals node and link to DischargeClinicalNote"""
    note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
    vitals_string = row['discharge_vitals']
    
    if not note_id:
        return False
    
    vitals_dict = parse_vitals(vitals_string)
    
    if not vitals_dict:
        return False
    
    # Create unique ID for vitals node
    vitals_id = f"{note_id}_discharge_vitals"
    
    query = """
    MATCH (dcn:DischargeClinicalNote {note_id: $note_id})
    MERGE (dv:DischargeVitals {vitals_id: $vitals_id})
    SET dv.name = 'DischargeVitals',
        dv.note_id = $note_id
    """
    
    # Dynamically add all vitals properties
    for key, value in vitals_dict.items():
        query += f", dv.{key} = ${key}"
    
    query += """
    MERGE (dcn)-[:RECORDED_VITALS]->(dv)
    RETURN dcn IS NOT NULL as note_exists
    """
    
    params = {'note_id': note_id, 'vitals_id': vitals_id}
    params.update(vitals_dict)
    
    result = session.run(query, **params)
    record = result.single()
    return record and record['note_exists']

def create_discharge_labs_node(session, row):
    """Create DischargeLabs node with array of lab tests and link to DischargeClinicalNote"""
    note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
    labs_string = row['discharge_labs']
    
    if not note_id:
        return False
    
    labs_list = parse_labs(labs_string)
    
    if not labs_list:
        return False
    
    # Create unique ID for labs node
    labs_id = f"{note_id}_discharge_labs"
    
    query = """
    MATCH (dcn:DischargeClinicalNote {note_id: $note_id})
    MERGE (dl:DischargeLabs {labs_id: $labs_id})
    SET dl.name = 'DischargeLabs',
        dl.note_id = $note_id,
        dl.lab_tests = $lab_tests
    MERGE (dcn)-[:RECORDED_LAB_RESULTS]->(dl)
    RETURN dcn IS NOT NULL as note_exists
    """
    
    result = session.run(query, note_id=note_id, labs_id=labs_id, lab_tests=labs_list)
    record = result.single()
    return record and record['note_exists']

def create_discharge_medications_node(session, row):
    """Create DischargeMedications node with array of medications and link to DischargeClinicalNote"""
    note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
    medications_string = row['discharge_medications']
    
    if not note_id:
        return False
    
    medications_list = parse_medications(medications_string)
    
    if not medications_list:
        return False
    
    # Create unique ID for medications node
    med_id = f"{note_id}_discharge_medications"
    
    query = """
    MATCH (dcn:DischargeClinicalNote {note_id: $note_id})
    MERGE (dm:DischargeMedications {medications_id: $med_id})
    SET dm.name = 'DischargeMedications',
        dm.note_id = $note_id,
        dm.medications = $medications
    MERGE (dcn)-[:RECORDED_MEDICATIONS]->(dm)
    RETURN dcn IS NOT NULL as note_exists
    """
    
    result = session.run(query, note_id=note_id, med_id=med_id, medications=medications_list)
    record = result.single()
    return record and record['note_exists']

def create_discharge_node(session, row):
    """Create Discharge node and link to Patient - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    
    properties = {
        'subject_id': subject_id,
        'hadm_id': hadm_id,
        'note_id': str(row['note_id']) if pd.notna(row['note_id']) else None,
        'disposition': str(row['disposition']) if pd.notna(row['disposition']) else None,
        'facility_name': str(row['facility_name']) if pd.notna(row['facility_name']) else None,
        'allergies': str(row['allergies']) if pd.notna(row['allergies']) else None,
        'major_procedure': str(row['major_procedure']) if pd.notna(row['major_procedure']) else None
    }
    
    query = """
    MERGE (p:Patient {subject_id: $subject_id})
    ON CREATE SET p.name = 'Patient'
    WITH p
    MERGE (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
    ON CREATE SET 
        d.name = 'Discharge',
        d.subject_id = $subject_id,
        d.note_id = $note_id,
        d.disposition = $disposition,
        d.facility_name = $facility_name,
        d.allergies = $allergies,
        d.major_procedure = $major_procedure
    ON MATCH SET
        d.name = 'Discharge',
        d.subject_id = $subject_id,
        d.note_id = $note_id,
        d.disposition = $disposition,
        d.facility_name = $facility_name,
        d.allergies = $allergies,
        d.major_procedure = $major_procedure
    RETURN d IS NOT NULL as discharge_exists
    """
    
    result = session.run(query, **properties)
    record = result.single()
    return record and record['discharge_exists']

def create_medications_started_node(session, row):
    """Create MedicationStarted node and link to Discharge - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    medications_string = row['medications_started']
    
    medications_list = parse_medications(medications_string)
    
    if not medications_list:
        return False
    
    # Create unique ID for medications node
    med_id = f"{hadm_id}_medications_started"
    
    query = """
    MATCH (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH d
    MERGE (ms:MedicationStarted {medications_id: $med_id})
    SET ms.name = 'MedicationStarted',
        ms.hadm_id = $hadm_id,
        ms.medications = $medications
    MERGE (d)-[:STARTED_MEDICATIONS]->(ms)
    RETURN d IS NOT NULL as discharge_exists
    """
    
    result = session.run(query, hadm_id=hadm_id, subject_id=subject_id, med_id=med_id, medications=medications_list)
    record = result.single()
    return record and record['discharge_exists']

def create_medications_stopped_node(session, row):
    """Create MedicationStopped node and link to Discharge - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    medications_string = row['medications_stopped']
    
    medications_list = parse_medications(medications_string)
    
    if not medications_list:
        return False
    
    # Create unique ID for medications node
    med_id = f"{hadm_id}_medications_stopped"
    
    query = """
    MATCH (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH d
    MERGE (ms:MedicationStopped {medications_id: $med_id})
    SET ms.name = 'MedicationStopped',
        ms.hadm_id = $hadm_id,
        ms.medications = $medications
    MERGE (d)-[:STOPPED_MEDICATIONS]->(ms)
    RETURN d IS NOT NULL as discharge_exists
    """
    
    result = session.run(query, hadm_id=hadm_id, subject_id=subject_id, med_id=med_id, medications=medications_list)
    record = result.single()
    return record and record['discharge_exists']

def create_medications_to_avoid_node(session, row):
    """Create MedicationToAvoid node and link to Discharge - verify subject_id to prevent cross-patient assignments"""
    subject_id = int(row['subject_id'])
    hadm_id = int(row['hadm_id'])
    medications_string = row['medications_to_avoid']
    
    medications_list = parse_medications(medications_string)
    
    if not medications_list:
        return False
    
    # Create unique ID for medications node
    med_id = f"{hadm_id}_medications_to_avoid"
    
    query = """
    MATCH (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
    WITH d
    MERGE (ma:MedicationToAvoid {medications_id: $med_id})
    SET ma.name = 'MedicationToAvoid',
        ma.hadm_id = $hadm_id,
        ma.medications = $medications
    MERGE (d)-[:LISTED_MEDICATIONS_TO_AVOID]->(ma)
    RETURN d IS NOT NULL as discharge_exists
    """
    
    result = session.run(query, hadm_id=hadm_id, subject_id=subject_id, med_id=med_id, medications=medications_list)
    record = result.single()
    return record and record['discharge_exists']

def add_hospitalization_data(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_hospitalization_data.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    """Main function to add all hospitalization data to Neo4j"""
    # Load configuration
    config = Config()
    SCRIPT_NAME = '18_add_hospitalization_data'

    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'

    # Check if file exists
    if not CLINICAL_NOTES_CSV.exists():
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping hospitalization data creation.")
        return

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(str(CLINICAL_NOTES_CSV))
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with hadm_id (required for linking)
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        # Initialize counters
        stats = {
            'hospital_admission': 0,
            'admission_vitals': 0,
            'admission_labs': 0,
            'admission_medications': 0,
            'discharge_clinical_note': 0,
            'discharge_vitals': 0,
            'discharge_labs': 0,
            'discharge_medications': 0,
            'discharge': 0,
            'medications_started': 0,
            'medications_stopped': 0,
            'medications_to_avoid': 0,
            'skipped': 0
        }

        with neo4j_conn.session() as session:
            # Check for existing DischargeClinicalNote nodes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            notes_with_data = set()
            
            # Get note_ids that already have DischargeClinicalNote nodes (indicating hospitalization data exists)
            query_existing = """
            MATCH (dcn:DischargeClinicalNote)
            RETURN DISTINCT dcn.note_id AS note_id
            """
            result = session.run(query_existing)
            notes_with_data = {str(record["note_id"]) for record in result if record["note_id"] is not None}
            logger.info(f"Found {len(notes_with_data)} notes with existing hospitalization data")
            
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            # Use set to track which patients we've already marked in tracker (avoid duplicate tracking)
            patients_tracked_this_run = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(clinical_notes_df), desc="Adding hospitalization data", unit="record")
            for idx, row in clinical_notes_df.iterrows():
                note_id = str(row['note_id']) if pd.notna(row.get('note_id')) else None
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if note_id and note_id in notes_with_data:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': len(patients_tracked_this_run), 'Skipped': skipped_count})
                        continue
                
                # Skip if note already has hospitalization data (incremental load)
                if note_id and note_id in notes_with_data:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': len(patients_tracked_this_run), 'Skipped': skipped_count})
                    continue
                
                record_processed = False
                try:
                    # 1. Create HospitalAdmission node
                    if create_hospital_admission_node(session, row):
                        stats['hospital_admission'] += 1
                        record_processed = True
                        
                        # 2. Create AdmissionVitals node
                        if create_admission_vitals_node(session, row):
                            stats['admission_vitals'] += 1
                        
                        # 3. Create AdmissionLabs node
                        if create_admission_labs_node(session, row):
                            stats['admission_labs'] += 1
                        
                        # 4. Create AdmissionMedications node
                        if create_admission_medications_node(session, row):
                            stats['admission_medications'] += 1
                    
                    # 5. Create DischargeClinicalNote node
                    if create_discharge_clinical_note_node(session, row):
                        stats['discharge_clinical_note'] += 1
                        record_processed = True
                        
                        # 6. Create DischargeVitals node
                        if create_discharge_vitals_node(session, row):
                            stats['discharge_vitals'] += 1
                        
                        # 7. Create DischargeLabs node
                        if create_discharge_labs_node(session, row):
                            stats['discharge_labs'] += 1
                        
                        # 8. Create DischargeMedications node
                        if create_discharge_medications_node(session, row):
                            stats['discharge_medications'] += 1
                    
                    # 9. Create Discharge node
                    if create_discharge_node(session, row):
                        stats['discharge'] += 1
                        record_processed = True
                        
                        # 10. Create MedicationStarted node
                        if create_medications_started_node(session, row):
                            stats['medications_started'] += 1
                        
                        # 11. Create MedicationStopped node
                        if create_medications_stopped_node(session, row):
                            stats['medications_stopped'] += 1
                        
                        # 12. Create MedicationToAvoid node
                        if create_medications_to_avoid_node(session, row):
                            stats['medications_to_avoid'] += 1
                    
                    # Mark patient as processed immediately after successful processing (only once per patient per run)
                    if subject_id is not None and record_processed and subject_id not in patients_tracked_this_run:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                patients_tracked_this_run.add(subject_id)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                    
                except Exception as e:
                    logger.error(f"Error processing record {idx + 1}: {e}")
                    stats['skipped'] += 1
                    # Mark patient as failed immediately if we have subject_id
                    if subject_id is not None and subject_id not in failed_patients:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                                failed_patients.append(subject_id)
                            except Exception as tracker_error:
                                logger.error(f"Error marking patient {subject_id} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': len(patients_tracked_this_run), 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and patients_tracked_this_run:
                logger.info(f"Successfully processed and tracked {len(patients_tracked_this_run)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("SUMMARY")
        logger.info("=" * 80)
        logger.info(f"HospitalAdmission nodes created/updated: {stats['hospital_admission']}")
        logger.info(f"  - AdmissionVitals: {stats['admission_vitals']}")
        logger.info(f"  - AdmissionLabs: {stats['admission_labs']}")
        logger.info(f"  - AdmissionMedications: {stats['admission_medications']}")
        logger.info(f"DischargeClinicalNote nodes created/updated: {stats['discharge_clinical_note']}")
        logger.info(f"  - DischargeVitals: {stats['discharge_vitals']}")
        logger.info(f"  - DischargeLabs: {stats['discharge_labs']}")
        logger.info(f"  - DischargeMedications: {stats['discharge_medications']}")
        logger.info(f"Discharge nodes created/updated: {stats['discharge']}")
        logger.info(f"  - MedicationStarted: {stats['medications_started']}")
        logger.info(f"  - MedicationStopped: {stats['medications_stopped']}")
        logger.info(f"  - MedicationToAvoid: {stats['medications_to_avoid']}")
        logger.info(f"Records skipped (errors): {stats['skipped']}")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Skipped {skipped_count} notes with existing hospitalization data")
        logger.info("=" * 80)
        logger.info("✓ Hospitalization data added successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    add_hospitalization_data()

