# add_discharge_clinical_note.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os
from typing import Optional
from incremental_load_utils import IncrementalLoadChecker
from etl_tracker import ETLTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def add_discharge_clinical_note_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '15_add_discharge_clinical_note'

    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')

    # Check if file exists
    if not os.path.exists(CLINICAL_NOTES_CSV):
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping discharge clinical note creation.")
        return

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with hadm_id (required for linking)
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            # Check for existing DischargeClinicalNote nodes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            notes_with_discharge_note = set()
            
            # Get note_ids that already have DischargeClinicalNote nodes
            query_existing = """
            MATCH (dcn:DischargeClinicalNote)
            RETURN DISTINCT dcn.note_id AS note_id
            """
            result = session.run(query_existing)
            notes_with_discharge_note = {str(record["note_id"]) for record in result if record["note_id"] is not None}
            logger.info(f"Found {len(notes_with_discharge_note)} discharge notes with existing DischargeClinicalNote nodes")
            
            created_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                
                if not note_id:
                    logger.warning(f"Skipping record with hadm_id {hadm_id} - missing note_id")
                    skipped_count += 1
                    continue
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if note_id in notes_with_discharge_note:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping note_id {note_id} (hadm_id {hadm_id}, patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if note already has DischargeClinicalNote (incremental load)
                if note_id in notes_with_discharge_note:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping note_id {note_id} (hadm_id {hadm_id}) - already has DischargeClinicalNote (incremental load). Total skipped: {skipped_count}")
                    continue
                
                # Prepare properties, handling NaN values and missing columns
                properties = {
                    'note_id': note_id,
                    'hadm_id': hadm_id,
                    'mental_status': str(row.get('mental_status')) if pd.notna(row.get('mental_status')) else None,
                    'level_of_consciousness': str(row.get('level_of_consciousness')) if pd.notna(row.get('level_of_consciousness')) else None,
                    'activity_status': str(row.get('activity_status')) if pd.notna(row.get('activity_status')) else None,
                    'discharge_instructions': str(row.get('discharge_instructions')) if pd.notna(row.get('discharge_instructions')) else None,
                    'disposition': str(row.get('disposition')) if pd.notna(row.get('disposition')) else None,
                    'hospital_course': str(row.get('hospital_course')) if pd.notna(row.get('hospital_course')) else None,
                    'imaging_count': int(row.get('imaging_count')) if pd.notna(row.get('imaging_count')) else None,
                    'imaging_studies': str(row.get('imaging_studies')) if pd.notna(row.get('imaging_studies')) else None,
                    'major_procedure': str(row.get('major_procedure')) if pd.notna(row.get('major_procedure')) else None,
                    'microbiology_findings': str(row.get('microbiology_findings')) if pd.notna(row.get('microbiology_findings')) else None,
                    'antibiotic_plan': str(row.get('antibiotic_plan')) if pd.notna(row.get('antibiotic_plan')) else None
                }
                
                # Create DischargeClinicalNote node and link to Discharge
                query = """
                MATCH (d:Discharge {hadm_id: $hadm_id})
                MERGE (dcn:DischargeClinicalNote {note_id: $note_id})
                ON CREATE SET 
                    dcn.name = 'DischargeClinicalNote',
                    dcn.hadm_id = $hadm_id,
                    dcn.mental_status = $mental_status,
                    dcn.level_of_consciousness = $level_of_consciousness,
                    dcn.activity_status = $activity_status,
                    dcn.discharge_instructions = $discharge_instructions,
                    dcn.disposition = $disposition,
                    dcn.hospital_course = $hospital_course,
                    dcn.imaging_count = $imaging_count,
                    dcn.imaging_studies = $imaging_studies,
                    dcn.major_procedure = $major_procedure,
                    dcn.microbiology_findings = $microbiology_findings,
                    dcn.antibiotic_plan = $antibiotic_plan
                ON MATCH SET
                    dcn.name = 'DischargeClinicalNote',
                    dcn.hadm_id = $hadm_id,
                    dcn.mental_status = $mental_status,
                    dcn.level_of_consciousness = $level_of_consciousness,
                    dcn.activity_status = $activity_status,
                    dcn.discharge_instructions = $discharge_instructions,
                    dcn.disposition = $disposition,
                    dcn.hospital_course = $hospital_course,
                    dcn.imaging_count = $imaging_count,
                    dcn.imaging_studies = $imaging_studies,
                    dcn.major_procedure = $major_procedure,
                    dcn.microbiology_findings = $microbiology_findings,
                    dcn.antibiotic_plan = $antibiotic_plan
                MERGE (d)-[:DOCUMENTED_IN_NOTE]->(dcn)
                RETURN d IS NOT NULL as discharge_exists, 
                       dcn.note_id as created_id
                """
                
                result = session.run(query, **properties)
                record = result.single()
                
                if record and record['discharge_exists']:
                    created_count += 1
                    if subject_id is not None:
                        processed_patients.add(subject_id)
                    logger.info(f"Created/Updated DischargeClinicalNote {note_id} for hadm_id {hadm_id}")
                else:
                    logger.warning(f"No Discharge node found for hadm_id {hadm_id}")
                    skipped_count += 1
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"\nSummary:")
        logger.info(f"  Created/Updated: {created_count}")
        logger.info(f"  Skipped (no discharge node, missing data, or already exists): {skipped_count}")
        if skipped_count > 0 and len(notes_with_discharge_note) > 0:
            logger.info(f"Incremental load summary: Processed {created_count} DischargeClinicalNote nodes, skipped {skipped_count} notes (including {len(notes_with_discharge_note)} with existing notes)")
        logger.info("DischargeClinicalNote nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    add_discharge_clinical_note_nodes()

