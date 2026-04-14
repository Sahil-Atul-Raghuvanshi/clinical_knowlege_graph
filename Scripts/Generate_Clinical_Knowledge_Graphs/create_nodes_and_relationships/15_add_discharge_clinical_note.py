# add_discharge_clinical_note.py
import pandas as pd
import logging
import os
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

def add_discharge_clinical_note_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_discharge_clinical_note.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '15_add_discharge_clinical_note'

    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'

    # Check if file exists
    if not CLINICAL_NOTES_CSV.exists():
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping discharge clinical note creation.")
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

        with neo4j_conn.session() as session:
            # Check for existing DischargeClinicalNote nodes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
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
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(clinical_notes_df), desc="Adding discharge clinical note nodes", unit="record")
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                
                if not note_id:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if note_id in notes_with_discharge_note:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if note already has DischargeClinicalNote (incremental load)
                if note_id in notes_with_discharge_note:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
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
                    
                    # Create DischargeClinicalNote node and link to Discharge - verify subject_id to prevent cross-patient assignments
                    if subject_id is None:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    query = """
                    MATCH (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
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
                    
                    properties['subject_id'] = subject_id
                    result = session.run(query, **properties)
                    record = result.single()
                    
                    if record and record['discharge_exists']:
                        created_count += 1
                        
                        # Mark patient as processed immediately after successful processing
                        if subject_id is not None:
                            if tracker:
                                try:
                                    tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                    processed_patients.add(subject_id)
                                except Exception as e:
                                    logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                    else:
                        logger.warning(f"No Discharge node found for hadm_id {hadm_id}")
                        skipped_count += 1
                except Exception as e:
                    logger.error(f"Error processing note_id {note_id} for patient {subject_id}: {e}")
                    # Mark patient as failed immediately if we have subject_id
                    if subject_id is not None and subject_id not in failed_patients:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                                failed_patients.append(subject_id)
                            except Exception as tracker_error:
                                logger.error(f"Error marking patient {subject_id} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and processed_patients:
                logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
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
        neo4j_conn.close()


if __name__ == "__main__":
    add_discharge_clinical_note_nodes()

