# add_past_history.py
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

def add_past_history_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_past_history.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '12_add_past_history'

    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'

    # Check if file exists
    if not CLINICAL_NOTES_CSV.exists():
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping past history creation.")
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
        
        # Filter to records with hadm_id
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with neo4j_conn.session() as session:
            # Check for existing PatientPastHistory nodes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            admissions_with_history = set()
            
            # Get admissions that already have PatientPastHistory nodes
            query_existing = """
            MATCH (pph:PatientPastHistory)
            RETURN DISTINCT pph.hadm_id AS hadm_id
            """
            result = session.run(query_existing)
            admissions_with_history = {int(record["hadm_id"]) for record in result if record["hadm_id"] is not None}
            logger.info(f"Found {len(admissions_with_history)} admissions with existing PatientPastHistory nodes")
            
            created_count = 0
            updated_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(clinical_notes_df), desc="Adding past history nodes", unit="record")
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_history:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if admission already has past history (incremental load)
                if hadm_id in admissions_with_history:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    # Prepare properties, handling NaN values
                    properties = {
                        'hadm_id': hadm_id,
                        'past_medical_history': str(row['past_medical_history']) if pd.notna(row['past_medical_history']) else None,
                        'social_history': str(row['social_history']) if pd.notna(row['social_history']) else None,
                        'family_history': str(row['family_history']) if pd.notna(row['family_history']) else None
                    }
                    
                    # Check if all history fields are empty/None
                    if all(v is None for k, v in properties.items() if k != 'hadm_id'):
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    # Create PatientPastHistory node and link to HospitalAdmission - verify subject_id to prevent cross-patient assignments
                    if subject_id is None:
                        logger.warning(f"Skipping past history for hadm_id {hadm_id} - missing subject_id")
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    query = """
                    MATCH (ha:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
                    MERGE (pph:PatientPastHistory {hadm_id: $hadm_id})
                    ON CREATE SET 
                        pph.name = 'PatientPastHistory',
                        pph.past_medical_history = $past_medical_history,
                        pph.social_history = $social_history,
                        pph.family_history = $family_history
                    ON MATCH SET
                        pph.name = 'PatientPastHistory',
                        pph.past_medical_history = $past_medical_history,
                        pph.social_history = $social_history,
                        pph.family_history = $family_history
                    MERGE (ha)-[:INCLUDED_PAST_HISTORY]->(pph)
                    RETURN ha IS NOT NULL as admission_exists, 
                           pph.hadm_id as created_id,
                           CASE WHEN ha IS NULL THEN false ELSE true END as created
                    """
                    
                    properties['subject_id'] = subject_id
                    result = session.run(query, **properties)
                    record = result.single()
                    
                    if record and record['admission_exists']:
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
                        logger.warning(f"No HospitalAdmission found for hadm_id {hadm_id}")
                        skipped_count += 1
                except Exception as e:
                    logger.error(f"Error processing hadm_id {hadm_id} for patient {subject_id}: {e}")
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
        logger.info(f"  Skipped (no admission, empty data, or already exists): {skipped_count}")
        if skipped_count > 0 and len(admissions_with_history) > 0:
            logger.info(f"Incremental load summary: Processed {created_count} PatientPastHistory nodes, skipped {skipped_count} admissions (including {len(admissions_with_history)} with existing history)")
        logger.info("PatientPastHistory nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    add_past_history_nodes()

