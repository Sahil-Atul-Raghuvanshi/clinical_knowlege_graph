# update_chief_complaints.py
import pandas as pd
import logging
import os
import sys
import re
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


def normalize_text(text):
    """Normalize text for semantic comparison"""
    if not text or pd.isna(text):
        return ""
    # Convert to lowercase and remove extra whitespace
    normalized = str(text).lower().strip()
    # Remove extra spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    return normalized

def is_semantically_duplicate(existing_complaint, new_complaint):
    """
    Check if new complaint is semantically already in existing complaint.
    Returns True if duplicate (should not add), False if unique (should add).
    """
    existing_norm = normalize_text(existing_complaint)
    new_norm = normalize_text(new_complaint)
    
    # If either is empty
    if not new_norm:
        return True  # Don't add empty
    if not existing_norm:
        return False  # Add to empty
    
    # Check if new complaint is already contained in existing
    if new_norm in existing_norm:
        return True
    
    # Check if they're the same
    if existing_norm == new_norm:
        return True
    
    # Split by common delimiters and check each part
    existing_parts = [normalize_text(part) for part in re.split(r'[,;]', existing_complaint)]
    
    for part in existing_parts:
        if part and new_norm in part:
            return True
        if part and part in new_norm:
            return True
    
    return False

def update_chief_complaints(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_update_chief_complaints.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '13_update_chief_complaints'

    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'

    # Check if file exists
    if not CLINICAL_NOTES_CSV.exists():
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping chief complaints update.")
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
        
        # Filter to records with both hadm_id and chief_complaint
        clinical_notes_df = clinical_notes_df[
            clinical_notes_df['hadm_id'].notna() & 
            clinical_notes_df['chief_complaint'].notna()
        ]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id and chief_complaint")

        with neo4j_conn.session() as session:
            # Check for already updated chief complaints (incremental load support)
            # This script updates existing InitialAssessment nodes, so we check if the complaint
            # from notes is already in the assessment's chiefcomplaint field
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            
            updated_count = 0
            skipped_count = 0
            no_ed_count = 0
            already_updated_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(clinical_notes_df), desc="Updating chief complaints", unit="record")
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                chief_complaint_from_notes = str(row['chief_complaint'])
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still process to check if update is needed, but mark as skipped for tracking
                
                # Find EmergencyDepartment node with this hadm_id and subject_id, get InitialAssessment
                # Verify subject_id to prevent cross-patient assignments
                if subject_id is None:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Updated': updated_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    query_get = """
                    MATCH (ed:EmergencyDepartment {hadm_id: $hadm_id, subject_id: $subject_id})
                    OPTIONAL MATCH (ed)-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia:InitialAssessment)
                    RETURN ed.event_id as stay_id, 
                           ia.chiefcomplaint as current_complaint,
                           ia IS NOT NULL as has_assessment
                    """
                    
                    result = session.run(query_get, hadm_id=hadm_id, subject_id=subject_id)
                    record = result.single()
                    
                    if not record:
                        no_ed_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Updated': updated_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    if not record['has_assessment']:
                        no_ed_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Updated': updated_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    stay_id = record['stay_id']
                    current_complaint = record['current_complaint']
                    
                    # Check if we should add the new complaint
                    if is_semantically_duplicate(current_complaint, chief_complaint_from_notes):
                        already_updated_count += 1
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Updated': updated_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    # Append the new complaint
                    if current_complaint and str(current_complaint).strip():
                        updated_complaint = f"{current_complaint}, {chief_complaint_from_notes}"
                    else:
                        updated_complaint = chief_complaint_from_notes
                    
                    # Update the InitialAssessment node
                    query_update = """
                    MATCH (ed:EmergencyDepartment {event_id: $stay_id})
                    MATCH (ed)-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia:InitialAssessment)
                    SET ia.chiefcomplaint = $updated_complaint
                    """
                    
                    session.run(query_update, 
                               stay_id=stay_id, 
                               updated_complaint=updated_complaint)
                    
                    updated_count += 1
                    
                    # Mark patient as processed immediately after successful processing
                    if subject_id is not None:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                processed_patients.add(subject_id)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
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
                pbar.set_postfix({'Updated': updated_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and processed_patients:
                logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"\nSummary:")
        logger.info(f"  Updated: {updated_count}")
        logger.info(f"  Skipped (duplicate or already updated): {skipped_count}")
        logger.info(f"  No ED/Assessment found: {no_ed_count}")
        if already_updated_count > 0:
            logger.info(f"Incremental load summary: Processed {updated_count} chief complaints, skipped {already_updated_count} that were already updated")
        logger.info("Chief complaint update completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    update_chief_complaints()

