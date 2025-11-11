# update_chief_complaints.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os
import re
from typing import Optional
from incremental_load_utils import IncrementalLoadChecker
from etl_tracker import ETLTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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

def update_chief_complaints(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '13_update_chief_complaints'

    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')

    # Check if file exists
    if not os.path.exists(CLINICAL_NOTES_CSV):
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping chief complaints update.")
        return

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with both hadm_id and chief_complaint
        clinical_notes_df = clinical_notes_df[
            clinical_notes_df['hadm_id'].notna() & 
            clinical_notes_df['chief_complaint'].notna()
        ]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id and chief_complaint")

        with driver.session() as session:
            # Check for already updated chief complaints (incremental load support)
            # This script updates existing InitialAssessment nodes, so we check if the complaint
            # from notes is already in the assessment's chiefcomplaint field
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            
            updated_count = 0
            skipped_count = 0
            no_ed_count = 0
            already_updated_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                chief_complaint_from_notes = str(row['chief_complaint'])
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still process to check if update is needed, but mark as skipped for tracking
                
                # Find EmergencyDepartment node with this hadm_id and get InitialAssessment
                query_get = """
                MATCH (ed:EmergencyDepartment {hadm_id: $hadm_id})
                OPTIONAL MATCH (ed)-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia:InitialAssessment)
                RETURN ed.event_id as stay_id, 
                       ia.chiefcomplaint as current_complaint,
                       ia IS NOT NULL as has_assessment
                """
                
                result = session.run(query_get, hadm_id=hadm_id)
                record = result.single()
                
                if not record:
                    logger.warning(f"No EmergencyDepartment found for hadm_id {hadm_id}")
                    no_ed_count += 1
                    continue
                
                if not record['has_assessment']:
                    logger.warning(f"No InitialAssessment found for EmergencyDepartment stay_id {record['stay_id']}")
                    no_ed_count += 1
                    continue
                
                stay_id = record['stay_id']
                current_complaint = record['current_complaint']
                
                # Check if we should add the new complaint
                if is_semantically_duplicate(current_complaint, chief_complaint_from_notes):
                    already_updated_count += 1
                    if already_updated_count == 1 or already_updated_count % 100 == 0:
                        logger.info(f"Skipping duplicate complaint for stay_id {stay_id}: '{chief_complaint_from_notes}' already in '{current_complaint}' (incremental load). Total skipped: {already_updated_count}")
                    skipped_count += 1
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
                
                logger.info(f"Updated InitialAssessment for stay_id {stay_id}: '{current_complaint}' -> '{updated_complaint}'")
                updated_count += 1
                if subject_id is not None:
                    processed_patients.add(subject_id)
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
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
        driver.close()


if __name__ == "__main__":
    update_chief_complaints()

