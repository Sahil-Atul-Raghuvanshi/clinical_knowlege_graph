# add_past_history.py
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

def add_past_history_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '12_add_past_history'

    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')

    # Check if file exists
    if not os.path.exists(CLINICAL_NOTES_CSV):
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping past history creation.")
        return

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with hadm_id
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            # Check for existing PatientPastHistory nodes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
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
            skipped_patients = set()
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_history:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping admission {hadm_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if admission already has past history (incremental load)
                if hadm_id in admissions_with_history:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping admission {hadm_id} - already has PatientPastHistory (incremental load). Total skipped: {skipped_count}")
                    continue
                
                # Prepare properties, handling NaN values
                properties = {
                    'hadm_id': hadm_id,
                    'past_medical_history': str(row['past_medical_history']) if pd.notna(row['past_medical_history']) else None,
                    'social_history': str(row['social_history']) if pd.notna(row['social_history']) else None,
                    'family_history': str(row['family_history']) if pd.notna(row['family_history']) else None
                }
                
                # Check if all history fields are empty/None
                if all(v is None for k, v in properties.items() if k != 'hadm_id'):
                    logger.info(f"Skipping hadm_id {hadm_id} - all history fields are empty")
                    skipped_count += 1
                    continue
                
                # Create PatientPastHistory node and link to HospitalAdmission
                query = """
                MATCH (ha:HospitalAdmission {hadm_id: $hadm_id})
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
                
                result = session.run(query, **properties)
                record = result.single()
                
                if record and record['admission_exists']:
                    created_count += 1
                    if subject_id is not None:
                        processed_patients.add(subject_id)
                    logger.info(f"Created/Updated PatientPastHistory for hadm_id {hadm_id}")
                else:
                    logger.warning(f"No HospitalAdmission found for hadm_id {hadm_id}")
                    skipped_count += 1
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
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
        driver.close()


if __name__ == "__main__":
    add_past_history_nodes()

