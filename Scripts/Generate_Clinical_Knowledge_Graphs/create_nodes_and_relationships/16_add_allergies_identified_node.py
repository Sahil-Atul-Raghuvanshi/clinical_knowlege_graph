# add_allergies_identified_node.py
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


def parse_allergies(allergy_string):
    """Parse allergy string to extract individual allergies"""
    if pd.isna(allergy_string) or not allergy_string:
        return []
    
    allergy_string = str(allergy_string).strip()
    
    # Check for common "no allergy" indicators
    no_allergy_patterns = [
        'none', 'no known allergies', 'nkda', 'no known drug allergies',
        'n/a', 'na', 'nil', 'no allergies'
    ]
    
    if allergy_string.lower() in no_allergy_patterns:
        return []
    
    # Split by common delimiters: comma, semicolon, pipe, or "and"
    allergies = re.split(r'[,;|]|\sand\s', allergy_string)
    
    # Clean and filter allergies
    allergies = [a.strip() for a in allergies if a.strip()]
    
    # Remove duplicates while preserving order
    seen = set()
    unique_allergies = []
    for allergy in allergies:
        allergy_lower = allergy.lower()
        if allergy_lower not in seen and allergy_lower not in no_allergy_patterns:
            seen.add(allergy_lower)
            unique_allergies.append(allergy)
    
    return unique_allergies

def add_allergy_identified_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_allergies_identified_node.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '16_add_allergies_identified_node'

    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'

    # Check if file exists
    if not CLINICAL_NOTES_CSV.exists():
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping allergy identification.")
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
            # Check for existing AllergyIdentified nodes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            admissions_with_allergies = set()
            
            # Get admissions that already have AllergyIdentified nodes
            query_existing = """
            MATCH (d:Discharge)-[:HAS_ALLERGY]->(ai:AllergyIdentified)
            RETURN DISTINCT d.hadm_id AS hadm_id
            """
            result = session.run(query_existing)
            admissions_with_allergies = {int(record["hadm_id"]) for record in result if record["hadm_id"] is not None}
            logger.info(f"Found {len(admissions_with_allergies)} admissions with existing AllergyIdentified nodes")
            
            created_count = 0
            skipped_count = 0
            total_allergies = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(clinical_notes_df), desc="Adding allergy identified nodes", unit="record")
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_allergies:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if admission already has allergies (incremental load)
                if hadm_id in admissions_with_allergies:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    # Parse allergies from the allergies column
                    allergies = parse_allergies(row.get('allergies'))
                    
                    if not allergies:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    # Process each allergy - verify subject_id to prevent cross-patient assignments
                    if subject_id is None:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                    
                    for allergy_name in allergies:
                        # Create AllergyIdentified node and link to Discharge
                        query = """
                        MATCH (d:Discharge {hadm_id: $hadm_id, subject_id: $subject_id})
                        MERGE (ai:AllergyIdentified {allergy_name: $allergy_name, hadm_id: $hadm_id})
                        ON CREATE SET 
                            ai.name = 'AllergyIdentified',
                            ai.note_id = $note_id,
                            ai.allergy_name = $allergy_name,
                            ai.hadm_id = $hadm_id
                        ON MATCH SET
                            ai.name = 'AllergyIdentified',
                            ai.note_id = $note_id
                        MERGE (d)-[:HAS_ALLERGY]->(ai)
                        RETURN d IS NOT NULL as discharge_exists, 
                               ai.allergy_name as created_allergy
                        """
                        
                        result = session.run(query, 
                                           hadm_id=hadm_id, 
                                           subject_id=subject_id,
                                           note_id=note_id,
                                           allergy_name=allergy_name)
                        record = result.single()
                        
                        if record and record['discharge_exists']:
                            created_count += 1
                            total_allergies += 1
                        else:
                            logger.warning(f"No Discharge node found for hadm_id {hadm_id}")
                    
                    # Mark patient as processed immediately after processing all allergies for this patient
                    if subject_id is not None and subject_id not in processed_patients:
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
        logger.info(f"  Total allergy nodes created/updated: {created_count}")
        logger.info(f"  Records skipped (no allergies or already exists): {skipped_count}")
        logger.info(f"  Total unique allergies processed: {total_allergies}")
        if skipped_count > 0 and len(admissions_with_allergies) > 0:
            logger.info(f"Incremental load summary: Processed {created_count} AllergyIdentified nodes, skipped {skipped_count} admissions (including {len(admissions_with_allergies)} with existing allergies)")
        logger.info("AllergyIdentified nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    add_allergy_identified_nodes()

