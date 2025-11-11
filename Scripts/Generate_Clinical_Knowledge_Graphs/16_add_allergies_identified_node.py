# add_allergies_identified_node.py
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

def add_allergy_identified_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '16_add_allergies_identified_node'

    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')

    # Check if file exists
    if not os.path.exists(CLINICAL_NOTES_CSV):
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping allergy identification.")
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
            # Check for existing AllergyIdentified nodes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
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
            skipped_patients = set()
            
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
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping admission {hadm_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if admission already has allergies (incremental load)
                if hadm_id in admissions_with_allergies:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping admission {hadm_id} - already has AllergyIdentified nodes (incremental load). Total skipped: {skipped_count}")
                    continue
                
                # Parse allergies from the allergies column
                allergies = parse_allergies(row.get('allergies'))
                
                if not allergies:
                    logger.info(f"No allergies found for hadm_id {hadm_id}")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing {len(allergies)} allergy(ies) for hadm_id {hadm_id}: {', '.join(allergies)}")
                
                # Process each allergy
                for allergy_name in allergies:
                    # Create AllergyIdentified node and link to Discharge
                    query = """
                    MATCH (d:Discharge {hadm_id: $hadm_id})
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
                                       note_id=note_id,
                                       allergy_name=allergy_name)
                    record = result.single()
                    
                    if record and record['discharge_exists']:
                        created_count += 1
                        total_allergies += 1
                        if subject_id is not None and allergy_name == allergies[0]:  # Only add once per patient
                            processed_patients.add(subject_id)
                        logger.info(f"  Created/Updated AllergyIdentified '{allergy_name}' for hadm_id {hadm_id}")
                    else:
                        logger.warning(f"  No Discharge node found for hadm_id {hadm_id}")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
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
        driver.close()


if __name__ == "__main__":
    add_allergy_identified_nodes()

