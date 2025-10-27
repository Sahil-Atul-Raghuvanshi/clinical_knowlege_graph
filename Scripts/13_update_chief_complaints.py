# update_chief_complaints.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_folder_name():
    """Read folder name from foldername.txt"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        foldername_path = os.path.join(script_dir, 'foldername.txt')
        with open(foldername_path, 'r') as f:
            folder_name = f.read().strip()
        logger.info(f"Using folder name: {folder_name}")
        return folder_name
    except Exception as e:
        logger.error(f"Error reading folder name: {e}")
        raise

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

def update_chief_complaints():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File path
    CLINICAL_NOTES_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\discharge_clinical_note_flattened.csv"

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
            updated_count = 0
            skipped_count = 0
            no_ed_count = 0
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                chief_complaint_from_notes = str(row['chief_complaint'])
                
                # Find EmergencyDepartment node with this hadm_id and get InitialAssessment
                query_get = """
                MATCH (ed:EmergencyDepartment {hadm_id: $hadm_id})
                OPTIONAL MATCH (ed)-[:HAS_INITIAL_ASSESSMENT]->(ia:InitialAssessment)
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
                    logger.info(f"Skipping duplicate complaint for stay_id {stay_id}: '{chief_complaint_from_notes}' already in '{current_complaint}'")
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
                MATCH (ed)-[:HAS_INITIAL_ASSESSMENT]->(ia:InitialAssessment)
                SET ia.chiefcomplaint = $updated_complaint
                """
                
                session.run(query_update, 
                           stay_id=stay_id, 
                           updated_complaint=updated_complaint)
                
                logger.info(f"Updated InitialAssessment for stay_id {stay_id}: '{current_complaint}' -> '{updated_complaint}'")
                updated_count += 1
        
        logger.info(f"\nSummary:")
        logger.info(f"  Updated: {updated_count}")
        logger.info(f"  Skipped (duplicate): {skipped_count}")
        logger.info(f"  No ED/Assessment found: {no_ed_count}")
        logger.info("Chief complaint update completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    update_chief_complaints()

