# add_past_history.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def add_past_history_nodes():
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File path
    CLINICAL_NOTES_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\note\discharge_clinical_note_flattened.csv"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with hadm_id
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            created_count = 0
            updated_count = 0
            skipped_count = 0
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                
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
                    logger.info(f"Created/Updated PatientPastHistory for hadm_id {hadm_id}")
                else:
                    logger.warning(f"No HospitalAdmission found for hadm_id {hadm_id}")
                    skipped_count += 1
        
        logger.info(f"\nSummary:")
        logger.info(f"  Created/Updated: {created_count}")
        logger.info(f"  Skipped (no admission or empty data): {skipped_count}")
        logger.info("PatientPastHistory nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    add_past_history_nodes()

