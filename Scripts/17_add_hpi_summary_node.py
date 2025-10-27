# add_hpi_summary_node.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

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

def add_hpi_summary_nodes():
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
        
        # Filter to records with hadm_id and hpi_summary
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            created_count = 0
            skipped_count = 0
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                hpi_summary = str(row['hpi_summary']) if pd.notna(row['hpi_summary']) else None
                
                if not hpi_summary:
                    logger.info(f"No HPI summary found for hadm_id {hadm_id}")
                    skipped_count += 1
                    continue
                
                logger.info(f"Processing HPI summary for hadm_id {hadm_id}")
                
                # Create HPISummary node and link to HospitalAdmission
                query = """
                MATCH (ha:HospitalAdmission {hadm_id: $hadm_id})
                MERGE (hpi:HPISummary {hadm_id: $hadm_id})
                ON CREATE SET 
                    hpi.name = 'HPISummary',
                    hpi.note_id = $note_id,
                    hpi.hadm_id = $hadm_id,
                    hpi.summary = $hpi_summary
                ON MATCH SET
                    hpi.name = 'HPISummary',
                    hpi.note_id = $note_id,
                    hpi.summary = $hpi_summary
                MERGE (ha)-[:INCLUDED_HPI_SUMMARY]->(hpi)
                RETURN ha IS NOT NULL as admission_exists, 
                       hpi.hadm_id as created_id
                """
                
                result = session.run(query, 
                                   hadm_id=hadm_id, 
                                   note_id=note_id,
                                   hpi_summary=hpi_summary)
                record = result.single()
                
                if record and record['admission_exists']:
                    created_count += 1
                    logger.info(f"  Created/Updated HPISummary for hadm_id {hadm_id}")
                else:
                    logger.warning(f"  No HospitalAdmission node found for hadm_id {hadm_id}")
                    skipped_count += 1
        
        logger.info(f"\nSummary:")
        logger.info(f"  HPI Summary nodes created/updated: {created_count}")
        logger.info(f"  Records skipped (no HPI summary or no admission): {skipped_count}")
        logger.info("HPISummary nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    add_hpi_summary_nodes()

