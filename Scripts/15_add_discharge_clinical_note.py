# add_discharge_clinical_note.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def add_discharge_clinical_note_nodes():
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
        
        # Filter to records with hadm_id (required for linking)
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            created_count = 0
            skipped_count = 0
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                
                if not note_id:
                    logger.warning(f"Skipping record with hadm_id {hadm_id} - missing note_id")
                    skipped_count += 1
                    continue
                
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
                
                # Create DischargeClinicalNote node and link to Discharge
                query = """
                MATCH (d:Discharge {hadm_id: $hadm_id})
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
                
                result = session.run(query, **properties)
                record = result.single()
                
                if record and record['discharge_exists']:
                    created_count += 1
                    logger.info(f"Created/Updated DischargeClinicalNote {note_id} for hadm_id {hadm_id}")
                else:
                    logger.warning(f"No Discharge node found for hadm_id {hadm_id}")
                    skipped_count += 1
        
        logger.info(f"\nSummary:")
        logger.info(f"  Created/Updated: {created_count}")
        logger.info(f"  Skipped (no discharge node or missing data): {skipped_count}")
        logger.info("DischargeClinicalNote nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    add_discharge_clinical_note_nodes()

