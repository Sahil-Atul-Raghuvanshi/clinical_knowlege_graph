# add_hpi_summary_node.py
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

def add_hpi_summary_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '17_add_hpi_summary_node'

    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')

    # Check if file exists
    if not os.path.exists(CLINICAL_NOTES_CSV):
        logger.info(f"Discharge clinical note file not found: {CLINICAL_NOTES_CSV}")
        logger.info("No discharge clinical note available for this patient. Skipping HPI summary creation.")
        return

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        logger.info(f"Loaded {len(clinical_notes_df)} clinical note records")
        
        # Filter to records with hadm_id and hpi_summary
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} records with hadm_id")

        with driver.session() as session:
            # Check for existing HPISummary nodes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            admissions_with_hpi = set()
            
            # Get admissions that already have HPISummary nodes
            query_existing = """
            MATCH (hpi:HPISummary)
            RETURN DISTINCT hpi.hadm_id AS hadm_id
            """
            result = session.run(query_existing)
            admissions_with_hpi = {int(record["hadm_id"]) for record in result if record["hadm_id"] is not None}
            logger.info(f"Found {len(admissions_with_hpi)} admissions with existing HPISummary nodes")
            
            created_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                note_id = str(row['note_id']) if pd.notna(row['note_id']) else None
                hpi_summary = str(row['hpi_summary']) if pd.notna(row['hpi_summary']) else None
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_hpi:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping admission {hadm_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if admission already has HPI summary (incremental load)
                if hadm_id in admissions_with_hpi:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping admission {hadm_id} - already has HPISummary (incremental load). Total skipped: {skipped_count}")
                    continue
                
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
                    if subject_id is not None:
                        processed_patients.add(subject_id)
                    logger.info(f"  Created/Updated HPISummary for hadm_id {hadm_id}")
                else:
                    logger.warning(f"  No HospitalAdmission node found for hadm_id {hadm_id}")
                    skipped_count += 1
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"\nSummary:")
        logger.info(f"  HPI Summary nodes created/updated: {created_count}")
        logger.info(f"  Records skipped (no HPI summary, no admission, or already exists): {skipped_count}")
        if skipped_count > 0 and len(admissions_with_hpi) > 0:
            logger.info(f"Incremental load summary: Processed {created_count} HPISummary nodes, skipped {skipped_count} admissions (including {len(admissions_with_hpi)} with existing HPI summaries)")
        logger.info("HPISummary nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    add_hpi_summary_nodes()

