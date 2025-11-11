# add_assessment_nodes.py
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

def create_initial_assessment_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '11_add_assessment_nodes'

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    TRIAGE_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'triage.csv')

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load triage data
        triage_df = pd.read_csv(TRIAGE_CSV)
        
        logger.info(f"Loaded {len(triage_df)} triage records")

        with driver.session() as session:
            # Check for existing InitialAssessment nodes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            stays_with_assessment = set()
            
            # Get stay_ids that already have InitialAssessment nodes
            query_existing = """
            MATCH (ia:InitialAssessment)
            RETURN DISTINCT ia.stay_id AS stay_id
            """
            result = session.run(query_existing)
            stays_with_assessment = {str(record["stay_id"]) for record in result if record["stay_id"] is not None}
            logger.info(f"Found {len(stays_with_assessment)} ED stays with existing InitialAssessment nodes")
            
            created_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()
            
            # Iterate over each triage record
            for _, row in triage_df.iterrows():
                stay_id = str(row['stay_id']) if pd.notna(row['stay_id']) else None
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                
                # Skip if stay_id is missing
                if not stay_id:
                    continue
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if stay_id in stays_with_assessment:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping stay_id {stay_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if already has InitialAssessment (incremental load)
                if stay_id in stays_with_assessment:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping stay_id {stay_id} - already has InitialAssessment (incremental load). Total skipped: {skipped_count}")
                    continue
                
                # Prepare properties, handling NaN values
                properties = {
                    'stay_id': str(stay_id),
                    'temperature': float(row['temperature']) if pd.notna(row['temperature']) else None,
                    'heartrate': float(row['heartrate']) if pd.notna(row['heartrate']) else None,
                    'resprate': float(row['resprate']) if pd.notna(row['resprate']) else None,
                    'o2sat': float(row['o2sat']) if pd.notna(row['o2sat']) else None,
                    'sbp': float(row['sbp']) if pd.notna(row['sbp']) else None,
                    'dbp': float(row['dbp']) if pd.notna(row['dbp']) else None,
                    'pain': str(row['pain']) if pd.notna(row['pain']) else None,
                    'acuity': float(row['acuity']) if pd.notna(row['acuity']) else None,
                    'chiefcomplaint': str(row['chiefcomplaint']) if pd.notna(row['chiefcomplaint']) else None
                }
                
                # Create InitialAssessment node and link to EmergencyDepartment
                query = """
                MATCH (ed:EmergencyDepartment {event_id: $stay_id})
                MERGE (ia:InitialAssessment {stay_id: $stay_id})
                ON CREATE SET 
                    ia.name = 'InitialAssessment',
                    ia.temperature = $temperature,
                    ia.heartrate = $heartrate,
                    ia.resprate = $resprate,
                    ia.o2sat = $o2sat,
                    ia.sbp = $sbp,
                    ia.dbp = $dbp,
                    ia.pain = $pain,
                    ia.acuity = $acuity,
                    ia.chiefcomplaint = $chiefcomplaint
                ON MATCH SET
                    ia.name = 'InitialAssessment',
                    ia.temperature = $temperature,
                    ia.heartrate = $heartrate,
                    ia.resprate = $resprate,
                    ia.o2sat = $o2sat,
                    ia.sbp = $sbp,
                    ia.dbp = $dbp,
                    ia.pain = $pain,
                    ia.acuity = $acuity
                MERGE (ed)-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia)
                """
                
                session.run(query, **properties)
                
                created_count += 1
                if subject_id is not None:
                    processed_patients.add(subject_id)
                logger.info(f"Created InitialAssessment for ED stay {stay_id}")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {created_count} InitialAssessment nodes, skipped {skipped_count} ED stays with existing assessments")
                
        logger.info("All initial assessment nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    create_initial_assessment_nodes()

