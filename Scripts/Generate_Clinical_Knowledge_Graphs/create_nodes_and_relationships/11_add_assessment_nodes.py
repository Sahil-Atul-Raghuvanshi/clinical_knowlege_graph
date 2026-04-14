# add_assessment_nodes.py
import pandas as pd
import logging
import os
import sys
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

def create_initial_assessment_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_assessment_nodes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '11_add_assessment_nodes'

    # File paths (relative to script location)
    project_root = script_dir.parent.parent.parent
    TRIAGE_CSV = project_root / 'Filtered_Data' / 'ed' / 'triage.csv'

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    try:
        # Load triage data
        triage_df = pd.read_csv(str(TRIAGE_CSV))
        
        logger.info(f"Loaded {len(triage_df)} triage records")

        with neo4j_conn.session() as session:
            # Check for existing InitialAssessment nodes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
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
            failed_patients = []
            skipped_patients = set()
            
            # Iterate over each triage record
            pbar = tqdm(total=len(triage_df), desc="Adding assessment nodes", unit="record")
            for _, row in triage_df.iterrows():
                stay_id = str(row['stay_id']) if pd.notna(row['stay_id']) else None
                subject_id = int(row['subject_id']) if pd.notna(row.get('subject_id')) else None
                
                # Skip if stay_id is missing
                if not stay_id:
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                # Check per-patient, per-script tracking first (if we have subject_id)
                if subject_id is not None and tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if stay_id in stays_with_assessment:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if already has InitialAssessment (incremental load)
                if stay_id in stays_with_assessment:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': created_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
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
                    
                    # Mark patient as processed immediately after successful processing
                    if subject_id is not None:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                processed_patients.add(subject_id)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing stay {stay_id} for patient {subject_id}: {e}")
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
            
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {created_count} InitialAssessment nodes, skipped {skipped_count} ED stays with existing assessments")
                
        logger.info("All initial assessment nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    create_initial_assessment_nodes()

