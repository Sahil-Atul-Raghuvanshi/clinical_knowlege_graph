# add_drg_codes.py
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

def add_drg_codes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_drg_codes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    """Add DRG codes to the knowledge graph"""
    # Load configuration
    config = Config()
    SCRIPT_NAME = '8_add_drg_codes'
    
    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    DRGCODES_CSV = project_root / 'Filtered_Data' / 'hosp' / 'drgcodes.csv'
    
    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()
    
    try:
        # Load data
        drg_df = pd.read_csv(str(DRGCODES_CSV))
        logger.info(f"Loaded {len(drg_df)} DRG code records")
        
        with neo4j_conn.session() as session:
            # Check for existing DRG codes (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            admissions_with_drg = set()
            
            # Get admissions that already have DRG codes
            query_existing = """
            MATCH (h:HospitalAdmission)-[:WAS_ASSIGNED_DRG_CODE]->(drg:DRG)
            RETURN DISTINCT h.hadm_id AS hadm_id
            """
            result = session.run(query_existing)
            admissions_with_drg = {int(record["hadm_id"]) for record in result if record["hadm_id"] is not None}
            logger.info(f"Found {len(admissions_with_drg)} admissions with existing DRG codes")
            
            drg_count = 0
            link_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(drg_df), desc="Adding DRG codes", unit="record")
            for _, row in drg_df.iterrows():
                subject_id = int(row["subject_id"])
                hadm_id = row["hadm_id"]
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_drg:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': drg_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if admission already has DRG codes (incremental load)
                if hadm_id in admissions_with_drg:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': drg_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    drg_type = row["drg_type"]
                    drg_code = str(row["drg_code"])
                    description = row["description"]
                    drg_severity = float(row["drg_severity"]) if pd.notna(row.get("drg_severity")) else None
                    drg_mortality = float(row["drg_mortality"]) if pd.notna(row.get("drg_mortality")) else None
                    
                    # Create unique identifier for DRG node per admission (hadm_id + type + code)
                    # This ensures each admission has its own DRG node, keeping knowledge graphs independent
                    drg_id = f"{hadm_id}_{drg_type}_{drg_code}"
                    
                    # Create or update DRG node
                    # Name should be HCFA_DRG or APR_DRG based on drg_type
                    drg_name = f"{drg_type}_DRG"
                    
                    query_drg = """
                    MERGE (drg:DRG {drg_id: $drg_id})
                    ON CREATE SET 
                        drg.name = $drg_name,
                        drg.hadm_id = $hadm_id,
                        drg.drg_type = $drg_type,
                        drg.drg_code = $drg_code,
                        drg.description = $description,
                        drg.drg_severity = $drg_severity,
                        drg.drg_mortality = $drg_mortality
                    ON MATCH SET
                        drg.name = $drg_name,
                        drg.hadm_id = $hadm_id,
                        drg.description = $description,
                        drg.drg_severity = $drg_severity,
                        drg.drg_mortality = $drg_mortality
                    """
                    session.run(query_drg,
                               drg_id=drg_id,
                               drg_name=drg_name,
                               hadm_id=hadm_id,
                               drg_type=drg_type,
                               drg_code=drg_code,
                               description=description,
                               drg_severity=drg_severity,
                               drg_mortality=drg_mortality)
                    drg_count += 1
                    
                    # Link DRG to HospitalAdmission - verify subject_id to prevent cross-patient assignments
                    query_link = """
                    MATCH (h:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
                    MATCH (drg:DRG {drg_id: $drg_id})
                    MERGE (h)-[r:WAS_ASSIGNED_DRG_CODE]->(drg)
                    ON CREATE SET r.subject_id = $subject_id
                    """
                    session.run(query_link,
                               hadm_id=hadm_id,
                               drg_id=drg_id,
                               subject_id=subject_id)
                    link_count += 1
                    
                    # Mark patient as processed immediately after successful processing
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                            processed_patients.add(subject_id)
                        except Exception as e:
                            logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing DRG code for patient {subject_id}, hadm_id {hadm_id}: {e}")
                    # Mark patient as failed immediately
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                            failed_patients.append(subject_id)
                        except Exception as tracker_error:
                            logger.error(f"Error marking patient {subject_id} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': drg_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and processed_patients:
                logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            # Log incremental load summary
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {drg_count} DRG codes, skipped {skipped_count} admissions with existing DRG codes")
        
        logger.info(f"Successfully added {drg_count} DRG nodes and created {link_count} links to HospitalAdmission nodes!")
        
    except FileNotFoundError:
        logger.error(f"File not found: {DRGCODES_CSV}")
        logger.error("Please ensure drgcodes.csv exists in the specified folder")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        neo4j_conn.close()

if __name__ == "__main__":
    add_drg_codes()

