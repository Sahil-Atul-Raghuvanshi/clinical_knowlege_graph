# add_icu_stays_label.py
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

def add_icu_stays_label(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_icu_stays_label.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    """Add ICUStay label and additional properties to UnitAdmission nodes that are ICU stays"""
    # Load configuration
    config = Config()
    SCRIPT_NAME = '3_add_icu_stays_label'
    
    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    ICUSTAYS_CSV = project_root / 'Filtered_Data' / 'icu' / 'icustays.csv'
    
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
        icustays_df = pd.read_csv(str(ICUSTAYS_CSV))
        logger.info(f"Loaded {len(icustays_df)} ICU stay records")
        
        with neo4j_conn.session() as session:
            # Check for existing ICUStay labels (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            stays_with_icustay_label = set()
            
            # Get stay_ids that already have ICUStay label
            query_existing = """
            MATCH (icu:ICUStay)
            RETURN DISTINCT icu.event_id AS event_id
            """
            result = session.run(query_existing)
            stays_with_icustay_label = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(stays_with_icustay_label)} ICU stays with existing ICUStay label")
            
            icu_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(icustays_df), desc="Adding ICU stay labels", unit="stay")
            for _, row in icustays_df.iterrows():
                subject_id = int(row["subject_id"])
                hadm_id = row["hadm_id"]
                stay_id = str(int(row["stay_id"]))  # This matches transfer_id in transfers.csv
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if stay_id in stays_with_icustay_label:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': icu_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if already has ICUStay label (incremental load)
                if stay_id in stays_with_icustay_label:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': icu_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    first_careunit = row["first_careunit"]
                    last_careunit = row["last_careunit"]
                    intime = pd.to_datetime(row["intime"]).strftime("%Y-%m-%d %H:%M:%S")
                    outtime = pd.to_datetime(row["outtime"]).strftime("%Y-%m-%d %H:%M:%S")
                    los = float(row["los"])  # Length of stay in days
                    
                    # Find the UnitAdmission node by event_id (which was set from transfer_id)
                    # Remove UnitAdmission label and replace with ICUStay label plus additional properties
                    query = """
                    MATCH (u:UnitAdmission {event_id: $event_id})
                    REMOVE u:UnitAdmission
                    SET u:ICUStay,
                        u.name = 'ICUStay',
                        u.first_careunit = $first_careunit,
                        u.last_careunit = $last_careunit,
                        u.los = $los
                    RETURN u.event_id as event_id, u.careunit as careunit
                    """
                    
                    result = session.run(query,
                                        event_id=stay_id,
                                        first_careunit=first_careunit,
                                        last_careunit=last_careunit,
                                        los=los)
                    
                    record = result.single()
                    if record:
                        icu_count += 1
                        # Mark patient as processed immediately after successful processing
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                processed_patients.add(subject_id)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                    else:
                        logger.warning(f"Could not find UnitAdmission node with event_id: {stay_id}")
                except Exception as e:
                    logger.error(f"Error processing ICU stay {stay_id} for patient {subject_id}: {e}")
                    # Mark patient as failed immediately
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                            failed_patients.append(subject_id)
                        except Exception as tracker_error:
                            logger.error(f"Error marking patient {subject_id} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': icu_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and processed_patients:
                logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"Successfully converted {icu_count} UnitAdmission nodes to ICUStay!")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {icu_count} ICU stays, skipped {skipped_count} ICU stays with existing label")
        logger.info(f"ICU admissions now have exclusive :ICUStay label (UnitAdmission label removed)")
        
    except FileNotFoundError:
        logger.error(f"File not found: {ICUSTAYS_CSV}")
        logger.error("Please ensure icustays.csv exists in the specified folder")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        neo4j_conn.close()

if __name__ == "__main__":
    add_icu_stays_label()

