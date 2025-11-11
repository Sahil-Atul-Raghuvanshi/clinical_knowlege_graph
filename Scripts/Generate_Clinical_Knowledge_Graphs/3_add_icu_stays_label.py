# add_icu_stays_label.py
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

def add_icu_stays_label(tracker: Optional[ETLTracker] = None):
    """Add ICUStay label and additional properties to UnitAdmission nodes that are ICU stays"""
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '3_add_icu_stays_label'
    
    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    ICUSTAYS_CSV = os.path.join(project_root, 'Filtered_Data', 'icu', 'icustays.csv')
    
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Load data
        icustays_df = pd.read_csv(ICUSTAYS_CSV)
        logger.info(f"Loaded {len(icustays_df)} ICU stay records")
        
        with driver.session() as session:
            # Check for existing ICUStay labels (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
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
            skipped_patients = set()
            
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
                        if skipped_count == 1 or skipped_count % 50 == 0:
                            logger.info(f"Skipping stay_id {stay_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if already has ICUStay label (incremental load)
                if stay_id in stays_with_icustay_label:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 50 == 0:
                        logger.info(f"Skipping stay_id {stay_id} - already has ICUStay label (incremental load). Total skipped: {skipped_count}")
                    continue
                
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
                    processed_patients.add(subject_id)
                    logger.info(f"Converted to ICUStay: {record['careunit']} (stay_id: {stay_id}, LOS: {los:.2f} days)")
                else:
                    logger.warning(f"Could not find UnitAdmission node with event_id: {stay_id}")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
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
        driver.close()

if __name__ == "__main__":
    add_icu_stays_label()

