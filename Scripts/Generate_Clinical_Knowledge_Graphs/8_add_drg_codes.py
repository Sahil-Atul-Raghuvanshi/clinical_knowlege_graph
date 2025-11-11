# add_drg_codes.py
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

def add_drg_codes(tracker: Optional[ETLTracker] = None):
    """Add DRG codes to the knowledge graph"""
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '8_add_drg_codes'
    
    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    DRGCODES_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'drgcodes.csv')
    
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Load data
        drg_df = pd.read_csv(DRGCODES_CSV)
        logger.info(f"Loaded {len(drg_df)} DRG code records")
        
        with driver.session() as session:
            # Check for existing DRG codes (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
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
            skipped_patients = set()
            
            for _, row in drg_df.iterrows():
                subject_id = int(row["subject_id"])
                hadm_id = row["hadm_id"]
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_drg:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping admission {hadm_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if admission already has DRG codes (incremental load)
                if hadm_id in admissions_with_drg:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping admission {hadm_id} - already has DRG codes (incremental load). Total skipped: {skipped_count}")
                    continue
                
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
                
                # Link DRG to HospitalAdmission
                query_link = """
                MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                MATCH (drg:DRG {drg_id: $drg_id})
                MERGE (h)-[r:WAS_ASSIGNED_DRG_CODE]->(drg)
                ON CREATE SET r.subject_id = $subject_id
                """
                session.run(query_link,
                           hadm_id=hadm_id,
                           drg_id=drg_id,
                           subject_id=subject_id)
                link_count += 1
                processed_patients.add(subject_id)
                
                logger.info(f"Processed DRG {drg_id} ({drg_type}) for admission {hadm_id} - {description[:50]}...")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
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
        driver.close()

if __name__ == "__main__":
    add_drg_codes()

