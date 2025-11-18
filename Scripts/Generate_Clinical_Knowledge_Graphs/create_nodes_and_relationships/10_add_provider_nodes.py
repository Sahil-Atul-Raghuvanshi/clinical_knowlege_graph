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

def add_provider_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_provider_nodes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '10_add_provider_nodes'
    
    # Path to data files (relative to script location)
    project_root = script_dir.parent.parent.parent
    ADMISSIONS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'admissions.csv'
    
    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()
    
    try:
        # Test connection
        neo4j_conn.execute_query("RETURN 1 as test")
        logger.info(f"Connection successful!")
        
        # Load admissions data to get provider-admission relationships
        admissions_df = pd.read_csv(str(ADMISSIONS_CSV))
        logger.info(f"Found {len(admissions_df)} admission records")
        
        # Get unique provider IDs that are actually linked to admissions
        linked_providers = admissions_df['admit_provider_id'].dropna()
        linked_providers = linked_providers[linked_providers.astype(str).str.strip() != '']
        unique_linked_providers = linked_providers.unique()
        logger.info(f"Found {len(unique_linked_providers)} unique provider IDs linked to admissions")
        logger.info(f"Note: Provider nodes will be scoped per patient to maintain isolated knowledge graphs")
        
        # Create provider nodes and relationships in one go
        # IMPORTANT: Provider nodes are scoped per patient to keep knowledge graphs isolated
        with neo4j_conn.session() as session:
            # Check for existing provider relationships (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            admissions_with_providers = set()
            
            # Get admissions that already have provider relationships
            query_existing = """
            MATCH (p:Provider)-[:MANAGED_ADMISSION]->(h:HospitalAdmission)
            RETURN DISTINCT h.hadm_id AS hadm_id
            """
            result = session.run(query_existing)
            admissions_with_providers = {int(record["hadm_id"]) for record in result if record["hadm_id"] is not None}
            logger.info(f"Found {len(admissions_with_providers)} admissions with existing provider relationships")
            
            relationship_count = 0
            provider_count = 0
            skipped_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            failed_patients = []
            skipped_patients = set()
            
            pbar = tqdm(total=len(admissions_df), desc="Adding provider nodes", unit="admission")
            for _, row in admissions_df.iterrows():
                hadm_id = int(row['hadm_id'])
                subject_id = int(row['subject_id'])
                admit_provider_id = row['admit_provider_id']
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id, SCRIPT_NAME):
                    skipped_patients.add(subject_id)
                    # Still check event-level to avoid duplicate work
                    if hadm_id in admissions_with_providers:
                        skipped_count += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': relationship_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if admission already has provider (incremental load)
                if hadm_id in admissions_with_providers:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': relationship_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    # Only process if admit_provider_id is not null/empty
                    if pd.notna(admit_provider_id) and str(admit_provider_id).strip():
                        provider_id_clean = str(admit_provider_id).strip()
                        
                        # Create patient-specific provider node (using both provider_id and subject_id as composite key)
                        # This ensures each patient's knowledge graph remains isolated
                        # First check if HospitalAdmission exists with matching subject_id
                        check_query = """
                        MATCH (h:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
                        RETURN h.hadm_id as hadm_id
                        """
                        check_result = session.run(check_query, hadm_id=hadm_id, subject_id=subject_id)
                        if check_result.single() is None:
                            logger.warning(f"HospitalAdmission {hadm_id} with subject_id {subject_id} not found. Skipping provider relationship creation.")
                            pbar.update(1)
                            pbar.set_postfix({'Processed': relationship_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                            continue
                        
                        query = """
                        MERGE (p:Provider {provider_id: $provider_id, subject_id: $subject_id})
                        ON CREATE SET 
                            p.name = 'Provider',
                            p.original_provider_id = $provider_id,
                            p.created_at = datetime()
                        ON MATCH SET
                            p.name = 'Provider',
                            p.original_provider_id = $provider_id
                        WITH p
                        MATCH (h:HospitalAdmission {hadm_id: $hadm_id, subject_id: $subject_id})
                        MERGE (p)-[:MANAGED_ADMISSION]->(h)
                        RETURN p.provider_id as created_provider
                        """
                        
                        result = session.run(query, 
                                           provider_id=provider_id_clean,
                                           subject_id=int(subject_id),
                                           hadm_id=hadm_id)
                        
                        result_record = result.single()
                        if result_record is None:
                            logger.warning(f"Failed to create provider relationship for Provider {provider_id_clean} -> HospitalAdmission {hadm_id}")
                            pbar.update(1)
                            pbar.set_postfix({'Processed': relationship_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                            continue
                        
                        created_provider = result_record["created_provider"]
                        relationship_count += 1
                        
                        # Mark patient as processed immediately after successful processing
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                                processed_patients.add(subject_id)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing admission {hadm_id} for patient {subject_id}: {e}")
                    # Mark patient as failed immediately
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                            failed_patients.append(subject_id)
                        except Exception as tracker_error:
                            logger.error(f"Error marking patient {subject_id} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': relationship_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()
            
            # Log summary
            if tracker and processed_patients:
                logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"Created {relationship_count} provider-admission relationships successfully!")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {relationship_count} provider relationships, skipped {skipped_count} admissions with existing providers")
        
        # Optional: Create summary statistics
        with neo4j_conn.session() as session:
            # Count total provider nodes (patient-specific)
            result = session.run("MATCH (p:Provider) RETURN count(p) as provider_count")
            result_record = result.single()
            provider_count = result_record["provider_count"] if result_record else 0
            
            # Count total hospital admissions
            result = session.run("MATCH (h:HospitalAdmission) RETURN count(h) as admission_count")
            result_record = result.single()
            admission_count = result_record["admission_count"] if result_record else 0
            
            # Count provider-admission relationships
            result = session.run("MATCH (p:Provider)-[:MANAGED_ADMISSION]->(h:HospitalAdmission) RETURN count(*) as relationship_count")
            result_record = result.single()
            rel_count = result_record["relationship_count"] if result_record else 0
            
            # Count unique patients with providers
            result = session.run("MATCH (p:Provider) RETURN count(DISTINCT p.subject_id) as patient_count")
            result_record = result.single()
            patient_count = result_record["patient_count"] if result_record else 0
            
            logger.info(f"Summary: {provider_count} patient-scoped provider nodes across {patient_count} patients")
            logger.info(f"         {admission_count} admissions, {rel_count} provider-admission relationships")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        neo4j_conn.close()

if __name__ == "__main__":
    add_provider_nodes()
