import pandas as pd
from neo4j import GraphDatabase
import logging
import os
from typing import Optional
from incremental_load_utils import IncrementalLoadChecker
from etl_tracker import ETLTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_provider_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '10_add_provider_nodes'
    
    # Path to data files (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    ADMISSIONS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'admissions.csv')
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            logger.info(f"Connection successful!")
        
        # Load admissions data to get provider-admission relationships
        admissions_df = pd.read_csv(ADMISSIONS_CSV)
        logger.info(f"Found {len(admissions_df)} admission records")
        
        # Get unique provider IDs that are actually linked to admissions
        linked_providers = admissions_df['admit_provider_id'].dropna()
        linked_providers = linked_providers[linked_providers.astype(str).str.strip() != '']
        unique_linked_providers = linked_providers.unique()
        logger.info(f"Found {len(unique_linked_providers)} unique provider IDs linked to admissions")
        logger.info(f"Note: Provider nodes will be scoped per patient to maintain isolated knowledge graphs")
        
        # Create provider nodes and relationships in one go
        # IMPORTANT: Provider nodes are scoped per patient to keep knowledge graphs isolated
        with driver.session() as session:
            # Check for existing provider relationships (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
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
            skipped_patients = set()
            
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
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping admission {hadm_id} (patient {subject_id} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if admission already has provider (incremental load)
                if hadm_id in admissions_with_providers:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping admission {hadm_id} - already has provider (incremental load). Total skipped: {skipped_count}")
                    continue
                
                # Only process if admit_provider_id is not null/empty
                if pd.notna(admit_provider_id) and str(admit_provider_id).strip():
                    provider_id_clean = str(admit_provider_id).strip()
                    
                    # Create patient-specific provider node (using both provider_id and subject_id as composite key)
                    # This ensures each patient's knowledge graph remains isolated
                    # First check if HospitalAdmission exists
                    check_query = """
                    MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                    RETURN h.hadm_id as hadm_id
                    """
                    check_result = session.run(check_query, hadm_id=hadm_id)
                    if check_result.single() is None:
                        logger.warning(f"HospitalAdmission {hadm_id} not found. Skipping provider relationship creation.")
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
                    MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
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
                        continue
                    
                    created_provider = result_record["created_provider"]
                    relationship_count += 1
                    processed_patients.add(subject_id)
                    logger.info(f"Created/merged Provider {created_provider} (Patient {subject_id}) -> HospitalAdmission {hadm_id}")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
        
        logger.info(f"Created {relationship_count} provider-admission relationships successfully!")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {relationship_count} provider relationships, skipped {skipped_count} admissions with existing providers")
        
        # Optional: Create summary statistics
        with driver.session() as session:
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
        driver.close()

if __name__ == "__main__":
    add_provider_nodes()
