import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_provider_nodes():
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    
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
            relationship_count = 0
            provider_count = 0
            
            for _, row in admissions_df.iterrows():
                hadm_id = row['hadm_id']
                subject_id = row['subject_id']
                admit_provider_id = row['admit_provider_id']
                
                # Only process if admit_provider_id is not null/empty
                if pd.notna(admit_provider_id) and str(admit_provider_id).strip():
                    provider_id_clean = str(admit_provider_id).strip()
                    
                    # Create patient-specific provider node (using both provider_id and subject_id as composite key)
                    # This ensures each patient's knowledge graph remains isolated
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
                                       hadm_id=int(hadm_id))
                    
                    created_provider = result.single()["created_provider"]
                    relationship_count += 1
                    logger.info(f"Created/merged Provider {created_provider} (Patient {subject_id}) -> HospitalAdmission {hadm_id}")
        
        logger.info(f"Created {relationship_count} provider-admission relationships successfully!")
        
        # Optional: Create summary statistics
        with driver.session() as session:
            # Count total provider nodes (patient-specific)
            result = session.run("MATCH (p:Provider) RETURN count(p) as provider_count")
            provider_count = result.single()["provider_count"]
            
            # Count total hospital admissions
            result = session.run("MATCH (h:HospitalAdmission) RETURN count(h) as admission_count")
            admission_count = result.single()["admission_count"]
            
            # Count provider-admission relationships
            result = session.run("MATCH (p:Provider)-[:MANAGED_ADMISSION]->(h:HospitalAdmission) RETURN count(*) as relationship_count")
            rel_count = result.single()["relationship_count"]
            
            # Count unique patients with providers
            result = session.run("MATCH (p:Provider) RETURN count(DISTINCT p.subject_id) as patient_count")
            patient_count = result.single()["patient_count"]
            
            logger.info(f"Summary: {provider_count} patient-scoped provider nodes across {patient_count} patients")
            logger.info(f"         {admission_count} admissions, {rel_count} provider-admission relationships")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    add_provider_nodes()
