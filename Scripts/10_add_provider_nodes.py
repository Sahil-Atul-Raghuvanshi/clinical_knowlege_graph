import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_folder_name():
    """Read folder name from foldername.txt"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        foldername_path = os.path.join(script_dir, 'foldername.txt')
        with open(foldername_path, 'r') as f:
            folder_name = f.read().strip()
        logger.info(f"Using folder name: {folder_name}")
        return folder_name
    except Exception as e:
        logger.error(f"Error reading folder name: {e}")
        raise

def add_provider_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"
    
    # Path to data files - dynamically constructed
    ADMISSIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\admissions.csv"
    
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
        logger.info(f"Found {len(unique_linked_providers)} unique providers linked to admissions")
        
        # Create provider nodes and relationships in one go
        with driver.session() as session:
            relationship_count = 0
            provider_count = 0
            
            for _, row in admissions_df.iterrows():
                hadm_id = row['hadm_id']
                admit_provider_id = row['admit_provider_id']
                
                # Only process if admit_provider_id is not null/empty
                if pd.notna(admit_provider_id) and str(admit_provider_id).strip():
                    provider_id_clean = str(admit_provider_id).strip()
                    
                    # Create provider node and relationship in single query
                    query = """
                    MERGE (p:Provider {provider_id: $provider_id})
                    ON CREATE SET p.created_at = datetime()
                    WITH p
                    MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                    MERGE (p)-[:ADMITS_PATIENT]->(h)
                    RETURN p.provider_id as created_provider
                    """
                    
                    result = session.run(query, 
                                       provider_id=provider_id_clean,
                                       hadm_id=int(hadm_id))
                    
                    created_provider = result.single()["created_provider"]
                    relationship_count += 1
                    logger.info(f"Created/merged Provider {created_provider} -> HospitalAdmission {hadm_id}")
        
        logger.info(f"Created {relationship_count} provider-admission relationships successfully!")
        
        # Optional: Create summary statistics
        with driver.session() as session:
            # Count total providers
            result = session.run("MATCH (p:Provider) RETURN count(p) as provider_count")
            provider_count = result.single()["provider_count"]
            
            # Count total hospital admissions
            result = session.run("MATCH (h:HospitalAdmission) RETURN count(h) as admission_count")
            admission_count = result.single()["admission_count"]
            
            # Count provider-admission relationships
            result = session.run("MATCH (p:Provider)-[:ADMITS_PATIENT]->(h:HospitalAdmission) RETURN count(*) as relationship_count")
            rel_count = result.single()["relationship_count"]
            
            logger.info(f"Summary: {provider_count} providers, {admission_count} admissions, {rel_count} relationships")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    add_provider_nodes()
