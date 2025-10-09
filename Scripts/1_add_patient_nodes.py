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

def add_patient_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"
    
    # Path to patient CSV file - dynamically constructed
    PATIENTS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\patients.csv"
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            logger.info(f"Connection successful!")
        
        # Load patient data
        df = pd.read_csv(PATIENTS_CSV)
        logger.info(f"Found {len(df)} patient records")
        
        # Create patient nodes
        with driver.session() as session:
            for _, row in df.iterrows():
                query = """
                MERGE (p:Patient {subject_id: $subject_id})
                ON CREATE SET 
                    p.gender = $gender,
                    p.anchor_age = $anchor_age,
                    p.anchor_year = $anchor_year,
                    p.anchor_year_group = $anchor_year_group,
                    p.dod = $dod
                """
                
                # Handle null values
                dod = row['dod'] if pd.notna(row['dod']) else None
                
                session.run(query, 
                           subject_id=int(row['subject_id']),
                           gender=row['gender'],
                           anchor_age=int(row['anchor_age']),
                           anchor_year=int(row['anchor_year']),
                           anchor_year_group=row['anchor_year_group'],
                           dod=dod)
                
                logger.info(f"Created patient node for subject_id: {row['subject_id']}")
        
        logger.info("Patient nodes created successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    add_patient_nodes()