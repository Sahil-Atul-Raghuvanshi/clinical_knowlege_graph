# add_assessment_nodes.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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

def create_initial_assessment_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File paths - dynamically constructed
    TRIAGE_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\triage.csv"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load triage data
        triage_df = pd.read_csv(TRIAGE_CSV)
        
        logger.info(f"Loaded {len(triage_df)} triage records")

        with driver.session() as session:
            # Iterate over each triage record
            for _, row in triage_df.iterrows():
                stay_id = row['stay_id']
                
                # Skip if stay_id is missing
                if pd.isna(stay_id):
                    continue
                
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
                
                logger.info(f"Created InitialAssessment for ED stay {stay_id}")
                
        logger.info("All initial assessment nodes created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

    finally:
        driver.close()


if __name__ == "__main__":
    create_initial_assessment_nodes()

