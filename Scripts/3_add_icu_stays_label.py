# add_icu_stays_label.py
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

def add_icu_stays_label():
    """Add ICUStay label and additional properties to UnitAdmission nodes that are ICU stays"""
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"
    
    # File path
    ICUSTAYS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\icustays.csv"
    
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Load data
        icustays_df = pd.read_csv(ICUSTAYS_CSV)
        logger.info(f"Loaded {len(icustays_df)} ICU stay records")
        
        with driver.session() as session:
            icu_count = 0
            
            for _, row in icustays_df.iterrows():
                subject_id = int(row["subject_id"])
                hadm_id = row["hadm_id"]
                stay_id = str(int(row["stay_id"]))  # This matches transfer_id in transfers.csv
                first_careunit = row["first_careunit"]
                last_careunit = row["last_careunit"]
                intime = pd.to_datetime(row["intime"]).strftime("%Y-%m-%d %H:%M:%S")
                outtime = pd.to_datetime(row["outtime"]).strftime("%Y-%m-%d %H:%M:%S")
                los = float(row["los"])  # Length of stay in days
                
                # Find the UnitAdmission node by event_id (which was set from transfer_id)
                # and add ICUStay label plus additional properties
                query = """
                MATCH (u:UnitAdmission {event_id: $event_id})
                SET u:ICUStay,
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
                    logger.info(f"Added ICUStay label to {record['careunit']} (stay_id: {stay_id}, LOS: {los:.2f} days)")
                else:
                    logger.warning(f"Could not find UnitAdmission node with event_id: {stay_id}")
        
        logger.info(f"Successfully added ICUStay label to {icu_count} UnitAdmission nodes!")
        logger.info(f"These nodes now have dual labels: :UnitAdmission:ICUStay")
        
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

