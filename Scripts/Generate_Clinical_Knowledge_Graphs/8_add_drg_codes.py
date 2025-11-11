# add_drg_codes.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def add_drg_codes():
    """Add DRG codes to the knowledge graph"""
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    
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
            drg_count = 0
            link_count = 0
            
            for _, row in drg_df.iterrows():
                subject_id = int(row["subject_id"])
                hadm_id = row["hadm_id"]
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
                
                logger.info(f"Processed DRG {drg_id} ({drg_type}) for admission {hadm_id} - {description[:50]}...")
        
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

