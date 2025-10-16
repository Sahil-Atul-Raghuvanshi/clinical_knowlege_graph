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

def create_ed_diagnosis_nodes(driver, folder_name):
    """Create diagnosis nodes for Emergency Department visits"""
    logger.info("Processing ED diagnoses...")
    
    # File path for ED diagnosis
    ED_DIAGNOSIS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\ed_diagnosis.csv"
    
    try:
        # Load ED diagnosis data
        ed_diag_df = pd.read_csv(ED_DIAGNOSIS_CSV)
        
        # Group diagnoses by stay_id
        grouped_diagnoses = ed_diag_df.groupby('stay_id').agg({
            'icd_title': lambda x: list(x),
            'subject_id': 'first'  # Take the first subject_id for each stay
        }).reset_index()
        
        with driver.session() as session:
            for _, row in grouped_diagnoses.iterrows():
                stay_id = str(row['stay_id'])
                subject_id = str(row['subject_id'])
                diagnosis_titles = row['icd_title']  # Already a list from groupby
                
                # Create Diagnosis node and link it to EmergencyDepartment
                query_ed_diagnosis = """
                MATCH (ed:EmergencyDepartment {event_id:$stay_id})
                MERGE (diag:Diagnosis {
                    event_id:$stay_id,
                    subject_id:$subject_id,
                    ed_diagnosis:true
                })
                SET diag.titles = $titles,
                    diag.diagnosis_count = $count
                MERGE (ed)-[:HAS_DIAGNOSES]->(diag)
                """
                session.run(query_ed_diagnosis,
                          stay_id=stay_id,
                          subject_id=subject_id,
                          titles=diagnosis_titles,
                          count=len(diagnosis_titles))
                
                logger.info(f"Added {len(diagnosis_titles)} ED diagnoses for stay_id {stay_id}")
                
    except Exception as e:
        logger.error(f"Error processing ED diagnoses: {e}")
        raise

def create_diagnosis_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths - dynamically constructed
    DIAGNOSES_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\diagnoses_icd.csv"
    ICD_LOOKUP_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\d_icd_diagnoses.csv"

    # Load CSVs
    diag_df = pd.read_csv(DIAGNOSES_CSV)
    icd_lookup = pd.read_csv(ICD_LOOKUP_CSV)

    # Merge to add long_title
    diag_df = diag_df.merge(icd_lookup, on=["icd_code", "icd_version"], how="left")

    try:
        with driver.session() as session:
            # Fetch all discharge nodes with their associated hadm_id
            query_discharges = """
            MATCH (d:Discharge)
            RETURN d.event_id AS event_id, d.hadm_id AS hadm_id, d.subject_id AS subject_id
            """
            discharges = session.run(query_discharges)

            for record in discharges:
                event_id = record["event_id"]
                hadm_id_raw = record["hadm_id"]
                subject_id_raw = record["subject_id"]
                
                if event_id is None or hadm_id_raw is None or subject_id_raw is None:
                    logger.warning(f"Skipping discharge with missing IDs: event_id={event_id}, hadm_id={hadm_id_raw}, subject_id={subject_id_raw}")
                    continue
                
                hadm_id = str(hadm_id_raw).strip()
                subject_id = str(subject_id_raw).strip()
                
                try:
                    hadm_id_int = int(hadm_id)
                    subject_id_int = int(subject_id)
                except ValueError:
                    logger.warning(f"Skipping discharge with invalid ID format: hadm_id={hadm_id}, subject_id={subject_id}")
                    continue

                # Filter diagnoses for this admission (diagnoses are linked to admissions, not events)
                diags_for_admission = diag_df[
                    (diag_df["subject_id"] == subject_id_int) &
                    (diag_df["hadm_id"] == hadm_id_int)
                ].sort_values(by="seq_num")

                if diags_for_admission.empty:
                    continue

                # Build array of diagnosis titles
                diagnosis_titles = []
                for _, row in diags_for_admission.iterrows():
                    title = str(row["long_title"]) if pd.notna(row["long_title"]) else "Unknown"
                    diagnosis_titles.append(title)

                # Create Diagnosis node with array of titles and link it to the Discharge
                query_diagnosis = """
                MATCH (d:Discharge {event_id:$event_id})
                MERGE (diag:Diagnosis {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                SET diag.titles = $titles,
                    diag.diagnosis_count = $count
                MERGE (d)-[:HAS_DIAGNOSES]->(diag)
                """
                session.run(query_diagnosis, event_id=event_id, hadm_id=hadm_id_int, 
                           subject_id=subject_id_int, titles=diagnosis_titles, count=len(diags_for_admission))

                logger.info(f"Added {len(diags_for_admission)} diagnoses for discharge event {event_id} (admission {hadm_id})")

        logger.info("All diagnoses processed successfully!")

    finally:
        # Don't close the driver here as we need it for ED diagnoses
        pass

    try:
        # Process ED diagnoses
        create_ed_diagnosis_nodes(driver, folder_name)
    finally:
        driver.close()


if __name__ == "__main__":
    create_diagnosis_nodes()
