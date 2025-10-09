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

def create_procedure_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths - dynamically constructed
    PROCEDURES_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\procedures_icd.csv"
    ICD_LOOKUP_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\d_icd_procedures.csv"

    # Load CSVs
    proc_df = pd.read_csv(PROCEDURES_CSV)
    icd_lookup = pd.read_csv(ICD_LOOKUP_CSV)

    # Merge to add long_title
    proc_df = proc_df.merge(icd_lookup, on=["icd_code", "icd_version"], how="left")

    try:
        with driver.session() as session:
            # Fetch events with intime/outtime
            query_events = """
            MATCH (e)
            WHERE e.intime IS NOT NULL AND e.outtime IS NOT NULL
            RETURN e.event_id AS event_id, e.subject_id AS subject_id, e.hadm_id AS hadm_id,
                   e.intime AS intime, e.outtime AS outtime
            """
            events = session.run(query_events)

            for record in events:
                event_id = str(record["event_id"]).strip() if record["event_id"] is not None else None
                subject_id_raw = record["subject_id"]
                hadm_id_raw = record["hadm_id"]
                
                if event_id is None or subject_id_raw is None or hadm_id_raw is None:
                    logger.warning(f"Skipping event with missing IDs: event_id={event_id}, subject_id={subject_id_raw}, hadm_id={hadm_id_raw}")
                    continue
                
                subject_id = str(subject_id_raw).strip()
                hadm_id = str(hadm_id_raw).strip()
                
                try:
                    subject_id_int = int(subject_id)
                    hadm_id_int = int(hadm_id)
                except ValueError:
                    logger.warning(f"Skipping event with invalid ID format: subject_id={subject_id}, hadm_id={hadm_id}")
                    continue
                
                intime = pd.to_datetime(record["intime"])
                outtime = pd.to_datetime(record["outtime"])

                # Filter procedures for this event
                procs_for_event = proc_df[
                    (proc_df["subject_id"] == subject_id_int) &
                    (proc_df["hadm_id"] == hadm_id_int) &
                    (pd.to_datetime(proc_df["chartdate"]) >= intime) &
                    (pd.to_datetime(proc_df["chartdate"]) <= outtime)
                ].sort_values(by=["seq_num", "chartdate"])

                if procs_for_event.empty:
                    continue

                # Create ProceduresBatch node and link it to the Event (no cross-links to other batch types)
                query_batch = """
                MATCH (e {event_id:$event_id})
                WHERE NOT e:PrescriptionBatch AND NOT e:ProceduresBatch AND NOT e:LabEventsBatch
                MERGE (pb:ProceduresBatch {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                ON CREATE SET pb.name = "Procedures"
                MERGE (e)-[:HAS_PROCEDURES]->(pb)
                """
                session.run(query_batch, event_id=event_id, hadm_id=hadm_id_int, subject_id=subject_id_int)

                # Add procedure nodes
                proc_counter = 1
                for _, row in procs_for_event.iterrows():
                    proc_props = {
                        "subject_id": int(row["subject_id"]),
                        "hadm_id": int(row["hadm_id"]),
                        "seq_num": int(row["seq_num"]),
                        "chartdate": str(row["chartdate"]),
                        "icd_code": str(row["icd_code"]),
                        "icd_version": str(row["icd_version"]),
                        "title": str(row["long_title"]) if pd.notna(row["long_title"]) else "Unknown",
                        "name": f"Procedure_{proc_counter}"
                    }

                    query_proc = """
                    MERGE (p:Procedure {
                        subject_id:$subject_id,
                        hadm_id:$hadm_id,
                        seq_num:$seq_num,
                        icd_code:$icd_code,
                        icd_version:$icd_version
                    })
                    ON CREATE SET p.chartdate=$chartdate, p.title=$title, p.name=$name
                    ON MATCH SET  p.chartdate=$chartdate, p.title=$title, p.name=$name
                    """
                    session.run(query_proc, **proc_props)

                    # Link Procedure → ProceduresBatch
                    query_link = """
                    MATCH (pb:ProceduresBatch {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                    MATCH (p:Procedure {subject_id:$subject_id, hadm_id:$hadm_id, seq_num:$seq_num, icd_code:$icd_code})
                    MERGE (pb)-[:HAS_PROCEDURE]->(p)
                    """
                    session.run(query_link, event_id=event_id, hadm_id=hadm_id_int,
                                subject_id=subject_id_int, seq_num=int(row["seq_num"]), icd_code=str(row["icd_code"]))

                    proc_counter += 1

                # No cross-relationships: ProceduresBatch isolated from PrescriptionBatch and LabEventsBatch per user request

                logger.info(f"Added {len(procs_for_event)} procedures for event {event_id}")

        logger.info("All procedures processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_procedure_nodes()
