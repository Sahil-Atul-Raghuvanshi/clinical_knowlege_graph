# add_prescription_nodes.py
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

def create_prescription_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File path - dynamically constructed
    PRESCRIPTIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\prescriptions.csv"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load prescriptions
        presc_df = pd.read_csv(PRESCRIPTIONS_CSV)
        presc_df["starttime"] = pd.to_datetime(presc_df["starttime"], errors="coerce")
        presc_df["stoptime"] = pd.to_datetime(presc_df["stoptime"], errors="coerce")
        
        # Remove duplicate prescriptions before processing
        initial_count = len(presc_df)
        presc_df = presc_df.drop_duplicates(subset=['poe_id'], keep='first')
        final_count = len(presc_df)
        logger.info(f"Removed {initial_count - final_count} duplicate prescriptions. {final_count} unique prescriptions remaining.")

        with driver.session() as session:
            # Fetch all event nodes with intime/outtime
            query_events = """
            MATCH (e)
            WHERE e.intime IS NOT NULL AND e.outtime IS NOT NULL
            RETURN e.event_id AS event_id, e.intime AS intime, e.outtime AS outtime
            """
            events = session.run(query_events)

            event_list = [
                {
                    "event_id": record["event_id"],
                    "intime": pd.to_datetime(record["intime"]),
                    "outtime": pd.to_datetime(record["outtime"]),
                }
                for record in events
            ]

            logger.info(f"Found {len(event_list)} events with intime/outtime")

            # Iterate over events and associate prescriptions
            for event in event_list:
                event_id = event["event_id"]
                intime = event["intime"]
                outtime = event["outtime"]

                # Filter prescriptions within the event period
                presc_for_event = presc_df[
                    (presc_df["starttime"] >= intime) & (presc_df["starttime"] <= outtime)
                ]

                if presc_for_event.empty:
                    continue

                # Create PrescriptionBatch node with unique identifier
                batch_id = f"prescription_batch_{event_id}"
                logger.info(f"Creating PrescriptionBatch with batch_id: {batch_id}, event_id: {event_id}")
                query_batch = """
                MERGE (pb:PrescriptionBatch {batch_id: $batch_id})
                ON CREATE SET pb.event_id = $event_id, pb.prescription_count = $count, pb.name = "Prescriptions"
                ON MATCH SET pb.prescription_count = $count, pb.name = "Prescriptions"
                """
                session.run(query_batch, batch_id=batch_id, event_id=event_id, count=len(presc_for_event))

                # Link Event → PrescriptionBatch (ENSURE no self-loop and no cross-links to other batch types)
                query_link_batch = """
                MATCH (e {event_id: $event_id})
                MATCH (pb:PrescriptionBatch {batch_id: $batch_id})
                WHERE e <> pb AND NOT e:PrescriptionBatch AND NOT e:ProceduresBatch AND NOT e:LabEventsBatch
                MERGE (e)-[:HAS_PRESCRIPTIONS]->(pb)
                """
                session.run(query_link_batch, event_id=event_id, batch_id=batch_id)

                # Sort prescriptions by starttime for consistent naming
                presc_for_event_sorted = presc_for_event.sort_values(by="starttime", ascending=True).reset_index(drop=True)

                # Reset counter per batch (so numbering starts from 1 each time)
                counter = 1

                # Create individual prescription nodes and link → PrescriptionBatch
                for _, row in presc_for_event_sorted.iterrows():
                    prescription_id = str(row["poe_id"]).strip() if pd.notna(row["poe_id"]) else None
                    if not prescription_id:
                        continue

                    raw_subject_id = str(row["subject_id"]).split("-")[0]
                    try:
                        subject_id = int(raw_subject_id)
                    except ValueError:
                        logger.warning(f"Skipping prescription with invalid subject_id: {row['subject_id']}")
                        continue

                    hadm_id = str(row["hadm_id"]).strip() if pd.notna(row["hadm_id"]) else None

                    # Prepare all properties dynamically
                    presc_props = {
                        "poe_id": prescription_id,
                        "subject_id": subject_id,
                        "hadm_id": hadm_id,
                        "pharmacy_id": row.get("pharmacy_id"),
                        "poe_seq": row.get("poe_seq"),
                        "order_provider_id": row.get("order_provider_id"),
                        "starttime": row["starttime"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row["starttime"]) else None,
                        "stoptime": row["stoptime"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row["stoptime"]) else None,
                        "drug_type": row.get("drug_type"),
                        "drug": row.get("drug"),
                        "formulary_drug_cd": row.get("formulary_drug_cd"),
                        "gsn": row.get("gsn"),
                        "ndc": row.get("ndc"),
                        "prod_strength": row.get("prod_strength"),
                        "form_rx": row.get("form_rx"),
                        "dose_val_rx": row.get("dose_val_rx"),
                        "dose_unit_rx": row.get("dose_unit_rx"),
                        "form_val_disp": row.get("form_val_disp"),
                        "form_unit_disp": row.get("form_unit_disp"),
                        "doses_per_24_hrs": row.get("doses_per_24_hrs"),
                        "route": row.get("route"),
                        "name": f"Prescription_{counter}",
                    }

                    # Create/update Prescription node
                    props_cypher = ", ".join([f"p.{k} = ${k}" for k in presc_props.keys()])
                    query_presc = f"""
                    MERGE (p:Prescription {{poe_id: $poe_id}})
                    ON CREATE SET {props_cypher}
                    ON MATCH SET {props_cypher}
                    """
                    session.run(query_presc, **presc_props)

                    # Link Prescription → PrescriptionBatch
                    query_link_presc = """
                    MATCH (p:Prescription {poe_id: $poe_id})
                    MATCH (pb:PrescriptionBatch {batch_id: $batch_id})
                    WHERE p <> pb
                    MERGE (p)-[:PART_OF_BATCH]->(pb)
                    """
                    session.run(query_link_presc, poe_id=prescription_id, batch_id=batch_id)

                    counter += 1  # increment within batch

                # No cross-relationships: PrescriptionBatch isolated from ProceduresBatch and LabEventsBatch per user request
                
                logger.info(f"Processed {len(presc_for_event_sorted)} prescriptions for event {event_id}")

        logger.info("All prescriptions processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()


if __name__ == "__main__":
    create_prescription_nodes()
