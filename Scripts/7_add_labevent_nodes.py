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

def create_labevent_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths - dynamically constructed
    LABEVENTS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\labevents.csv"
    LAB_LOOKUP_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\d_labitems.csv"

    # Load CSVs
    labevents_df = pd.read_csv(LABEVENTS_CSV)
    labitems_lookup = pd.read_csv(LAB_LOOKUP_CSV)

    # Merge to add lab item details
    labevents_df = labevents_df.merge(labitems_lookup, on="itemid", how="left")

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

                # Filter lab events for this event
                labevents_for_event = labevents_df[
                    (labevents_df["subject_id"] == subject_id_int) &
                    (labevents_df["hadm_id"] == hadm_id_int) &
                    (pd.to_datetime(labevents_df["charttime"]) >= intime) &
                    (pd.to_datetime(labevents_df["charttime"]) <= outtime)
                ].sort_values(by=["charttime", "labevent_id"])

                if labevents_for_event.empty:
                    continue

                # Create LabEventsBatch node and link it to the Event (no cross-links to other batch types)
                query_batch = """
                MATCH (e {event_id:$event_id})
                WHERE NOT e:PrescriptionBatch AND NOT e:ProceduresBatch AND NOT e:LabEventsBatch
                MERGE (leb:LabEventsBatch {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                ON CREATE SET leb.name = "LabEvents"
                MERGE (e)-[:HAS_LAB_EVENTS]->(leb)
                """
                session.run(query_batch, event_id=event_id, hadm_id=hadm_id_int, subject_id=subject_id_int)

                # Process lab events
                labevent_counter = 1
                processed_items = set()  # Track which lab items we've already created
                
                for _, row in labevents_for_event.iterrows():
                    # Create LabItem node (one per unique itemid)
                    itemid = int(row["itemid"])
                    if itemid not in processed_items:
                        labitem_props = {
                            "itemid": itemid,
                            "label": str(row["label"]) if pd.notna(row["label"]) else "Unknown",
                            "fluid": str(row["fluid"]) if pd.notna(row["fluid"]) else "Unknown",
                            "category": str(row["category"]) if pd.notna(row["category"]) else "Unknown",
                            "name": f"LabItem_{itemid}"
                        }

                        query_labitem = """
                        MERGE (li:LabItem {itemid:$itemid})
                        ON CREATE SET li.label=$label, li.fluid=$fluid, li.category=$category, li.name=$name
                        ON MATCH SET li.label=$label, li.fluid=$fluid, li.category=$category, li.name=$name
                        """
                        session.run(query_labitem, **labitem_props)
                        processed_items.add(itemid)

                    # Create LabEvent node
                    labevent_props = {
                        "labevent_id": int(row["labevent_id"]),
                        "subject_id": int(row["subject_id"]),
                        "hadm_id": int(row["hadm_id"]),
                        "specimen_id": int(row["specimen_id"]) if pd.notna(row["specimen_id"]) else None,
                        "itemid": itemid,
                        "order_provider_id": str(row["order_provider_id"]) if pd.notna(row["order_provider_id"]) else None,
                        "charttime": str(row["charttime"]),
                        "storetime": str(row["storetime"]) if pd.notna(row["storetime"]) else None,
                        "value": str(row["value"]) if pd.notna(row["value"]) else None,
                        "valuenum": float(row["valuenum"]) if pd.notna(row["valuenum"]) else None,
                        "valueuom": str(row["valueuom"]) if pd.notna(row["valueuom"]) else None,
                        "ref_range_lower": float(row["ref_range_lower"]) if pd.notna(row["ref_range_lower"]) else None,
                        "ref_range_upper": float(row["ref_range_upper"]) if pd.notna(row["ref_range_upper"]) else None,
                        "flag": str(row["flag"]) if pd.notna(row["flag"]) else None,
                        "priority": str(row["priority"]) if pd.notna(row["priority"]) else None,
                        "comments": str(row["comments"]) if pd.notna(row["comments"]) else None,
                        "name": f"LabEvent_{labevent_counter}"
                    }

                    query_labevent = """
                    MERGE (le:LabEvent {
                        labevent_id:$labevent_id,
                        subject_id:$subject_id,
                        hadm_id:$hadm_id,
                        itemid:$itemid
                    })
                    ON CREATE SET le.specimen_id=$specimen_id, le.order_provider_id=$order_provider_id,
                                  le.charttime=$charttime, le.storetime=$storetime, le.value=$value,
                                  le.valuenum=$valuenum, le.valueuom=$valueuom, le.ref_range_lower=$ref_range_lower,
                                  le.ref_range_upper=$ref_range_upper, le.flag=$flag, le.priority=$priority,
                                  le.comments=$comments, le.name=$name
                    ON MATCH SET  le.specimen_id=$specimen_id, le.order_provider_id=$order_provider_id,
                                  le.charttime=$charttime, le.storetime=$storetime, le.value=$value,
                                  le.valuenum=$valuenum, le.valueuom=$valueuom, le.ref_range_lower=$ref_range_lower,
                                  le.ref_range_upper=$ref_range_upper, le.flag=$flag, le.priority=$priority,
                                  le.comments=$comments, le.name=$name
                    """
                    session.run(query_labevent, **labevent_props)

                    # Link LabEvent → LabEventsBatch
                    query_link_batch = """
                    MATCH (leb:LabEventsBatch {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                    MATCH (le:LabEvent {labevent_id:$labevent_id, subject_id:$subject_id, hadm_id:$hadm_id, itemid:$itemid})
                    MERGE (leb)-[:HAS_LAB_EVENT]->(le)
                    """
                    session.run(query_link_batch, event_id=event_id, hadm_id=hadm_id_int,
                                subject_id=subject_id_int, labevent_id=int(row["labevent_id"]), itemid=itemid)

                    # Link LabEvent → LabItem (one-to-one relationship)
                    query_link_item = """
                    MATCH (le:LabEvent {labevent_id:$labevent_id, subject_id:$subject_id, hadm_id:$hadm_id, itemid:$itemid})
                    MATCH (li:LabItem {itemid:$itemid})
                    MERGE (le)-[:MEASURED]->(li)
                    """
                    session.run(query_link_item, labevent_id=int(row["labevent_id"]), 
                                subject_id=subject_id_int, hadm_id=hadm_id_int, itemid=itemid)

                    labevent_counter += 1

                # No cross-relationships: LabEventsBatch isolated from PrescriptionBatch and ProceduresBatch per user request
                
                logger.info(f"Added {len(labevents_for_event)} lab events for event {event_id}")

        logger.info("All lab events processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_labevent_nodes()
