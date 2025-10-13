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
    
    # Convert charttime to datetime for proper sorting
    labevents_df['charttime'] = pd.to_datetime(labevents_df['charttime'])

    try:
        with driver.session() as session:
            # Delete any existing cross-connections before processing
            logger.info("Checking for and deleting cross-connections...")
            
            # Delete HAS_PRESCRIPTIONS relationships from Lab nodes
            query1 = """
            MATCH (lab)-[r:HAS_PRESCRIPTIONS]->()
            WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result1 = session.run(query1)
            count1 = result1.single()["deleted_count"]
            if count1 > 0:
                logger.info(f"Deleted {count1} HAS_PRESCRIPTIONS from Lab nodes")
            
            # Delete HAS_PROCEDURES relationships from Lab nodes
            query2 = """
            MATCH (lab)-[r:HAS_PROCEDURES]->()
            WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result2 = session.run(query2)
            count2 = result2.single()["deleted_count"]
            if count2 > 0:
                logger.info(f"Deleted {count2} HAS_PROCEDURES from Lab nodes")
            
            # Delete ANY remaining relationships between LabEvents and Prescriptions
            query3 = """
            MATCH (lab)-[r]-(presc)
            WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
              AND (presc:Prescription OR presc:PrescriptionBatch OR presc:PrescriptionsBatch OR presc:Medicine)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result3 = session.run(query3)
            count3 = result3.single()["deleted_count"]
            if count3 > 0:
                logger.info(f"Deleted {count3} connections between LabEvents and Prescriptions")
            
            # Delete ANY remaining relationships between LabEvents and Procedures
            query4 = """
            MATCH (lab)-[r]-(proc)
            WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
              AND (proc:Procedure OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result4 = session.run(query4)
            count4 = result4.single()["deleted_count"]
            if count4 > 0:
                logger.info(f"Deleted {count4} connections between LabEvents and Procedures")
            
            total_deleted = count1 + count2 + count3 + count4
            if total_deleted > 0:
                logger.info(f"Total cross-connections deleted: {total_deleted}")
            else:
                logger.info("No cross-connections found.")
            
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
                    (labevents_df["charttime"] >= intime) &
                    (labevents_df["charttime"] <= outtime)
                ].sort_values(by=["charttime", "labevent_id"])

                if labevents_for_event.empty:
                    continue

                # Create LabEvents node (central node) and link it to the Event
                query_labevents = """
                MATCH (e {event_id:$event_id})
                WHERE NOT e:PrescriptionBatch AND NOT e:ProceduresBatch AND NOT e:LabEventsBatch AND NOT e:LabEvents
                MERGE (le:LabEvents {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                ON CREATE SET le.name = "LabEvents"
                MERGE (e)-[:HAS_LAB_EVENTS]->(le)
                """
                session.run(query_labevents, event_id=event_id, hadm_id=hadm_id_int, subject_id=subject_id_int)

                # Group lab events by charttime to create Collections
                collections = labevents_for_event.groupby('charttime')
                collection_counter = 1
                
                for charttime, collection_events in collections:
                    # Create Collection node
                    collection_props = {
                        "event_id": event_id,
                        "hadm_id": hadm_id_int,
                        "subject_id": subject_id_int,
                        "charttime": charttime.strftime('%Y-%m-%d %H:%M:%S'),
                        "name": f"Collection_{collection_counter}"
                    }
                    
                    query_collection = """
                    MERGE (c:Collection {
                        event_id:$event_id,
                        hadm_id:$hadm_id,
                        subject_id:$subject_id,
                        charttime:$charttime
                    })
                    ON CREATE SET c.name=$name
                    ON MATCH SET c.name=$name
                    """
                    session.run(query_collection, **collection_props)
                    
                    # Link Collection → LabEvents
                    query_link_collection = """
                    MATCH (le:LabEvents {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                    MATCH (c:Collection {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id, charttime:$charttime})
                    MERGE (le)-[:HAS_COLLECTION]->(c)
                    """
                    session.run(query_link_collection, event_id=event_id, hadm_id=hadm_id_int,
                               subject_id=subject_id_int, charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    # Group collection events by specimen_id to create Specimens
                    specimens = collection_events.groupby('specimen_id')
                    
                    for specimen_id, specimen_events in specimens:
                        if pd.isna(specimen_id):
                            continue
                            
                        # Create Specimen node
                        specimen_props = {
                            "event_id": event_id,
                            "hadm_id": hadm_id_int,
                            "subject_id": subject_id_int,
                            "charttime": charttime.strftime('%Y-%m-%d %H:%M:%S'),
                            "specimen_id": int(specimen_id),
                            "name": f"Specimen_{int(specimen_id)}"
                        }
                        
                        query_specimen = """
                        MERGE (s:Specimen {
                            event_id:$event_id,
                            hadm_id:$hadm_id,
                            subject_id:$subject_id,
                            charttime:$charttime,
                            specimen_id:$specimen_id
                        })
                        ON CREATE SET s.name=$name
                        ON MATCH SET s.name=$name
                        """
                        session.run(query_specimen, **specimen_props)
                        
                        # Link Specimen → Collection
                        query_link_specimen = """
                        MATCH (c:Collection {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id, charttime:$charttime})
                        MATCH (s:Specimen {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id, charttime:$charttime, specimen_id:$specimen_id})
                        MERGE (c)-[:HAS_SPECIMEN]->(s)
                        """
                        session.run(query_link_specimen, event_id=event_id, hadm_id=hadm_id_int,
                                   subject_id=subject_id_int, charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'),
                                   specimen_id=int(specimen_id))
                        
                        # Process individual lab events for this specimen
                        labevent_counter = 1
                        
                        for _, row in specimen_events.iterrows():
                            # Create LabEvent node with lab item data included
                            labevent_props = {
                                "labevent_id": int(row["labevent_id"]),
                                "subject_id": int(row["subject_id"]),
                                "hadm_id": int(row["hadm_id"]),
                                "specimen_id": int(row["specimen_id"]) if pd.notna(row["specimen_id"]) else None,
                                "itemid": int(row["itemid"]),
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
                                # Lab item properties included directly
                                "item_label": str(row["label"]) if pd.notna(row["label"]) else "Unknown",
                                "item_fluid": str(row["fluid"]) if pd.notna(row["fluid"]) else "Unknown",
                                "item_category": str(row["category"]) if pd.notna(row["category"]) else "Unknown",
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
                                          le.comments=$comments, le.item_label=$item_label, le.item_fluid=$item_fluid,
                                          le.item_category=$item_category, le.name=$name
                            ON MATCH SET  le.specimen_id=$specimen_id, le.order_provider_id=$order_provider_id,
                                          le.charttime=$charttime, le.storetime=$storetime, le.value=$value,
                                          le.valuenum=$valuenum, le.valueuom=$valueuom, le.ref_range_lower=$ref_range_lower,
                                          le.ref_range_upper=$ref_range_upper, le.flag=$flag, le.priority=$priority,
                                          le.comments=$comments, le.item_label=$item_label, le.item_fluid=$item_fluid,
                                          le.item_category=$item_category, le.name=$name
                            """
                            session.run(query_labevent, **labevent_props)

                            # Link LabEvent → Specimen
                            query_link_labevent = """
                            MATCH (s:Specimen {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id, charttime:$charttime, specimen_id:$specimen_id})
                            MATCH (le:LabEvent {labevent_id:$labevent_id, subject_id:$subject_id, hadm_id:$hadm_id, itemid:$itemid})
                            MERGE (s)-[:HAS_LAB_EVENT]->(le)
                            """
                            session.run(query_link_labevent, event_id=event_id, hadm_id=hadm_id_int,
                                        subject_id=subject_id_int, charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'),
                                        specimen_id=int(specimen_id), labevent_id=int(row["labevent_id"]), itemid=int(row["itemid"]))

                            labevent_counter += 1
                    
                    collection_counter += 1
                
                logger.info(f"Added {len(labevents_for_event)} lab events for event {event_id}")

        logger.info("All lab events processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_labevent_nodes()
