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

def create_microbiology_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths - dynamically constructed
    MICROEVENTS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\microbiologyevents.csv"

    # Load CSV
    microevents_df = pd.read_csv(MICROEVENTS_CSV)
    
    # Convert charttime to datetime for proper sorting
    microevents_df['charttime'] = pd.to_datetime(microevents_df['charttime'])

    try:
        with driver.session() as session:
            # Delete any existing cross-connections before processing
            logger.info("Checking for and deleting cross-connections...")
            
            # Delete HAS_PRESCRIPTIONS relationships from Microbiology nodes
            query1 = """
            MATCH (micro)-[r:HAS_PRESCRIPTIONS]->()
            WHERE (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result1 = session.run(query1)
            count1 = result1.single()["deleted_count"]
            if count1 > 0:
                logger.info(f"Deleted {count1} HAS_PRESCRIPTIONS from Microbiology nodes")
            
            # Delete HAS_PROCEDURES relationships from Microbiology nodes
            query2 = """
            MATCH (micro)-[r:HAS_PROCEDURES]->()
            WHERE (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result2 = session.run(query2)
            count2 = result2.single()["deleted_count"]
            if count2 > 0:
                logger.info(f"Deleted {count2} HAS_PROCEDURES from Microbiology nodes")
            
            # Delete ANY remaining relationships between MicrobiologyEvents and Prescriptions
            query3 = """
            MATCH (micro)-[r]-(presc)
            WHERE (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
              AND (presc:Prescription OR presc:PrescriptionsBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result3 = session.run(query3)
            count3 = result3.single()["deleted_count"]
            if count3 > 0:
                logger.info(f"Deleted {count3} connections between MicrobiologyEvents and Prescriptions")
            
            # Delete ANY remaining relationships between MicrobiologyEvents and Procedures
            query4 = """
            MATCH (micro)-[r]-(proc)
            WHERE (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
              AND (proc:Procedure OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result4 = session.run(query4)
            count4 = result4.single()["deleted_count"]
            if count4 > 0:
                logger.info(f"Deleted {count4} connections between MicrobiologyEvents and Procedures")
            
            # Delete ANY remaining relationships between MicrobiologyEvents and LabEvents
            query5 = """
            MATCH (micro)-[r]-(lab)
            WHERE (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
              AND (lab:LabEvents OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result5 = session.run(query5)
            count5 = result5.single()["deleted_count"]
            if count5 > 0:
                logger.info(f"Deleted {count5} connections between MicrobiologyEvents and LabEvents")
            
            # Delete HAS_MICROBIOLOGY_EVENTS from Prescription nodes
            query6 = """
            MATCH (presc)-[r:HAS_MICROBIOLOGY_EVENTS]->()
            WHERE (presc:Prescription OR presc:PrescriptionsBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result6 = session.run(query6)
            count6 = result6.single()["deleted_count"]
            if count6 > 0:
                logger.info(f"Deleted {count6} HAS_MICROBIOLOGY_EVENTS from Prescription nodes")
            
            # Delete HAS_MICROBIOLOGY_EVENTS from Procedure nodes
            query7 = """
            MATCH (proc)-[r:HAS_MICROBIOLOGY_EVENTS]->()
            WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result7 = session.run(query7)
            count7 = result7.single()["deleted_count"]
            if count7 > 0:
                logger.info(f"Deleted {count7} HAS_MICROBIOLOGY_EVENTS from Procedure nodes")
            
            # Delete ANY remaining relationships between Prescriptions and MicrobiologyEvents
            query8 = """
            MATCH (presc)-[r]-(micro)
            WHERE (presc:Prescription OR presc:PrescriptionsBatch)
              AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result8 = session.run(query8)
            count8 = result8.single()["deleted_count"]
            if count8 > 0:
                logger.info(f"Deleted {count8} connections between Prescriptions and MicrobiologyEvents")
            
            # Delete ANY remaining relationships between Procedures and MicrobiologyEvents
            query9 = """
            MATCH (proc)-[r]-(micro)
            WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
              AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result9 = session.run(query9)
            count9 = result9.single()["deleted_count"]
            if count9 > 0:
                logger.info(f"Deleted {count9} connections between Procedures and MicrobiologyEvents")
            
            # Delete old CONTAINED_MICROBIOLOGY_EVENT relationships
            query10 = """
            MATCH ()-[r:CONTAINED_MICROBIOLOGY_EVENT]->()
            DELETE r
            RETURN count(r) as deleted_count
            """
            result10 = session.run(query10)
            count10 = result10.single()["deleted_count"]
            if count10 > 0:
                logger.info(f"Deleted {count10} old CONTAINED_MICROBIOLOGY_EVENT relationships")
            
            # Delete old INCLUDED_MICROBIOLOGY_EVENTS relationships
            query11 = """
            MATCH ()-[r:INCLUDED_MICROBIOLOGY_EVENTS]->()
            DELETE r
            RETURN count(r) as deleted_count
            """
            result11 = session.run(query11)
            count11 = result11.single()["deleted_count"]
            if count11 > 0:
                logger.info(f"Deleted {count11} old INCLUDED_MICROBIOLOGY_EVENTS relationships")
            
            # Delete old MicrobiologyEvents batch nodes
            query12 = """
            MATCH (me:MicrobiologyEvents)
            DETACH DELETE me
            RETURN count(me) as deleted_count
            """
            result12 = session.run(query12)
            count12 = result12.single()["deleted_count"]
            if count12 > 0:
                logger.info(f"Deleted {count12} old MicrobiologyEvents batch nodes")
            
            # Delete old MicrobiologyEvent nodes (to ensure clean recreation)
            query13 = """
            MATCH (me:MicrobiologyEvent)
            DETACH DELETE me
            RETURN count(me) as deleted_count
            """
            result13 = session.run(query13)
            count13 = result13.single()["deleted_count"]
            if count13 > 0:
                logger.info(f"Deleted {count13} old MicrobiologyEvent nodes")
            
            total_deleted = count1 + count2 + count3 + count4 + count5 + count6 + count7 + count8 + count9 + count10 + count11 + count12 + count13
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
                
                if event_id is None or subject_id_raw is None:
                    logger.warning(f"Skipping event with missing IDs: event_id={event_id}, subject_id={subject_id_raw}, hadm_id={hadm_id_raw}")
                    continue
                
                subject_id = str(subject_id_raw).strip()
                hadm_id = str(hadm_id_raw).strip() if hadm_id_raw is not None else None
                
                try:
                    subject_id_int = int(subject_id)
                    hadm_id_int = int(hadm_id) if hadm_id else None
                except ValueError:
                    logger.warning(f"Skipping event with invalid ID format: subject_id={subject_id}, hadm_id={hadm_id}")
                    continue
                
                intime = pd.to_datetime(record["intime"])
                outtime = pd.to_datetime(record["outtime"])

                # Filter microbiology events for this event
                # Handle both cases: with hadm_id and without hadm_id
                if hadm_id_int is not None:
                    microevents_for_event = microevents_df[
                        (microevents_df["subject_id"] == subject_id_int) &
                        (microevents_df["hadm_id"] == hadm_id_int) &
                        (microevents_df["charttime"] >= intime) &
                        (microevents_df["charttime"] <= outtime)
                    ].sort_values(by=["charttime", "micro_specimen_id", "microevent_id"])
                else:
                    # When hadm_id is None, match records where hadm_id is null in the CSV
                    microevents_for_event = microevents_df[
                        (microevents_df["subject_id"] == subject_id_int) &
                        (microevents_df["hadm_id"].isna()) &
                        (microevents_df["charttime"] >= intime) &
                        (microevents_df["charttime"] <= outtime)
                    ].sort_values(by=["charttime", "micro_specimen_id", "microevent_id"])

                if microevents_for_event.empty:
                    continue

                # Ensure LabEvents node exists for this event (it should already exist from lab events script)
                # If it doesn't exist, create it
                query_ensure_labevents = """
                MATCH (e {event_id:$event_id})
                WHERE NOT e:PrescriptionsBatch AND NOT e:ProceduresBatch AND NOT e:LabEvents AND NOT e:LabEvent 
                      AND NOT e:MicrobiologyEvents AND NOT e:MicrobiologyEvent
                      AND NOT e:Prescription AND NOT e:Procedure AND NOT e:Procedures
                MERGE (le:LabEvents {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                ON CREATE SET le.name = "LabEvents"
                MERGE (e)-[:INCLUDED_LAB_EVENTS]->(le)
                """
                session.run(query_ensure_labevents, event_id=event_id, hadm_id=hadm_id_int, subject_id=subject_id_int)

                # Group microbiology events by specimen_id and charttime to create MicrobiologyEvent nodes
                microevent_groups = microevents_for_event.groupby(['micro_specimen_id', 'charttime'])
                microevent_counter = 1
                
                for (specimen_id, charttime), microevent_data in microevent_groups:
                    # Build micro_results array as formatted strings
                    micro_results = []
                    for _, row in microevent_data.iterrows():
                        spec_type_desc = str(row["spec_type_desc"]) if pd.notna(row["spec_type_desc"]) else "Unknown"
                        test_name = str(row["test_name"]) if pd.notna(row["test_name"]) else "Unknown"
                        org_name = str(row["org_name"]) if pd.notna(row["org_name"]) else None
                        ab_name = str(row["ab_name"]) if pd.notna(row["ab_name"]) else None
                        dilution_value = row["dilution_value"] if pd.notna(row["dilution_value"]) else None
                        interpretation = str(row["interpretation"]) if pd.notna(row["interpretation"]) else None
                        comments = str(row["comments"]) if pd.notna(row["comments"]) else None
                        
                        # Build the formatted string based on whether org_name exists
                        if org_name:
                            # With organism name
                            result_str = f"{spec_type_desc}: {test_name} → {org_name}"
                            # Add antibiotic info if available
                            if ab_name and dilution_value is not None and interpretation:
                                result_str += f" | {ab_name}={dilution_value}({interpretation})"
                        else:
                            # Without organism name - use comments
                            if comments:
                                result_str = f"{spec_type_desc}: {test_name} → {comments}"
                            else:
                                result_str = f"{spec_type_desc}: {test_name}"
                        
                        micro_results.append(result_str)
                    
                    # Create MicrobiologyEvent node with aggregated micro results as array of strings
                    microevent_props = {
                        "event_id": event_id,
                        "hadm_id": hadm_id_int,
                        "subject_id": subject_id_int,
                        "micro_specimen_id": int(specimen_id) if pd.notna(specimen_id) else None,
                        "charttime": charttime.strftime('%Y-%m-%d %H:%M:%S'),
                        "micro_results": micro_results,
                        "micro_count": len(micro_results),
                        "name": "MicrobiologyEvent"
                    }
                    
                    query_microevent = """
                    MERGE (me:MicrobiologyEvent {
                        event_id: $event_id,
                        hadm_id: $hadm_id,
                        subject_id: $subject_id,
                        micro_specimen_id: $micro_specimen_id,
                        charttime: $charttime
                    })
                    SET me.micro_results = $micro_results,
                        me.micro_count = $micro_count,
                        me.name = $name
                    """
                    session.run(query_microevent, **microevent_props)
                    
                    # Link MicrobiologyEvent → LabEvents
                    query_link_microevent = """
                    MATCH (le:LabEvents {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                    MATCH (me:MicrobiologyEvent {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id, 
                                                  micro_specimen_id: $micro_specimen_id, charttime: $charttime})
                    MERGE (le)-[:CONTAINED_MICROBIOLOGY_EVENT]->(me)
                    """
                    session.run(query_link_microevent, event_id=event_id, hadm_id=hadm_id_int,
                               subject_id=subject_id_int, micro_specimen_id=int(specimen_id) if pd.notna(specimen_id) else None,
                               charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    microevent_counter += 1
                
                logger.info(f"Added {len(microevents_for_event)} microbiology events for event {event_id}")

        logger.info("All microbiology events processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_microbiology_nodes()

