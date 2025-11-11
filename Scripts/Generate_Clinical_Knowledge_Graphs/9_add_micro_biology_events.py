import pandas as pd
from neo4j import GraphDatabase
import logging
import os
from typing import Optional
from incremental_load_utils import IncrementalLoadChecker
from etl_tracker import ETLTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_microbiology_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '9_add_micro_biology_events'

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    MICROEVENTS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'microbiologyevents.csv')

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
            
            # Check for existing microbiology events (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            events_with_microbiology = set()
            
            # Get events that already have MicrobiologyEvent nodes
            query_existing = """
            MATCH (le:LabEvents)-[:CONTAINED_MICROBIOLOGY_EVENT]->(me:MicrobiologyEvent)
            RETURN DISTINCT le.event_id AS event_id
            """
            result = session.run(query_existing)
            events_with_microbiology = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(events_with_microbiology)} events with existing microbiology events")
            
            # Fetch events with intime/outtime
            query_events = """
            MATCH (e)
            WHERE e.intime IS NOT NULL AND e.outtime IS NOT NULL
            RETURN e.event_id AS event_id, e.subject_id AS subject_id, e.hadm_id AS hadm_id,
                   e.intime AS intime, e.outtime AS outtime
            """
            events = session.run(query_events)
            
            skipped_count = 0
            processed_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()

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
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if event_id in events_with_microbiology:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping event {event_id} (patient {subject_id_int} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if event already has microbiology events (incremental load)
                if event_id in events_with_microbiology:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping event {event_id} - already has microbiology events (incremental load). Total skipped: {skipped_count}")
                    continue
                
                processed_count += 1
                
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
                # Use charttime when hadm_id is null
                if hadm_id_int is not None:
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
                else:
                    # When hadm_id is null, use charttime as identifier
                    first_charttime = microevents_for_event['charttime'].min().strftime('%Y-%m-%d %H:%M:%S')
                    query_ensure_labevents = """
                    MATCH (e {event_id:$event_id})
                    WHERE NOT e:PrescriptionsBatch AND NOT e:ProceduresBatch AND NOT e:LabEvents AND NOT e:LabEvent 
                          AND NOT e:MicrobiologyEvents AND NOT e:MicrobiologyEvent
                          AND NOT e:Prescription AND NOT e:Procedure AND NOT e:Procedures
                    MERGE (le:LabEvents {event_id:$event_id, subject_id:$subject_id, charttime:$charttime})
                    ON CREATE SET le.name = "LabEvents"
                    MERGE (e)-[:INCLUDED_LAB_EVENTS]->(le)
                    """
                    session.run(query_ensure_labevents, event_id=event_id, subject_id=subject_id_int, charttime=first_charttime)

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
                    # Use charttime when hadm_id is null
                    micro_specimen_id_val = int(specimen_id) if pd.notna(specimen_id) else None
                    charttime_str = charttime.strftime('%Y-%m-%d %H:%M:%S')
                    
                    if hadm_id_int is not None:
                        # When hadm_id exists, use it as identifier
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
                        session.run(query_microevent,
                                  event_id=event_id,
                                  hadm_id=hadm_id_int,
                                  subject_id=subject_id_int,
                                  micro_specimen_id=micro_specimen_id_val,
                                  charttime=charttime_str,
                                  micro_results=micro_results,
                                  micro_count=len(micro_results),
                                  name="MicrobiologyEvent")
                        
                        # Link MicrobiologyEvent → LabEvents (with hadm_id)
                        query_link_microevent = """
                        MATCH (le:LabEvents {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        MATCH (me:MicrobiologyEvent {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id, 
                                                      micro_specimen_id: $micro_specimen_id, charttime: $charttime})
                        MERGE (le)-[:CONTAINED_MICROBIOLOGY_EVENT]->(me)
                        """
                        session.run(query_link_microevent,
                                  event_id=event_id,
                                  hadm_id=hadm_id_int,
                                  subject_id=subject_id_int,
                                  micro_specimen_id=micro_specimen_id_val,
                                  charttime=charttime_str)
                    else:
                        # When hadm_id is null, use only charttime-based identifier
                        first_charttime = microevents_for_event['charttime'].min().strftime('%Y-%m-%d %H:%M:%S')
                        query_microevent = """
                        MERGE (me:MicrobiologyEvent {
                            event_id: $event_id,
                            subject_id: $subject_id,
                            micro_specimen_id: $micro_specimen_id,
                            charttime: $charttime
                        })
                        SET me.micro_results = $micro_results,
                            me.micro_count = $micro_count,
                            me.name = $name
                        """
                        session.run(query_microevent,
                                  event_id=event_id,
                                  subject_id=subject_id_int,
                                  micro_specimen_id=micro_specimen_id_val,
                                  charttime=charttime_str,
                                  micro_results=micro_results,
                                  micro_count=len(micro_results),
                                  name="MicrobiologyEvent")
                        
                        # Link MicrobiologyEvent → LabEvents (without hadm_id, using charttime)
                        query_link_microevent = """
                        MATCH (le:LabEvents {event_id: $event_id, subject_id: $subject_id, charttime: $le_charttime})
                        MATCH (me:MicrobiologyEvent {event_id: $event_id, subject_id: $subject_id, 
                                                      micro_specimen_id: $micro_specimen_id, charttime: $charttime})
                        MERGE (le)-[:CONTAINED_MICROBIOLOGY_EVENT]->(me)
                        """
                        session.run(query_link_microevent,
                                  event_id=event_id,
                                  subject_id=subject_id_int,
                                  le_charttime=first_charttime,
                                  micro_specimen_id=micro_specimen_id_val,
                                  charttime=charttime_str)
                    
                    microevent_counter += 1
                
                logger.info(f"Added {len(microevents_for_event)} microbiology events for event {event_id}")
                
                # Track patient as processed
                processed_patients.add(subject_id_int)
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            # Log incremental load summary
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {processed_count} events, skipped {skipped_count} events with existing microbiology events")

        logger.info("All microbiology events processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_microbiology_nodes()

