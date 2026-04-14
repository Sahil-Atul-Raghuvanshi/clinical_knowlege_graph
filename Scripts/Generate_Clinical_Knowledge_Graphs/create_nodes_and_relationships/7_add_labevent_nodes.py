import pandas as pd
import logging
import os
import sys
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# Add Scripts directory to path for imports
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent.parent
sys.path.insert(0, str(scripts_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection
from utils.incremental_load_utils import IncrementalLoadChecker
from utils.etl_tracker import ETLTracker

# Configure logging - write only to file, not console (to keep progress bar clean)
project_root = script_dir.parent.parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

# Configure logger to only use file handler (no console output)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prevent propagation to root logger (which would print to console)
logger.propagate = False

def create_labevent_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_labevent_nodes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '7_add_labevent_nodes'

    # File paths (relative to script location)
    project_root = script_dir.parent.parent.parent
    LABEVENTS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'labevents.csv'
    LAB_LOOKUP_CSV = project_root / 'Filtered_Data' / 'hosp' / 'd_labitems.csv'

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    # Load CSVs
    labevents_df = pd.read_csv(str(LABEVENTS_CSV))
    labitems_lookup = pd.read_csv(str(LAB_LOOKUP_CSV))

    # Merge to add lab item details
    labevents_df = labevents_df.merge(labitems_lookup, on="itemid", how="left")
    
    # Convert charttime to datetime for proper sorting
    labevents_df['charttime'] = pd.to_datetime(labevents_df['charttime'])

    try:
        with neo4j_conn.session() as session:
            # Delete any existing cross-connections before processing
            logger.info("Checking for and deleting cross-connections...")
            
            # Delete HAS_PRESCRIPTIONS relationships from Lab nodes
            query1 = """
            MATCH (lab)-[r:HAS_PRESCRIPTIONS]->()
            WHERE (lab:LabEvents OR lab:LabEvent)
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
            WHERE (lab:LabEvents OR lab:LabEvent)
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
            WHERE (lab:LabEvents OR lab:LabEvent)
              AND (presc:Prescription OR presc:PrescriptionsBatch)
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
            WHERE (lab:LabEvents OR lab:LabEvent)
              AND (proc:Procedure OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result4 = session.run(query4)
            count4 = result4.single()["deleted_count"]
            if count4 > 0:
                logger.info(f"Deleted {count4} connections between LabEvents and Procedures")
            
            # Delete INCLUDED_LAB_EVENTS from Diagnosis nodes (should never exist)
            query5 = """
            MATCH (diag:Diagnosis)-[r:INCLUDED_LAB_EVENTS]->(lab)
            WHERE (lab:LabEvents OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result5 = session.run(query5)
            count5 = result5.single()["deleted_count"]
            if count5 > 0:
                logger.info(f"Deleted {count5} INCLUDED_LAB_EVENTS from Diagnosis nodes")
            
            total_deleted = count1 + count2 + count3 + count4 + count5
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
            
            # Check for existing lab events (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            events_with_labs = checker.get_events_with_lab_events()
            skipped_events = 0
            processed_events = []
            
            # Track processed patients for this script (per-patient, per-script tracking)
            # Use set to track which patients we've already marked in tracker (avoid duplicate tracking)
            patients_tracked_this_run = set()
            failed_patients = []
            skipped_patients = set()
            
            # Convert to list to get length for progress bar
            event_list = list(events)
            pbar = tqdm(total=len(event_list), desc="Adding lab event nodes", unit="event")
            
            for record in event_list:
                event_id = str(record["event_id"]).strip() if record["event_id"] is not None else None
                subject_id_raw = record["subject_id"]
                hadm_id_raw = record["hadm_id"]
                
                if event_id is None or subject_id_raw is None or hadm_id_raw is None:
                    logger.warning(f"Skipping event with missing IDs: event_id={event_id}, subject_id={subject_id_raw}, hadm_id={hadm_id_raw}")
                    pbar.update(1)
                    pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients)})
                    continue
                
                subject_id = str(subject_id_raw).strip()
                hadm_id = str(hadm_id_raw).strip()
                
                try:
                    subject_id_int = int(subject_id)
                    hadm_id_int = int(hadm_id)
                except ValueError:
                    logger.warning(f"Skipping event with invalid ID format: subject_id={subject_id}, hadm_id={hadm_id}")
                    pbar.update(1)
                    pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients)})
                    continue
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if event_id in events_with_labs:
                        skipped_events += 1
                        pbar.update(1)
                        pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if event already has lab events (incremental load)
                if event_id in events_with_labs:
                    skipped_events += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients)})
                    continue
                
                try:
                    processed_events.append(event_id)
                    
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
                        pbar.update(1)
                        pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients)})
                        continue

                    # Create LabEvents node (central node) and link it to the Event
                    # Only create INCLUDED_LAB_EVENTS from these specific node types
                    # Note: Transfer and Admit events are labeled as UnitAdmission (not Transfer or Admission)
                    query_labevents = """
                    MATCH (e {event_id:$event_id})
                    WHERE (e:UnitAdmission OR e:HospitalAdmission OR e:Discharge OR e:EmergencyDepartment OR e:ICUStay)
                    MERGE (le:LabEvents {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                    ON CREATE SET le.name = "LabEvents"
                    MERGE (e)-[:INCLUDED_LAB_EVENTS]->(le)
                    RETURN e, le
                    """
                    result = session.run(query_labevents, event_id=event_id, hadm_id=hadm_id_int, subject_id=subject_id_int)
                    record = result.single()
                    if record is None:
                        logger.warning(f"Could not find event node with event_id={event_id} (subject_id={subject_id_int}, hadm_id={hadm_id_int}). LabEvents node created but not connected to event.")
                    else:
                        event_node = record.get("e")
                        if event_node is None:
                            logger.warning(f"Event node not found for event_id={event_id}. LabEvents node may not be connected.")

                    # Group lab events by charttime to create LabEvent nodes
                    labevent_groups = labevents_for_event.groupby('charttime')
                    labevent_counter = 1
                    
                    for charttime, labevent_data in labevent_groups:
                        # Build lab_results array as formatted strings from all specimens at this charttime
                        # Format: "item_label=valuenum+valueuom (ref: lower-upper) [flag] fluid, category"
                        # Example: "PTT=40.6sec (ref: 25-35) [abnormal] Blood, Blood Gas"
                        lab_results = []
                        abnormal_results = []
                        for _, row in labevent_data.iterrows():
                            item_label = str(row["label"]) if pd.notna(row["label"]) else "Unknown"
                            valuenum = float(row["valuenum"]) if pd.notna(row["valuenum"]) else None
                            valueuom = str(row["valueuom"]) if pd.notna(row["valueuom"]) else ""
                            ref_lower = float(row["ref_range_lower"]) if pd.notna(row["ref_range_lower"]) else None
                            ref_upper = float(row["ref_range_upper"]) if pd.notna(row["ref_range_upper"]) else None
                            flag = str(row["flag"]) if pd.notna(row["flag"]) else None
                            fluid = str(row["fluid"]) if pd.notna(row["fluid"]) else ""
                            category = str(row["category"]) if pd.notna(row["category"]) else ""
                            
                            # Build main measurement part: "item_label=valuenum+valueuom"
                            if valuenum is not None:
                                measurement = f"{item_label}={valuenum}{valueuom}"
                            else:
                                measurement = f"{item_label}=N/A"
                            
                            # Build reference range part: "(ref: lower-upper)"
                            ref_part = ""
                            if ref_lower is not None and ref_upper is not None:
                                ref_part = f" (ref: {ref_lower}-{ref_upper})"
                            
                            # Build flag part: "[flag]"
                            flag_part = f" [{flag}]" if flag else ""
                            
                            # Build metadata part: "fluid, category"
                            metadata_parts = []
                            if fluid:
                                metadata_parts.append(fluid)
                            if category:
                                metadata_parts.append(category)
                            metadata_part = f" {', '.join(metadata_parts)}" if metadata_parts else ""
                            
                            # Combine all parts
                            lab_result_str = f"{measurement}{ref_part}{flag_part}{metadata_part}"
                            lab_results.append(lab_result_str)
                            
                            # Check if result is abnormal (outside reference range)
                            is_abnormal = False
                            if valuenum is not None and ref_lower is not None and ref_upper is not None:
                                if valuenum < ref_lower or valuenum > ref_upper:
                                    is_abnormal = True
                            
                            # Add to abnormal_results if it's abnormal
                            if is_abnormal:
                                abnormal_results.append(lab_result_str)
                        
                        # Create LabEvent node with aggregated lab results as array of strings
                        labevent_props = {
                            "event_id": event_id,
                            "hadm_id": hadm_id_int,
                            "subject_id": subject_id_int,
                            "charttime": charttime.strftime('%Y-%m-%d %H:%M:%S'),
                            "lab_results": lab_results,
                            "abnormal_results": abnormal_results,
                            "lab_count": len(lab_results),
                            "abnormal_count": len(abnormal_results),
                            "name": "LabEvent"
                        }
                        
                        query_labevent = """
                        MERGE (le:LabEvent {
                            event_id: $event_id,
                            hadm_id: $hadm_id,
                            subject_id: $subject_id,
                            charttime: $charttime
                        })
                        SET le.lab_results = $lab_results,
                            le.abnormal_results = $abnormal_results,
                            le.lab_count = $lab_count,
                            le.abnormal_count = $abnormal_count,
                            le.name = $name
                        """
                        session.run(query_labevent, **labevent_props)
                        
                        # Link LabEvent → LabEvents
                        query_link_labevent = """
                        MATCH (leb:LabEvents {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        MATCH (le:LabEvent {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id, charttime: $charttime})
                        MERGE (leb)-[:CONTAINED_LAB_EVENT]->(le)
                        """
                        session.run(query_link_labevent, event_id=event_id, hadm_id=hadm_id_int,
                                   subject_id=subject_id_int, charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'))
                        
                        labevent_counter += 1
                    
                    # Mark patient as processed immediately after successful processing (only once per patient per run)
                    if subject_id_int not in patients_tracked_this_run:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='success')
                                patients_tracked_this_run.add(subject_id_int)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id_int} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing event {event_id} for patient {subject_id_int}: {e}")
                    # Mark patient as failed immediately if we have subject_id
                    if subject_id_int not in failed_patients:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='failed')
                                failed_patients.append(subject_id_int)
                            except Exception as tracker_error:
                                logger.error(f"Error marking patient {subject_id_int} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': len(processed_events), 'Skipped': skipped_events, 'Failed': len(failed_patients), 'Tracked': len(patients_tracked_this_run)})
            
            pbar.close()
            
            # Log incremental load summary
            if skipped_events > 0:
                logger.info(f"Incremental load summary: Skipped {skipped_events} events that already have lab events")
            
            # Log summary
            if tracker and patients_tracked_this_run:
                logger.info(f"Successfully processed and tracked {len(patients_tracked_this_run)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")

        logger.info("All lab events processed successfully!")

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    create_labevent_nodes()
