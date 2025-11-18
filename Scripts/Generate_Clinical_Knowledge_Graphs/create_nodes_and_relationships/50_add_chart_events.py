# add_chart_events.py
import pandas as pd
import logging
import os
import sys
from pathlib import Path
from typing import Optional

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

def create_chart_event_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_chart_events.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '50_add_chart_events'

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    # Check if ICUStay nodes exist
    with neo4j_conn.session() as session:
        check_query = """
        MATCH (icu:ICUStay)
        RETURN count(icu) as count
        """
        result = session.run(check_query)
        icu_count = result.single()["count"]
        if icu_count == 0:
            logger.error("No ICUStay nodes found! Please run Scripts/10_add_icu_stays_label.py first.")
            neo4j_conn.close()
            return

    # File paths (relative to script location)
    project_root = script_dir.parent.parent.parent
    CHARTEVENTS_CSV = project_root / 'Filtered_Data' / 'icu' / 'chartevents.csv'
    D_ITEMS_CSV = project_root / 'Filtered_Data' / 'icu' / 'd_items.csv'

    try:
        # Load CSVs
        chartevents_df = pd.read_csv(str(CHARTEVENTS_CSV))
        d_items_df = pd.read_csv(str(D_ITEMS_CSV))

        # Merge to add item details (label, param_type, lownormalvalue, highnormalvalue, unitname)
        chartevents_df = chartevents_df.merge(
            d_items_df[['itemid', 'label', 'param_type', 'lownormalvalue', 'highnormalvalue', 'unitname']], 
            on='itemid', 
            how='left'
        )
        
        # Convert charttime to datetime for proper sorting
        chartevents_df['charttime'] = pd.to_datetime(chartevents_df['charttime'])
        
        logger.info(f"Loaded {len(chartevents_df)} chart events")

    except FileNotFoundError as e:
        logger.error(f"Required file not found: {e}")
        return
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return

    try:
        with neo4j_conn.session() as session:
            # Check for existing chart events (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            icustays_with_chart_events = set()
            
            # Get ICU stays that already have chart events
            query_existing = """
            MATCH (icu:ICUStay)-[:RECORDED_CHART_EVENTS]->(ceb:ChartEventBatch)
            RETURN DISTINCT icu.event_id AS event_id
            """
            result = session.run(query_existing)
            icustays_with_chart_events = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(icustays_with_chart_events)} ICU stays with existing chart events")
            
            # For incremental load: Don't delete existing chart events
            # We'll skip ICU stays that already have chart events
            # Only clean up orphaned nodes (nodes without proper relationships)
            if icustays_with_chart_events:
                logger.info(f"Found {len(icustays_with_chart_events)} ICU stays with existing chart events - will skip (incremental load)")
            else:
                logger.info("No existing chart events found. Starting fresh.")
            
            # Clean up any orphaned ChartEvent nodes (nodes without proper batch relationships)
            cleanup_orphans = """
            MATCH (ce:ChartEvent)
            WHERE NOT EXISTS((ce)<-[:CONTAINED_CHART_EVENT]-(:ChartEventBatch))
            DETACH DELETE ce
            """
            session.run(cleanup_orphans)
            
            # Fetch ONLY ICUStay nodes
            query_icu_stays = """
            MATCH (icu:ICUStay)
            WHERE icu.intime IS NOT NULL AND icu.outtime IS NOT NULL
            RETURN icu.event_id AS event_id, 
                   icu.subject_id AS subject_id, 
                   icu.hadm_id AS hadm_id,
                   icu.intime AS intime, 
                   icu.outtime AS outtime
            """
            icu_stays = session.run(query_icu_stays)
            logger.info("Processing chart events for ICU stays only")
            
            skipped_count = 0
            processed_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            # Use set to track which patients we've already marked in tracker (avoid duplicate tracking)
            patients_tracked_this_run = set()
            failed_patients = []
            skipped_patients = set()

            for record in icu_stays:
                event_id = str(record["event_id"]).strip() if record["event_id"] is not None else None
                subject_id_raw = record["subject_id"]
                hadm_id_raw = record["hadm_id"]
                
                if event_id is None or subject_id_raw is None:
                    continue
                
                subject_id = str(subject_id_raw).strip()
                hadm_id = str(hadm_id_raw).strip() if hadm_id_raw is not None else None
                
                try:
                    subject_id_int = int(subject_id)
                    hadm_id_int = int(hadm_id) if hadm_id is not None else None
                except ValueError:
                    logger.warning(f"Skipping ICU stay with invalid ID format: subject_id={subject_id}, hadm_id={hadm_id}")
                    continue
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if event_id in icustays_with_chart_events:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 50 == 0:
                            logger.info(f"Skipping ICU stay {event_id} - patient already processed (tracker). Total skipped: {skipped_count}")
                        continue
                
                # Skip if ICU stay already has chart events (incremental load)
                if event_id in icustays_with_chart_events:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 50 == 0:
                        logger.info(f"Skipping ICU stay {event_id} - already has chart events (incremental load). Total skipped: {skipped_count}")
                    continue
                
                processed_count += 1
                try:
                    intime = pd.to_datetime(record["intime"])
                    outtime = pd.to_datetime(record["outtime"])

                    # Filter chart events for this ICU stay
                    chartevents_for_stay = chartevents_df[
                        (chartevents_df["subject_id"] == subject_id_int) &
                        (chartevents_df["hadm_id"] == hadm_id_int) &
                        (chartevents_df["charttime"] >= intime) &
                        (chartevents_df["charttime"] <= outtime)
                    ].sort_values(by=["charttime"])

                    if chartevents_for_stay.empty:
                        continue

                    # Create ChartEventBatch node and link it ONLY to ICUStay
                    query_batch = """
                    MATCH (icu:ICUStay {event_id: $event_id})
                    MERGE (ceb:ChartEventBatch {event_id: $event_id})
                    ON CREATE SET 
                        ceb.name = "ChartEvents",
                        ceb.hadm_id = $hadm_id,
                        ceb.subject_id = $subject_id
                    MERGE (icu)-[:RECORDED_CHART_EVENTS]->(ceb)
                    """
                    session.run(query_batch, 
                              event_id=event_id, 
                              hadm_id=hadm_id_int, 
                              subject_id=subject_id_int)

                    # Group chart events by charttime
                    chartevent_groups = chartevents_for_stay.groupby('charttime')
                    chartevent_counter = 1
                    
                    for charttime, chartevent_data in chartevent_groups:
                        # Build array of chart measurements
                        chart_measurements = []
                        
                        for _, row in chartevent_data.iterrows():
                            label = str(row["label"]) if pd.notna(row["label"]) else "Unknown"
                            value = row["value"] if pd.notna(row["value"]) else None
                            valuenum = row["valuenum"] if pd.notna(row["valuenum"]) else None
                            valueuom = row.get("valueuom")
                            if pd.isna(valueuom):
                                valueuom = row.get("unitname")  # Fallback to unitname from d_items
                            valueuom = str(valueuom) if pd.notna(valueuom) else ""
                            
                            param_type = str(row["param_type"]) if pd.notna(row["param_type"]) else ""
                            lownormal = row["lownormalvalue"] if pd.notna(row["lownormalvalue"]) else None
                            highnormal = row["highnormalvalue"] if pd.notna(row["highnormalvalue"]) else None
                            
                            # Build measurement string
                            if valuenum is not None:
                                measurement = f"{label} with value {valuenum}{valueuom}"
                            elif value is not None:
                                measurement = f"{label} with value {value}{valueuom}"
                            else:
                                measurement = f"{label} with value N/A"
                            
                            # Add normal range only for numeric measurements
                            if param_type == "Numeric" and lownormal is not None and highnormal is not None:
                                measurement += f" (normal range: {lownormal}-{highnormal})"
                            
                            chart_measurements.append(measurement)
                        
                        # Create ChartEvent node with array of measurements
                        chartevent_props = {
                            "event_id": event_id,
                            "hadm_id": hadm_id_int,
                            "subject_id": subject_id_int,
                            "charttime": charttime.strftime('%Y-%m-%d %H:%M:%S'),
                            "chart_measurements": chart_measurements,
                            "measurement_count": len(chart_measurements),
                            "name": "ChartEvent"
                        }
                        
                        query_chartevent = """
                        MERGE (ce:ChartEvent {
                            event_id: $event_id,
                            charttime: $charttime
                        })
                        SET ce.chart_measurements = $chart_measurements,
                            ce.measurement_count = $measurement_count,
                            ce.name = $name,
                            ce.hadm_id = $hadm_id,
                            ce.subject_id = $subject_id
                        """
                        session.run(query_chartevent, **chartevent_props)
                        
                        # Link ChartEvent → ChartEventBatch
                        query_link_chartevent = """
                        MATCH (ceb:ChartEventBatch {event_id: $event_id})
                        MATCH (ce:ChartEvent {event_id: $event_id, charttime: $charttime})
                        MERGE (ceb)-[:CONTAINED_CHART_EVENT]->(ce)
                        """
                        session.run(query_link_chartevent, 
                                  event_id=event_id, 
                                  charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'))
                        
                        chartevent_counter += 1
                    
                    # Mark patient as processed immediately after successful processing (only once per patient per run)
                    if subject_id_int not in patients_tracked_this_run:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='success')
                                patients_tracked_this_run.add(subject_id_int)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id_int} as processed in tracker: {e}")
                    
                    logger.info(f"Added {len(chartevents_for_stay)} chart events in {chartevent_counter - 1} time groups for ICU stay {event_id}")
                except Exception as e:
                    logger.error(f"Error processing ICU stay {event_id} for patient {subject_id_int}: {e}")
                    # Mark patient as failed immediately
                    if subject_id_int not in failed_patients:
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='failed')
                                failed_patients.append(subject_id_int)
                            except Exception as tracker_error:
                                logger.error(f"Error marking patient {subject_id_int} as failed in tracker: {tracker_error}")
            
            # Log incremental load summary
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {processed_count} ICU stays, skipped {skipped_count} ICU stays with existing chart events")
            
            # Log summary
            if tracker and patients_tracked_this_run:
                logger.info(f"Successfully processed and tracked {len(patients_tracked_this_run)} patients in tracker for script '{SCRIPT_NAME}'")
            if failed_patients:
                logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")

        logger.info("All chart events processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    create_chart_event_nodes()