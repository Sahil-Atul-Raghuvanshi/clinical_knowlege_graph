# add_chart_events.py
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

def create_chart_event_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # Check if ICUStay nodes exist
    with driver.session() as session:
        check_query = """
        MATCH (icu:ICUStay)
        RETURN count(icu) as count
        """
        result = session.run(check_query)
        icu_count = result.single()["count"]
        if icu_count == 0:
            logger.error("No ICUStay nodes found! Please run Scripts/10_add_icu_stays_label.py first.")
            return

    # File paths - dynamically constructed
    CHARTEVENTS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\chartevents.csv"
    D_ITEMS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\d_items.csv"

    try:
        # Load CSVs
        chartevents_df = pd.read_csv(CHARTEVENTS_CSV)
        d_items_df = pd.read_csv(D_ITEMS_CSV)

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
        with driver.session() as session:
            # Delete any existing chart event nodes and relationships
            logger.info("Cleaning up existing chart event nodes...")
            
            cleanup_query = """
            MATCH (ceb:ChartEventBatch)
            OPTIONAL MATCH (ceb)-[:HAS_CHART_EVENT]->(ce:ChartEvent)
            DETACH DELETE ce, ceb
            """
            session.run(cleanup_query)
            
            # Also clean up any orphaned ChartEvent nodes
            cleanup_orphans = """
            MATCH (ce:ChartEvent)
            WHERE NOT EXISTS((ce)<-[:HAS_CHART_EVENT]-(:ChartEventBatch))
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
                MERGE (icu)-[:HAS_CHART_EVENTS]->(ceb)
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
                    MERGE (ceb)-[:HAS_CHART_EVENT]->(ce)
                    """
                    session.run(query_link_chartevent, 
                              event_id=event_id, 
                              charttime=charttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    chartevent_counter += 1
                
                logger.info(f"Added {len(chartevents_for_stay)} chart events in {chartevent_counter - 1} time groups for ICU stay {event_id}")

        logger.info("All chart events processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()

    finally:
        driver.close()


if __name__ == "__main__":
    create_chart_event_nodes()