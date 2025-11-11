# patient_flow_through_the_hospital.py
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


def event_label(event_type):
    if event_type == "ed":
        return "EmergencyDepartment"
    elif event_type == "discharge":
        return "Discharge"
    elif event_type == "admit":
        return "UnitAdmission"
    elif event_type == "transfer":
        return "UnitAdmission"  # Transfer events should also be UnitAdmission nodes
    return event_type.capitalize()

def event_relationship(prev_type, next_type):
    """Generate relationship name based on next event type"""
    if next_type is None:
        next_type = "unknown"
    
    # Map event types to appropriate relationship names
    relationship_map = {
        "discharge": "LED_TO_DISCHARGE",
        "transfer": "LED_TO_UNIT_ADMISSION",  # Transfer events create UnitAdmission nodes
        "admit": "LED_TO_UNIT_ADMISSION",
        "ed": "LED_TO_ED"
    }
    
    return relationship_map.get(next_type.lower(), f"LED_TO_{next_type.upper()}")

def human_readable_period(intime, outtime):
    if pd.notna(intime) and pd.notna(outtime):
        delta = outtime - intime
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{days} days {hours} hours {minutes} minutes"
    return None

def calculate_gap(prev_time, next_time):
    """Calculate gap between discharge and next admission (or event)"""
    if pd.notna(prev_time) and pd.notna(next_time):
        delta = next_time - prev_time
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{days} days {hours} hours {minutes} minutes"
    return None

def process_ed_stays(session, edstays_df, transfers_df, subject_id):
    """
    Process ED stays from edstays.csv for a patient.
    This includes both standalone ED visits and those linked to hospital admissions.
    
    Note: If edstays.csv is empty, ED data will come from transfers.csv instead,
    which is handled in the main patient flow logic.
    """
    # Filter ED stays for this patient
    patient_edstays = edstays_df[edstays_df["subject_id"] == subject_id].copy()
    patient_edstays = patient_edstays.sort_values(by="intime").reset_index(drop=True)
    
    logger.info(f"Found {len(patient_edstays)} ED stay(s) in edstays.csv for subject {subject_id}")
    
    for seq_num, (_, stay) in enumerate(patient_edstays.iterrows(), start=1):
        stay_id = str(stay["stay_id"])
        # Handle nullable Int64 type properly
        hadm_id = int(stay["hadm_id"]) if pd.notna(stay["hadm_id"]) else None
        intime = stay["intime"]
        outtime = stay["outtime"]
        
        # Format times
        intime_str = intime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(intime) else None
        outtime_str = outtime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(outtime) else None
        period = human_readable_period(intime, outtime)
        
        # Create ED node
        query_ed = """
        MERGE (ed:EmergencyDepartment {event_id: $stay_id})
        ON CREATE SET 
            ed.name = 'Emergency Department',
            ed.subject_id = $subject_id,
            ed.hadm_id = $hadm_id,
            ed.intime = $intime,
            ed.outtime = $outtime,
            ed.period = $period,
            ed.disposition = $disposition,
            ed.arrival_transport = $arrival_transport,
            ed.ed_seq_num = $ed_seq_num
        ON MATCH SET
            ed.name = 'Emergency Department',
            ed.subject_id = $subject_id,
            ed.hadm_id = $hadm_id,
            ed.intime = $intime,
            ed.outtime = $outtime,
            ed.period = $period,
            ed.disposition = $disposition,
            ed.arrival_transport = $arrival_transport,
            ed.ed_seq_num = $ed_seq_num
        """
        session.run(query_ed,
                   stay_id=stay_id,
                   subject_id=int(subject_id),
                   hadm_id=hadm_id,
                   intime=intime_str,
                   outtime=outtime_str,
                   period=period,
                   disposition=stay["disposition"] if pd.notna(stay["disposition"]) else None,
                   arrival_transport=stay["arrival_transport"] if pd.notna(stay["arrival_transport"]) else None,
                   ed_seq_num=seq_num)
        
        # Always link Patient to ED (both standalone and admission-linked)
        query_patient = """
        MATCH (p:Patient {subject_id: $subject_id})
        MATCH (ed:EmergencyDepartment {event_id: $stay_id})
        MERGE (p)-[:VISITED_ED]->(ed)
        """
        session.run(query_patient, subject_id=int(subject_id), stay_id=stay_id)
        
        if pd.isna(hadm_id):
            logger.info(f"Processed standalone ED visit {stay_id} (Seq {seq_num}) for subject {subject_id}")
        else:
            logger.info(f"Processed ED visit {stay_id} (Seq {seq_num}) with hospital admission {hadm_id} for subject {subject_id}")

def create_patient_flow(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '2_patient_flow_through_the_hospital'

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    ADMISSIONS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'admissions.csv')
    TRANSFERS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'transfers.csv')
    SERVICES_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'services.csv')
    EDSTAYS_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'edstays.csv')

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load data
        admissions_df = pd.read_csv(ADMISSIONS_CSV)
        admissions_df["admittime"] = pd.to_datetime(admissions_df["admittime"], errors="coerce")
        admissions_df["dischtime"] = pd.to_datetime(admissions_df["dischtime"], errors="coerce")

        transfers_df = pd.read_csv(TRANSFERS_CSV)
        transfers_df["intime"] = pd.to_datetime(transfers_df["intime"], errors="coerce")
        transfers_df["outtime"] = pd.to_datetime(transfers_df["outtime"], errors="coerce")

        # Load services data
        services_df = pd.read_csv(SERVICES_CSV)
        services_df["transfertime"] = pd.to_datetime(services_df["transfertime"], errors="coerce")
        
        # Convert data types for services_df to match other dataframes
        services_df["subject_id"] = services_df["subject_id"].astype(int)
        services_df["hadm_id"] = services_df["hadm_id"].astype('Int64')

        # Load ED stays data
        edstays_df = pd.read_csv(EDSTAYS_CSV)
        edstays_df["intime"] = pd.to_datetime(edstays_df["intime"], errors="coerce")
        edstays_df["outtime"] = pd.to_datetime(edstays_df["outtime"], errors="coerce")
        
        # Ensure consistent data types for filtering and comparisons
        # Convert subject_id to int for all dataframes
        edstays_df["subject_id"] = edstays_df["subject_id"].astype(int)
        admissions_df["subject_id"] = admissions_df["subject_id"].astype(int)
        transfers_df["subject_id"] = transfers_df["subject_id"].astype(int)
        
        # Convert hadm_id to nullable Int64 to handle NaN values while maintaining integer type
        # This ensures proper comparisons between dataframes
        edstays_df["hadm_id"] = edstays_df["hadm_id"].astype('Int64')
        admissions_df["hadm_id"] = admissions_df["hadm_id"].astype('Int64')
        transfers_df["hadm_id"] = transfers_df["hadm_id"].astype('Int64')
        
        # Merge services with transfers based on subject_id, hadm_id, and matching times
        transfers_df = transfers_df.merge(
            services_df[["subject_id", "hadm_id", "transfertime", "curr_service"]],
            left_on=["subject_id", "hadm_id", "intime"],
            right_on=["subject_id", "hadm_id", "transfertime"],
            how="left"
        )
        # Rename curr_service to service_given
        transfers_df.rename(columns={"curr_service": "service_given"}, inplace=True)
        # Drop the extra transfertime column
        if "transfertime" in transfers_df.columns:
            transfers_df.drop(columns=["transfertime"], inplace=True)

        # Sort transfers
        transfers_df = transfers_df.sort_values(by=["subject_id", "hadm_id", "intime"]).reset_index(drop=True)

        with driver.session() as session:
            # Get unique subject_ids from both admissions and ED stays
            all_subjects = pd.concat([
                admissions_df["subject_id"],
                edstays_df["subject_id"]
            ]).unique()

            # Check for existing patients with complete graphs (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            skipped_count = 0
            processed_count = 0
            processed_patients = []
            patients_with_complete_graphs = set()  # Track all patients with complete graphs for batch sync
            
            for subject_id in all_subjects:
                subject_id_int = int(subject_id)
                
                # Check tracker first (faster than database query)
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping patient {subject_id} - already processed (tracker). Total skipped: {skipped_count}")
                    continue
                
                # Skip if patient already has complete graph
                if checker.patient_has_complete_graph(subject_id_int):
                    skipped_count += 1
                    patients_with_complete_graphs.add(subject_id_int)
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping patient {subject_id} - already has complete graph (incremental load). Total skipped: {skipped_count}")
                    continue
                
                processed_count += 1
                processed_patients.append(subject_id_int)
                # Process ED stays first (both standalone and admission-linked)
                process_ed_stays(session, edstays_df, transfers_df, subject_id)

                # Process hospital admissions if they exist
                patient_admissions = admissions_df[admissions_df["subject_id"] == subject_id]
                if patient_admissions.empty:
                    continue

                # Sort admissions chronologically
                patient_admissions_sorted = patient_admissions.sort_values(by="admittime").reset_index(drop=True)
                total_admissions = len(patient_admissions_sorted)

                # Update patient node with total admissions
                query_total_adm = """
                MATCH (p:Patient {subject_id: $subject_id})
                SET p.total_number_of_admissions = $total_admissions
                """
                session.run(query_total_adm, subject_id=int(subject_id), total_admissions=total_admissions)

                patient_transfers = transfers_df[transfers_df["subject_id"] == subject_id]
                
                # Get all ED stay_ids from edstays.csv for this patient to avoid duplicates
                patient_edstays_ids = set(edstays_df[edstays_df["subject_id"] == subject_id]["stay_id"].astype(str))
                
                # Track ED sequence numbers across all admissions for this patient
                # Start with count from edstays if available
                ed_seq_counter = len(patient_edstays_ids) + 1
                
                # Pre-assign ED sequence numbers to all ED events from transfers for this patient
                # EXCLUDE ED events that already exist in edstays.csv (same transfer_id = stay_id)
                patient_ed_transfers = patient_transfers[patient_transfers["eventtype"].str.lower() == "ed"].copy()
                patient_ed_transfers = patient_ed_transfers.sort_values(by="intime").reset_index(drop=True)
                
                # Create a mapping of transfer_id to ed_seq_num
                # Skip ED events that were already processed from edstays.csv
                ed_transfer_seq_map = {}
                for idx, ed_row in patient_ed_transfers.iterrows():
                    transfer_id = str(int(ed_row["transfer_id"]))
                    # Skip if this ED event was already created from edstays.csv
                    if transfer_id in patient_edstays_ids:
                        continue
                    ed_transfer_seq_map[transfer_id] = ed_seq_counter
                    ed_seq_counter += 1

                for adm_seq_num, (_, adm_row) in enumerate(patient_admissions_sorted.iterrows(), start=1):
                    hadm_id = adm_row["hadm_id"]
                    adm_intime = adm_row["admittime"]
                    adm_outtime = adm_row["dischtime"]

                    adm_intime_str = adm_intime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(adm_intime) else None
                    adm_outtime_str = adm_outtime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(adm_outtime) else None

                    # Handle optional times
                    deathtime = adm_row.get("deathtime")
                    deathtime_str = pd.to_datetime(deathtime).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(deathtime) else None

                    edregtime = adm_row.get("edregtime")
                    edregtime_str = pd.to_datetime(edregtime).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(edregtime) else None

                    edouttime = adm_row.get("edouttime")
                    edouttime_str = pd.to_datetime(edouttime).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(edouttime) else None

                    # Create HospitalAdmission node with sequence number
                    query = """
                    MERGE (h:HospitalAdmission {hadm_id: $hadm_id})
                    ON CREATE SET 
                        h.name = $name,
                        h.subject_id = $subject_id,
                        h.admittime = $admittime,
                        h.dischtime = $dischtime,
                        h.deathtime = $deathtime,
                        h.admission_type = $admission_type,
                        h.admit_provider_id = $admit_provider_id,
                        h.admission_location = $admission_location,
                        h.discharge_location = $discharge_location,
                        h.insurance = $insurance,
                        h.language = $language,
                        h.marital_status = $marital_status,
                        h.race = $race,
                        h.edregtime = $edregtime,
                        h.edouttime = $edouttime,
                        h.hospital_expire_flag = $hospital_expire_flag,
                        h.hospital_admission_sequence_number = $seq_num
                    ON MATCH SET
                        h.name = $name,
                        h.deathtime = $deathtime,
                        h.admission_type = $admission_type,
                        h.admit_provider_id = $admit_provider_id,
                        h.admission_location = $admission_location,
                        h.discharge_location = $discharge_location,
                        h.insurance = $insurance,
                        h.language = $language,
                        h.marital_status = $marital_status,
                        h.race = $race,
                        h.edregtime = $edregtime,
                        h.edouttime = $edouttime,
                        h.hospital_expire_flag = $hospital_expire_flag,
                        h.hospital_admission_sequence_number = $seq_num
                    """
                    session.run(query,
                                hadm_id=hadm_id,
                                name='HospitalAdmission',
                                subject_id=subject_id,
                                admittime=adm_intime_str,
                                dischtime=adm_outtime_str,
                                deathtime=deathtime_str,
                                admission_type=adm_row.get("admission_type"),
                                admit_provider_id=adm_row.get("admit_provider_id"),
                                admission_location=adm_row.get("admission_location"),
                                discharge_location=adm_row.get("discharge_location"),
                                insurance=adm_row.get("insurance"),
                                language=adm_row.get("language"),
                                marital_status=adm_row.get("marital_status"),
                                race=adm_row.get("race"),
                                edregtime=edregtime_str,
                                edouttime=edouttime_str,
                                hospital_expire_flag=adm_row.get("hospital_expire_flag"),
                                seq_num=adm_seq_num)  # sequence starts at 1

                    # Handle transfers
                    admission_transfers = patient_transfers[patient_transfers["hadm_id"] == hadm_id]
                    admission_transfers = admission_transfers.sort_values(by="intime").reset_index(drop=True)

                    previous_event_id = None
                    previous_event_type = None

                    for _, row in admission_transfers.iterrows():
                        event_id = str(int(row["transfer_id"]))
                        event_type = str(row["eventtype"]).lower() if pd.notna(row["eventtype"]) else "unknown"
                        careunit = row["careunit"] if pd.notna(row["careunit"]) else "Unknown"
                        intime = row["intime"]
                        outtime = row["outtime"]
                        service_given = row["service_given"] if pd.notna(row.get("service_given")) else None

                        intime_str = intime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(intime) else None
                        outtime_str = outtime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(outtime) else None
                        period = human_readable_period(intime, outtime)
                        label = event_label(event_type)
                        
                        # Set name based on event type
                        if event_type == "discharge":
                            name_value = "Discharge"  # Always use "Discharge" for discharge nodes
                        elif event_type in ["admit", "transfer"]:
                            name_value = careunit  # Use careunit for UnitAdmission nodes
                        else:
                            name_value = label  # Default to label for other types

                        # For ED nodes, include ed_seq_num
                        if event_type == "ed":
                            # Skip if this ED event was already created from edstays.csv
                            if event_id in patient_edstays_ids:
                                # This ED visit was already processed from edstays.csv, skip to avoid overwriting ed_seq_num
                                pass
                            else:
                                # Get pre-assigned sequence number from map
                                ed_seq_num = ed_transfer_seq_map.get(event_id, None)
                                
                                query_node = f"""
                                MERGE (e:{label} {{event_id: $event_id}})
                                ON CREATE SET 
                                    e.name = $name_value,
                                    e.subject_id = $subject_id,
                                    e.hadm_id = $hadm_id,
                                    e.transfer_id = $transfer_id,
                                    e.eventtype = $eventtype,
                                    e.careunit = $careunit,
                                    e.intime = $intime,
                                    e.outtime = $outtime,
                                    e.period = $period,
                                    e.service_given = $service_given,
                                    e.ed_seq_num = $ed_seq_num
                                ON MATCH SET
                                    e.name = $name_value,
                                    e.subject_id = $subject_id,
                                    e.hadm_id = $hadm_id,
                                    e.transfer_id = $transfer_id,
                                    e.eventtype = $eventtype,
                                    e.careunit = $careunit,
                                    e.intime = $intime,
                                    e.outtime = $outtime,
                                    e.period = $period,
                                    e.service_given = $service_given,
                                    e.ed_seq_num = $ed_seq_num
                                """
                                session.run(query_node,
                                            event_id=event_id,
                                            name_value=name_value,
                                            subject_id=int(subject_id),
                                            hadm_id=hadm_id,
                                            transfer_id=int(row["transfer_id"]),
                                            eventtype=event_type,
                                            careunit=careunit,
                                            intime=intime_str,
                                            outtime=outtime_str,
                                            period=period,
                                            service_given=service_given,
                                            ed_seq_num=ed_seq_num)
                        else:
                            query_node = f"""
                            MERGE (e:{label} {{event_id: $event_id}})
                            ON CREATE SET 
                                e.name = $name_value,
                                e.subject_id = $subject_id,
                                e.hadm_id = $hadm_id,
                                e.transfer_id = $transfer_id,
                                e.eventtype = $eventtype,
                                e.careunit = $careunit,
                                e.intime = $intime,
                                e.outtime = $outtime,
                                e.period = $period,
                                e.service_given = $service_given
                            ON MATCH SET
                                e.name = $name_value,
                                e.subject_id = $subject_id,
                                e.hadm_id = $hadm_id,
                                e.transfer_id = $transfer_id,
                                e.eventtype = $eventtype,
                                e.careunit = $careunit,
                                e.intime = $intime,
                                e.outtime = $outtime,
                                e.period = $period,
                                e.service_given = $service_given
                            """
                            session.run(query_node,
                                        event_id=event_id,
                                        name_value=name_value,
                                        subject_id=int(subject_id),
                                        hadm_id=hadm_id,
                                        transfer_id=int(row["transfer_id"]),
                                        eventtype=event_type,
                                        careunit=careunit,
                                        intime=intime_str,
                                        outtime=outtime_str,
                                        period=period,
                                        service_given=service_given)

                        if previous_event_id:
                            rel_next = event_relationship(previous_event_type, event_type)
                            query_flow = f"""
                            MATCH (e1 {{event_id: $prev_id}})
                            MATCH (e2 {{event_id: $curr_id}})
                            MERGE (e1)-[:{rel_next}]->(e2)
                            """
                            session.run(query_flow,
                                        prev_id=previous_event_id,
                                        curr_id=event_id)

                        previous_event_id = event_id
                        previous_event_type = event_type

                    # ====================================================================
                    # CRITICAL: Find the first event and connect Patient and HospitalAdmission
                    # ====================================================================
                    # The first event may be ED, admit, or transfer - not always ED!
                    # We need to:
                    # 1. Find the chronologically first event (ED, admit, or transfer)
                    # 2. Connect Patient to the first event
                    # 3. Connect HospitalAdmission to the first unit admission if timing allows
                    
                    # Collect all possible first events (ED from edstays, ED from transfers, admit, transfer)
                    first_events = []
                    
                    # 1. Check ED from edstays.csv for this admission
                    patient_ed = edstays_df[
                        (edstays_df["subject_id"] == subject_id) &
                        (edstays_df["hadm_id"] == hadm_id) &
                        (edstays_df["hadm_id"].notna())
                    ]
                    for _, ed_row in patient_ed.iterrows():
                        if pd.notna(ed_row["intime"]):
                            first_events.append({
                                'event_id': str(ed_row["stay_id"]),
                                'event_type': 'ed',
                                'label': 'EmergencyDepartment',
                                'intime': ed_row["intime"],
                                'outtime': ed_row["outtime"],
                                'source': 'edstays'
                            })
                    
                    # 2. Check ED from transfers.csv for this admission
                    ed_transfers = admission_transfers[admission_transfers["eventtype"].str.lower() == "ed"]
                    for _, ed_transfer in ed_transfers.iterrows():
                        ed_event_id = str(int(ed_transfer["transfer_id"]))
                        # Skip if already processed from edstays.csv
                        if ed_event_id in patient_edstays_ids:
                            continue
                        if pd.notna(ed_transfer["intime"]):
                            first_events.append({
                                'event_id': ed_event_id,
                                'event_type': 'ed',
                                'label': 'EmergencyDepartment',
                                'intime': ed_transfer["intime"],
                                'outtime': ed_transfer["outtime"],
                                'source': 'transfers'
                            })
                    
                    # 3. Check admit and transfer events (UnitAdmissions)
                    unit_admissions = admission_transfers[
                        admission_transfers["eventtype"].str.lower().isin(["admit", "transfer"])
                    ]
                    for _, unit_row in unit_admissions.iterrows():
                        if pd.notna(unit_row["intime"]):
                            first_events.append({
                                'event_id': str(int(unit_row["transfer_id"])),
                                'event_type': unit_row["eventtype"].lower(),
                                'label': 'UnitAdmission',
                                'intime': unit_row["intime"],
                                'outtime': unit_row["outtime"],
                                'source': 'transfers'
                            })
                    
                    # Sort all events by intime to find the first one
                    if first_events:
                        first_events_df = pd.DataFrame(first_events)
                        first_events_df = first_events_df.sort_values(by="intime").reset_index(drop=True)
                        first_event = first_events_df.iloc[0]
                        first_event_id = first_event["event_id"]
                        first_event_type = first_event["event_type"]
                        first_event_label = first_event["label"]
                        first_event_intime = first_event["intime"]
                        first_event_outtime = first_event["outtime"]
                        
                        logger.info(f"First event for admission {hadm_id}: {first_event_type} (event_id: {first_event_id}, intime: {first_event_intime})")
                        
                        # Connect Patient to the first event
                        if first_event_type == "ed":
                            # Connect Patient to ED
                            query_patient_first = """
                            MATCH (p:Patient {subject_id: $subject_id})
                            MATCH (e:EmergencyDepartment {event_id: $event_id})
                            MERGE (p)-[:VISITED_ED]->(e)
                            """
                            session.run(query_patient_first, subject_id=int(subject_id), event_id=first_event_id)
                            logger.info(f"Connected Patient {subject_id} to first event: ED {first_event_id}")
                        elif first_event_type in ["admit", "transfer"]:
                            # Connect Patient to UnitAdmission
                            query_patient_first = """
                            MATCH (p:Patient {subject_id: $subject_id})
                            MATCH (e:UnitAdmission {event_id: $event_id})
                            MERGE (p)-[:ADMITTED_TO_UNIT]->(e)
                            """
                            session.run(query_patient_first, subject_id=int(subject_id), event_id=first_event_id)
                            logger.info(f"Connected Patient {subject_id} to first event: UnitAdmission {first_event_id}")
                        
                        # Connect HospitalAdmission to the first unit admission (if first event is a unit admission)
                        # OR connect ED to HospitalAdmission if first event is ED
                        if first_event_type in ["admit", "transfer"]:
                            # First event is a unit admission
                            # Check if hospital admission time is within or before the first unit admission's time range
                            if pd.notna(adm_intime) and pd.notna(first_event_intime):
                                # Determine relationship based on timing
                                if adm_intime <= first_event_intime:
                                    # Hospital admission happened before or at the same time as first unit admission
                                    relationship = "LED_TO_FIRST_UNIT_ADMISSION"
                                    logger.info(f"Hospital admission {hadm_id} (time: {adm_intime}) led to first unit admission {first_event_id} (time: {first_event_intime})")
                                elif pd.notna(first_event_outtime) and adm_intime <= first_event_outtime:
                                    # Hospital admission happened during the first unit admission
                                    relationship = "LED_TO_FIRST_UNIT_ADMISSION"
                                    logger.info(f"Hospital admission {hadm_id} (time: {adm_intime}) occurred during first unit admission {first_event_id} (time: {first_event_intime} to {first_event_outtime})")
                                else:
                                    # Hospital admission happened after first unit admission (unusual but possible)
                                    relationship = "LED_TO_FIRST_UNIT_ADMISSION"
                                    logger.warning(f"Hospital admission {hadm_id} (time: {adm_intime}) occurred after first unit admission {first_event_id} (time: {first_event_intime})")
                                
                                query_hosp_to_unit = f"""
                                MATCH (h:HospitalAdmission {{hadm_id: $hadm_id}})
                                MATCH (u:UnitAdmission {{event_id: $event_id}})
                                MERGE (h)-[:{relationship}]->(u)
                                """
                                session.run(query_hosp_to_unit, hadm_id=hadm_id, event_id=first_event_id)
                                logger.info(f"Connected HospitalAdmission {hadm_id} to first UnitAdmission {first_event_id}")
                        elif first_event_type == "ed":
                            # First event is ED - connect ED to HospitalAdmission
                            if pd.notna(adm_intime) and pd.notna(first_event_intime):
                                if pd.notna(first_event_outtime):
                                    if adm_intime >= first_event_intime and adm_intime <= first_event_outtime:
                                        relationship = "LED_TO_ADMISSION_DURING_STAY"
                                        logger.info(f"Admission {hadm_id} occurred during ED stay {first_event_id}")
                                    elif adm_intime > first_event_outtime:
                                        relationship = "LED_TO_ADMISSION_AFTER_DISCHARGE"
                                        logger.info(f"Admission {hadm_id} occurred after ED discharge from {first_event_id}")
                                    else:
                                        relationship = "LED_TO_ADMISSION"
                                        logger.info(f"Admission {hadm_id} has unusual timing with ED {first_event_id}")
                                else:
                                    relationship = "LED_TO_ADMISSION"
                                    logger.info(f"Admission {hadm_id} linked to ED {first_event_id} (no outtime for ED)")
                                
                                query_ed_to_admission = f"""
                                MATCH (ed:EmergencyDepartment {{event_id: $ed_event_id}})
                                MATCH (h:HospitalAdmission {{hadm_id: $hadm_id}})
                                MERGE (ed)-[:{relationship}]->(h)
                                """
                                session.run(query_ed_to_admission, ed_event_id=first_event_id, hadm_id=hadm_id)
                                logger.info(f"Connected ED {first_event_id} to HospitalAdmission {hadm_id}")
                    else:
                        logger.warning(f"No events found for admission {hadm_id} - cannot determine first event")
                    
                    # Also create Patient→ED relationships for any additional ED events from transfers
                    # (in case there are multiple ED visits, we still want to link them all)
                    ed_transfers = admission_transfers[admission_transfers["eventtype"].str.lower() == "ed"]
                    for _, ed_transfer in ed_transfers.iterrows():
                        ed_event_id = str(int(ed_transfer["transfer_id"]))
                        # Skip if already processed from edstays.csv
                        if ed_event_id in patient_edstays_ids:
                            continue
                        query_patient_ed = """
                        MATCH (p:Patient {subject_id: $subject_id})
                        MATCH (ed:EmergencyDepartment {event_id: $ed_event_id})
                        MERGE (p)-[:VISITED_ED]->(ed)
                        """
                        session.run(query_patient_ed, subject_id=int(subject_id), ed_event_id=ed_event_id)
                        logger.info(f"Linked Patient {subject_id} to ED event {ed_event_id} from transfers")
                    
                    logger.info(f"Processed hospital admission {hadm_id} (Seq {adm_seq_num}) for subject {subject_id}")

                # Handle standalone ED visits (without hadm_id) from transfers.csv
                # These are ED visits that don't belong to any hospital admission
                standalone_ed_transfers = patient_transfers[
                    (patient_transfers["eventtype"].str.lower() == "ed") &
                    (patient_transfers["hadm_id"].isna())
                ]
                for _, ed_transfer in standalone_ed_transfers.iterrows():
                    ed_event_id = str(int(ed_transfer["transfer_id"]))
                    # Skip if already processed from edstays.csv
                    if ed_event_id in patient_edstays_ids:
                        continue
                    
                    # Create ED node if it doesn't exist (it should have been created in the loop above)
                    # But we need to make sure it exists and is connected to Patient
                    intime = ed_transfer["intime"]
                    outtime = ed_transfer["outtime"]
                    intime_str = intime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(intime) else None
                    outtime_str = outtime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(outtime) else None
                    period = human_readable_period(intime, outtime)
                    careunit = ed_transfer["careunit"] if pd.notna(ed_transfer["careunit"]) else "Emergency Department"
                    
                    # Get pre-assigned sequence number from map
                    ed_seq_num = ed_transfer_seq_map.get(ed_event_id, None)
                    
                    # Create or update ED node
                    query_standalone_ed = """
                    MERGE (e:EmergencyDepartment {event_id: $event_id})
                    ON CREATE SET 
                        e.name = $name_value,
                        e.subject_id = $subject_id,
                        e.hadm_id = NULL,
                        e.transfer_id = $transfer_id,
                        e.eventtype = $eventtype,
                        e.careunit = $careunit,
                        e.intime = $intime,
                        e.outtime = $outtime,
                        e.period = $period,
                        e.ed_seq_num = $ed_seq_num
                    ON MATCH SET
                        e.name = $name_value,
                        e.subject_id = $subject_id,
                        e.hadm_id = NULL,
                        e.transfer_id = $transfer_id,
                        e.eventtype = $eventtype,
                        e.careunit = $careunit,
                        e.intime = $intime,
                        e.outtime = $outtime,
                        e.period = $period,
                        e.ed_seq_num = $ed_seq_num
                    """
                    session.run(query_standalone_ed,
                                event_id=ed_event_id,
                                name_value=careunit,
                                subject_id=int(subject_id),
                                transfer_id=int(ed_transfer["transfer_id"]),
                                eventtype="ed",
                                careunit=careunit,
                                intime=intime_str,
                                outtime=outtime_str,
                                period=period,
                                ed_seq_num=ed_seq_num)
                    
                    # Connect Patient to standalone ED
                    query_patient_standalone_ed = """
                    MATCH (p:Patient {subject_id: $subject_id})
                    MATCH (ed:EmergencyDepartment {event_id: $ed_event_id})
                    MERGE (p)-[:VISITED_ED]->(ed)
                    """
                    session.run(query_patient_standalone_ed, subject_id=int(subject_id), ed_event_id=ed_event_id)
                    logger.info(f"Connected Patient {subject_id} to standalone ED visit {ed_event_id}")

                # Link Discharge events to subsequent ED visits with gap calculation
                # Get all discharge events for this subject
                query_discharges = """
                MATCH (d:Discharge {subject_id: $subject_id})
                WHERE d.intime IS NOT NULL
                RETURN d.event_id AS discharge_id, d.intime AS discharge_time
                ORDER BY d.intime
                """
                discharges = session.run(query_discharges, subject_id=int(subject_id))
                discharge_list = list(discharges)
                
                # Collect all ED visits from BOTH edstays.csv AND transfers.csv
                all_ed_visits = []
                
                # 1. Get ED visits from edstays.csv (if available)
                patient_ed_stays = edstays_df[edstays_df["subject_id"] == subject_id]
                for _, ed_stay in patient_ed_stays.iterrows():
                    all_ed_visits.append({
                        'event_id': str(ed_stay["stay_id"]),
                        'intime': ed_stay["intime"],
                        'source': 'edstays'
                    })
                
                # 2. Get ED visits from transfers.csv
                patient_ed_transfers = patient_transfers[patient_transfers["eventtype"].str.lower() == "ed"]
                for _, ed_transfer in patient_ed_transfers.iterrows():
                    all_ed_visits.append({
                        'event_id': str(int(ed_transfer["transfer_id"])),
                        'intime': ed_transfer["intime"],
                        'source': 'transfers'
                    })
                
                # Convert to DataFrame and sort by intime
                if all_ed_visits:
                    all_ed_df = pd.DataFrame(all_ed_visits)
                    all_ed_df = all_ed_df.sort_values(by="intime").reset_index(drop=True)
                    
                    logger.info(f"Found {len(all_ed_df)} total ED visits for subject {subject_id} ({len(patient_ed_stays)} from edstays, {len(patient_ed_transfers)} from transfers)")
                    
                    for discharge_record in discharge_list:
                        discharge_id = discharge_record["discharge_id"]
                        discharge_time = pd.to_datetime(discharge_record["discharge_time"])
                        
                        # Find the next ED visit after this discharge from combined list
                        subsequent_eds = all_ed_df[all_ed_df["intime"] > discharge_time]
                        
                        if not subsequent_eds.empty:
                            # Get the first ED visit after the discharge
                            next_ed = subsequent_eds.iloc[0]
                            next_ed_event_id = next_ed["event_id"]
                            next_ed_time = next_ed["intime"]
                            next_ed_source = next_ed["source"]
                            
                            # Calculate gap between discharge and ED visit
                            gap = calculate_gap(discharge_time, next_ed_time)
                            
                            # Create relationship from Discharge to EmergencyDepartment
                            query_discharge_to_ed = """
                            MATCH (d:Discharge {event_id: $discharge_id})
                            MATCH (ed:EmergencyDepartment {event_id: $ed_event_id})
                            MERGE (d)-[r:LED_TO_ED_VISIT]->(ed)
                            ON CREATE SET r.gap = $gap
                            ON MATCH SET r.gap = $gap
                            """
                            session.run(query_discharge_to_ed, 
                                      discharge_id=discharge_id, 
                                      ed_event_id=next_ed_event_id,
                                      gap=gap)
                            
                            logger.info(f"Linked Discharge {discharge_id} to ED {next_ed_event_id} (from {next_ed_source}) with gap: {gap}")
                else:
                    logger.info(f"No ED visits found for subject {subject_id} to link with discharges")

        # Mark processed patients in tracker
        # Note: This marks success for THIS script only, not the entire pipeline.
        # If a later script fails, this mark remains so we can skip this step on retry.
        if tracker and processed_patients:
            tracker.mark_patients_processed_batch(processed_patients, SCRIPT_NAME, status='success')
            logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip this step on next run)")
        
        # Sync tracker: Mark all patients with complete graphs as processed (even if they were skipped)
        if tracker and patients_with_complete_graphs:
            checker.sync_tracker_for_existing_patients(SCRIPT_NAME, patients_with_complete_graphs)
        
        # Log summary if incremental load was used
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {processed_count} patients, skipped {skipped_count} patients with existing graphs")
        
        logger.info("Patient flows with hospital admits, ED visits (from edstays.csv and/or transfers.csv), inter-admission links, sequence numbers, and total counts created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    create_patient_flow()
