# patient_flow_through_the_hospital.py
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

def create_patient_flow(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_patient_flow.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '2_patient_flow_through_the_hospital'

    # File paths (relative to script location)
    project_root = script_dir.parent.parent.parent
    ADMISSIONS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'admissions.csv'
    TRANSFERS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'transfers.csv'
    SERVICES_CSV = project_root / 'Filtered_Data' / 'hosp' / 'services.csv'
    EDSTAYS_CSV = project_root / 'Filtered_Data' / 'ed' / 'edstays.csv'

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    try:
        # Load data
        admissions_df = pd.read_csv(str(ADMISSIONS_CSV))
        admissions_df["admittime"] = pd.to_datetime(admissions_df["admittime"], errors="coerce")
        admissions_df["dischtime"] = pd.to_datetime(admissions_df["dischtime"], errors="coerce")

        transfers_df = pd.read_csv(str(TRANSFERS_CSV))
        transfers_df["intime"] = pd.to_datetime(transfers_df["intime"], errors="coerce")
        transfers_df["outtime"] = pd.to_datetime(transfers_df["outtime"], errors="coerce")

        # Load services data
        services_df = pd.read_csv(str(SERVICES_CSV))
        services_df["transfertime"] = pd.to_datetime(services_df["transfertime"], errors="coerce")
        
        # Convert data types for services_df to match other dataframes
        services_df["subject_id"] = services_df["subject_id"].astype(int)
        services_df["hadm_id"] = services_df["hadm_id"].astype('Int64')

        # Load ED stays data
        edstays_df = pd.read_csv(str(EDSTAYS_CSV))
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

        with neo4j_conn.session() as session:
            # Get unique subject_ids from both admissions and ED stays
            all_subjects = pd.concat([
                admissions_df["subject_id"],
                edstays_df["subject_id"]
            ]).unique()

            # Check for existing patients with complete graphs (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
            skipped_count = 0
            processed_count = 0
            processed_patients = []
            failed_patients = []
            patients_with_complete_graphs = set()  # Track all patients with complete graphs for batch sync
            
            pbar = tqdm(total=len(all_subjects), desc="Processing patient flows", unit="patient")
            for subject_id in all_subjects:
                subject_id_int = int(subject_id)
                
                # Check tracker first (faster than database query)
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                # Skip if patient already has complete graph
                if checker.patient_has_complete_graph(subject_id_int):
                    skipped_count += 1
                    patients_with_complete_graphs.add(subject_id_int)
                    pbar.update(1)
                    pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                processed_count += 1
                try:
                    # Process ED stays first (both standalone and admission-linked)
                    process_ed_stays(session, edstays_df, transfers_df, subject_id)

                    # Process hospital admissions if they exist
                    patient_admissions = admissions_df[admissions_df["subject_id"] == subject_id]
                    if patient_admissions.empty:
                        # Mark patient as processed if no admissions (ED-only visit)
                        if tracker:
                            try:
                                tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='success')
                                processed_patients.append(subject_id_int)
                            except Exception as e:
                                logger.error(f"Error marking patient {subject_id_int} as processed in tracker: {e}")
                        pbar.update(1)
                        pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
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
                        
                        logger.info(f"Processing {len(admission_transfers)} transfers for admission {hadm_id} (subject {subject_id})")

                        previous_event_id = None
                        previous_event_type = None
                        previous_careunit = None
                        previous_outtime = None
                        previous_intime = None
                        
                        # Track continuous same-careunit stays
                        first_event_id_in_continuous_stay = None  # Event ID of first transfer in continuous stay
                        first_intime_in_continuous_stay = None    # Intime of first transfer
                        is_in_continuous_stay = False             # Flag for continuous stay

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
                            
                            logger.info(f"Processing transfer: event_id={event_id}, event_type={event_type}, label={label}, careunit={careunit}, hadm_id={hadm_id}")
                            logger.error(f"*** AFTER Processing transfer log, before continuous stay check ***")
                            
                            # Check if this is a consecutive stay in the same care unit
                            is_continuous_same_unit = False
                            if (previous_careunit and 
                                previous_careunit == careunit and
                                event_type in ["admit", "transfer"] and
                                previous_event_type in ["admit", "transfer"] and
                                pd.notna(previous_outtime) and 
                                pd.notna(intime) and
                                previous_outtime == intime):  # Times match exactly (continuous)
                                is_continuous_same_unit = True
                                # If we're already tracking a continuous stay, use that event_id, otherwise use previous_event_id
                                if is_in_continuous_stay and first_event_id_in_continuous_stay:
                                    # Already in a continuous stay, keep using the first event_id
                                    pass
                                else:
                                    # Starting a new continuous stay - previous event becomes the first
                                    is_in_continuous_stay = True
                                    first_event_id_in_continuous_stay = previous_event_id
                                    first_intime_in_continuous_stay = previous_intime
                                logger.info(f"Detected continuous stay in {careunit}: extending stay from event {first_event_id_in_continuous_stay or previous_event_id} (current event {event_id})")
                            else:
                                # Not a continuous stay - reset tracking
                                is_in_continuous_stay = False
                                first_event_id_in_continuous_stay = None
                                first_intime_in_continuous_stay = None
                            
                            logger.error(f"*** AFTER continuous stay check, before name_value setting ***")
                            # Set name based on event type
                            if event_type == "discharge":
                                name_value = "Discharge"  # Always use "Discharge" for discharge nodes
                            elif event_type in ["admit", "transfer"]:
                                name_value = careunit  # Use careunit for UnitAdmission nodes
                            else:
                                name_value = label  # Default to label for other types

                            logger.error(f"*** AFTER name_value setting, before if/elif chain ***")
                            # For ED nodes, include ed_seq_num
                            logger.error(f"*** *** *** About to check event_type: {event_type} (repr: {repr(event_type)}) *** *** ***")
                            if event_type == "ed":
                                # Reset continuous stay tracking (ED breaks the sequence)
                                is_in_continuous_stay = False
                                first_event_id_in_continuous_stay = None
                                first_intime_in_continuous_stay = None
                                
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
                            elif event_type == "discharge":
                                # Reset continuous stay tracking (discharge breaks the sequence)
                                is_in_continuous_stay = False
                                first_event_id_in_continuous_stay = None
                                first_intime_in_continuous_stay = None
                                
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
                                
                                # Create relationship from previous event to discharge
                                # This is critical for merged continuous stay nodes - they need to connect to discharge
                                # Note: previous_event_id should already point to the merged node if the previous event was part of a continuous stay
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
                                    logger.info(f"Connected previous event {previous_event_id} (type: {previous_event_type}) to discharge {event_id}")
                            elif event_type in ["admit", "transfer"]:
                                # Handle UnitAdmission nodes (admit/transfer)
                                logger.error(f"*** *** *** ENTERING UnitAdmission block for {event_type} event: event_id={event_id}, careunit={careunit}, hadm_id={hadm_id}, is_continuous={is_continuous_same_unit} *** *** ***")
                                if is_continuous_same_unit:
                                    # This is a continuation of the same stay - update existing node
                                    # Use the first event_id and update outtime to current outtime
                                    merged_outtime_str = outtime_str
                                    
                                    # Get the first intime from the node if we don't have it yet
                                    if first_intime_in_continuous_stay is None:
                                        # Retrieve intime from the first event node
                                        get_intime_query = f"""
                                        MATCH (e:{label} {{event_id: $first_event_id}})
                                        RETURN e.intime AS first_intime
                                        """
                                        result = session.run(get_intime_query, first_event_id=first_event_id_in_continuous_stay or previous_event_id)
                                        record = result.single()
                                        if record and record["first_intime"]:
                                            first_intime_in_continuous_stay = pd.to_datetime(record["first_intime"])
                                        else:
                                            first_intime_in_continuous_stay = previous_intime
                                    
                                    merged_period = human_readable_period(first_intime_in_continuous_stay, outtime)
                                    
                                    query_node = f"""
                                    MATCH (e:{label} {{event_id: $first_event_id}})
                                    SET e.outtime = $outtime,
                                        e.period = $period
                                    """
                                    session.run(query_node,
                                                first_event_id=first_event_id_in_continuous_stay or previous_event_id,
                                                outtime=merged_outtime_str,
                                                period=merged_period)
                                    
                                    logger.info(f"Updated continuous stay node {first_event_id_in_continuous_stay or previous_event_id} with new outtime {merged_outtime_str}")
                                    
                                    # Don't create a new node or relationship - we're extending the existing one
                                    # Update previous_event_id to the merged node's ID for relationship purposes
                                    # but don't create a relationship since it's the same stay
                                else:
                                    # This is a new stay (either first in sequence or different care unit)
                                    # Reset continuous stay tracking - we'll set it if next event is continuous
                                    is_in_continuous_stay = False
                                    first_event_id_in_continuous_stay = None
                                    first_intime_in_continuous_stay = None
                                    
                                    try:
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
                                        
                                        logger.info(f"Created UnitAdmission node: event_id={event_id}, careunit={careunit}, hadm_id={hadm_id}, subject_id={subject_id}, label={label}")
                                    except Exception as unit_error:
                                        logger.error(f"ERROR creating UnitAdmission node: event_id={event_id}, careunit={careunit}, hadm_id={hadm_id}, error={unit_error}")
                                        logger.error(f"Query parameters: event_id={event_id}, name_value={name_value}, subject_id={subject_id}, hadm_id={hadm_id}, label={label}")
                                        raise  # Re-raise to be caught by outer exception handler
                                    
                                    # Create relationship from previous event (if exists and not in continuous stay)
                                    if previous_event_id and not is_continuous_same_unit:
                                        rel_next = event_relationship(previous_event_type, event_type)
                                        query_flow = f"""
                                        MATCH (e1 {{event_id: $prev_id}})
                                        MATCH (e2 {{event_id: $curr_id}})
                                        MERGE (e1)-[:{rel_next}]->(e2)
                                        """
                                        session.run(query_flow,
                                                    prev_id=previous_event_id,
                                                    curr_id=event_id)
                            else:
                                # Other event types - reset continuous stay tracking
                                logger.info(f"*** FALLING THROUGH TO ELSE BLOCK for event_type: {event_type}")
                                is_in_continuous_stay = False
                                first_event_id_in_continuous_stay = None
                                first_intime_in_continuous_stay = None
                                
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

                            # Update tracking variables
                            # For continuous stays, keep the first event_id as previous_event_id for relationship purposes
                            if is_continuous_same_unit:
                                # Keep the first event_id as previous_event_id (don't change it)
                                # This ensures relationships point to the merged node
                                # But update outtime so next iteration can check continuity
                                previous_outtime = outtime
                                # previous_event_id, previous_event_type, previous_careunit, previous_intime stay the same
                            else:
                                # Update all tracking variables for new event
                                # If we were in a continuous stay but this event is not continuous, reset the tracking
                                if is_in_continuous_stay:
                                    # The continuous stay has ended, reset tracking
                                    is_in_continuous_stay = False
                                    first_event_id_in_continuous_stay = None
                                    first_intime_in_continuous_stay = None
                                
                                # Use current event_id for the new event
                                previous_event_id = event_id
                                previous_event_type = event_type
                                previous_careunit = careunit
                                previous_outtime = outtime
                                previous_intime = intime

                        # ====================================================================
                        # CRITICAL: Find the first event and connect Patient and HospitalAdmission
                        # ====================================================================
                        # The first event may be ED, admit, or transfer - not always ED!
                        # We need to:
                        # 1. Find the chronologically first event (ED, admit, or transfer)
                        # 2. Connect Patient to the first event
                        # 3. Connect HospitalAdmission to the first unit admission if timing allows
                        
                        logger.info(f"*** CRITICAL SECTION: Finding first event for admission {hadm_id} (subject {subject_id}) ***")
                        
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
                        logger.info(f"Found {len(first_events)} potential first events for admission {hadm_id}")
                        if first_events:
                            first_events_df = pd.DataFrame(first_events)
                            first_events_df = first_events_df.sort_values(by="intime").reset_index(drop=True)
                            logger.info(f"All events sorted by intime for admission {hadm_id}:")
                            for idx, row in first_events_df.iterrows():
                                logger.info(f"  Event {idx}: {row['event_type']} (event_id: {row['event_id']}, intime: {row['intime']}, label: {row['label']})")
                            
                            first_event = first_events_df.iloc[0]
                            first_event_id = first_event["event_id"]
                            first_event_type = first_event["event_type"]
                            first_event_label = first_event["label"]
                            first_event_intime = first_event["intime"]
                            first_event_outtime = first_event["outtime"]
                            
                            logger.info(f"*** SELECTED FIRST EVENT for admission {hadm_id}: {first_event_type} (event_id: {first_event_id}, intime: {first_event_intime}, label: {first_event_label}) ***")
                            
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
                                # This is CRITICAL: Patient must be connected to the first UnitAdmission event
                                # when they are directly admitted (not through ED)
                                logger.info(f"*** CRITICAL: Connecting Patient {subject_id} to first UnitAdmission event: {first_event_id} (type: {first_event_type}) ***")
                                
                                # First verify the node exists
                                check_node = """
                                MATCH (e:UnitAdmission {event_id: $event_id})
                                RETURN e.event_id AS event_id, e.careunit AS careunit, e.eventtype AS eventtype
                                """
                                result = session.run(check_node, event_id=first_event_id)
                                node_check = result.single()
                                if node_check:
                                    logger.info(f"✓ UnitAdmission node {first_event_id} exists: careunit={node_check['careunit']}, eventtype={node_check['eventtype']}")
                                else:
                                    logger.error(f"✗ ERROR: UnitAdmission node {first_event_id} does NOT exist! This should have been created in the transfer loop.")
                                    logger.error(f"  This is a critical error - Patient cannot be connected to a non-existent node.")
                                
                                # Always attempt the connection - MERGE will create the relationship if both nodes exist
                                query_patient_first = """
                                MATCH (p:Patient {subject_id: $subject_id})
                                MATCH (e:UnitAdmission {event_id: $event_id})
                                MERGE (p)-[r:ADMITTED_TO_UNIT]->(e)
                                RETURN p.subject_id AS patient_id, e.event_id AS unit_id, e.careunit AS careunit, e.eventtype AS eventtype
                                """
                                try:
                                    result = session.run(query_patient_first, subject_id=int(subject_id), event_id=first_event_id)
                                    connection_result = result.single()
                                    if connection_result:
                                        logger.info(f"✓✓✓ SUCCESS: Connected Patient {subject_id} to first UnitAdmission event: {first_event_id}")
                                        logger.info(f"   UnitAdmission details: careunit={connection_result['careunit']}, eventtype={connection_result['eventtype']}")
                                    else:
                                        logger.error(f"✗✗✗ CRITICAL ERROR: Failed to connect Patient {subject_id} to UnitAdmission {first_event_id}")
                                        logger.error(f"   Query returned no result - Patient or UnitAdmission node may not exist")
                                except Exception as conn_error:
                                    logger.error(f"✗✗✗ EXCEPTION while connecting Patient {subject_id} to UnitAdmission {first_event_id}: {conn_error}")
                                    raise
                            
                            # Connect UnitAdmission to HospitalAdmission (if first event is a unit admission)
                            # OR connect ED to HospitalAdmission if first event is ED
                            if first_event_type in ["admit", "transfer"]:
                                # First event is a unit admission
                                # Check if hospital admission time is within or before the first unit admission's time range
                                if pd.notna(adm_intime) and pd.notna(first_event_intime):
                                    # Determine relationship based on timing
                                    if adm_intime <= first_event_intime:
                                        # Hospital admission happened before or at the same time as first unit admission
                                        relationship = "LED_TO_ADMISSION"
                                        logger.info(f"Unit admission {first_event_id} (time: {first_event_intime}) led to hospital admission {hadm_id} (time: {adm_intime})")
                                    elif pd.notna(first_event_outtime) and adm_intime <= first_event_outtime:
                                        # Hospital admission happened during the first unit admission
                                        relationship = "LED_TO_ADMISSION"
                                        logger.info(f"Unit admission {first_event_id} (time: {first_event_intime} to {first_event_outtime}) led to hospital admission {hadm_id} (time: {adm_intime})")
                                    else:
                                        # Hospital admission happened after first unit admission (unusual but possible)
                                        relationship = "LED_TO_ADMISSION"
                                        logger.warning(f"Unit admission {first_event_id} (time: {first_event_intime}) led to hospital admission {hadm_id} (time: {adm_intime}) - unusual timing")
                                    
                                    query_unit_to_hosp = f"""
                                    MATCH (u:UnitAdmission {{event_id: $event_id}})
                                    MATCH (h:HospitalAdmission {{hadm_id: $hadm_id}})
                                    MERGE (u)-[:{relationship}]->(h)
                                    """
                                    session.run(query_unit_to_hosp, hadm_id=hadm_id, event_id=first_event_id)
                                    logger.info(f"Connected UnitAdmission {first_event_id} to HospitalAdmission {hadm_id}")
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
                    
                    # Mark patient as processed immediately after successful processing
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='success')
                            processed_patients.append(subject_id_int)
                        except Exception as e:
                            logger.error(f"Error marking patient {subject_id_int} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing patient {subject_id_int}: {e}")
                    # Mark patient as failed immediately
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='failed')
                            failed_patients.append(subject_id_int)
                        except Exception as tracker_error:
                            logger.error(f"Error marking patient {subject_id_int} as failed in tracker: {tracker_error}")
                
                pbar.update(1)
                pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
            
            pbar.close()

        # Log summary
        if tracker and processed_patients:
            logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker for script '{SCRIPT_NAME}'")
        if failed_patients:
            logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
        
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
        neo4j_conn.close()

if __name__ == "__main__":
    create_patient_flow()
