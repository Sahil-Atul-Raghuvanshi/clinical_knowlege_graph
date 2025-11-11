# add_procedure_nodes.py
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

def create_procedure_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '5_add_procedure_nodes'

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    PROCEDURES_ICD_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'procedures_icd.csv')
    ICD_LOOKUP_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'd_icd_procedures.csv')
    PROCEDURE_EVENTS_CSV = os.path.join(project_root, 'Filtered_Data', 'icu', 'procedureevents.csv')
    D_ITEMS_CSV = os.path.join(project_root, 'Filtered_Data', 'icu', 'd_items.csv')

    # Load ICD procedures data
    try:
        proc_icd_df = pd.read_csv(PROCEDURES_ICD_CSV)
        icd_lookup = pd.read_csv(ICD_LOOKUP_CSV)
        
        # Convert icd_code and icd_version to string in both dataframes to ensure consistent data types for merging
        proc_icd_df['icd_code'] = proc_icd_df['icd_code'].astype(str)
        proc_icd_df['icd_version'] = proc_icd_df['icd_version'].astype(str)
        icd_lookup['icd_code'] = icd_lookup['icd_code'].astype(str)
        icd_lookup['icd_version'] = icd_lookup['icd_version'].astype(str)
        
        proc_icd_df = proc_icd_df.merge(icd_lookup, on=["icd_code", "icd_version"], how="left")
        logger.info(f"Loaded {len(proc_icd_df)} ICD procedure records")
    except FileNotFoundError as e:
        logger.warning(f"ICD procedures file not found: {e}")
        proc_icd_df = pd.DataFrame()

    # Load ICU procedure events data
    try:
        proc_events_df = pd.read_csv(PROCEDURE_EVENTS_CSV)
        d_items_df = pd.read_csv(D_ITEMS_CSV)
        # Merge to get item labels and reference ranges
        proc_events_df = proc_events_df.merge(
            d_items_df[['itemid', 'label', 'category', 'lownormalvalue', 'highnormalvalue']], 
            on='itemid', 
            how='left'
        )
        # Convert times
        proc_events_df['starttime'] = pd.to_datetime(proc_events_df['starttime'])
        proc_events_df['endtime'] = pd.to_datetime(proc_events_df['endtime'])
        logger.info(f"Loaded {len(proc_events_df)} ICU procedure event records")
    except FileNotFoundError as e:
        logger.warning(f"Procedure events file not found: {e}")
        proc_events_df = pd.DataFrame()

    try:
        with driver.session() as session:
            # Delete any existing cross-connections before processing
            logger.info("Checking for and deleting cross-connections...")
            
            # Delete HAS_PRESCRIPTIONS relationships from Procedure nodes
            query1 = """
            MATCH (proc)-[r:HAS_PRESCRIPTIONS]->()
            WHERE (proc:Procedures OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result1 = session.run(query1)
            count1 = result1.single()["deleted_count"]
            if count1 > 0:
                logger.info(f"Deleted {count1} HAS_PRESCRIPTIONS from Procedure nodes")
            
            # Delete HAS_LAB_EVENTS relationships from Procedure nodes
            query2 = """
            MATCH (proc)-[r:HAS_LAB_EVENTS]->()
            WHERE (proc:Procedures OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result2 = session.run(query2)
            count2 = result2.single()["deleted_count"]
            if count2 > 0:
                logger.info(f"Deleted {count2} HAS_LAB_EVENTS from Procedure nodes")
            
            # Delete ANY remaining relationships between Procedures and Prescriptions
            query3 = """
            MATCH (proc)-[r]-(presc)
            WHERE (proc:Procedures OR proc:ProceduresBatch)
              AND (presc:Prescription OR presc:PrescriptionsBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result3 = session.run(query3)
            count3 = result3.single()["deleted_count"]
            if count3 > 0:
                logger.info(f"Deleted {count3} connections between Procedures and Prescriptions")
            
            # Delete ANY remaining relationships between Procedures and LabEvents
            query4 = """
            MATCH (proc)-[r]-(lab)
            WHERE (proc:Procedures OR proc:ProceduresBatch)
              AND (lab:LabEvents OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result4 = session.run(query4)
            count4 = result4.single()["deleted_count"]
            if count4 > 0:
                logger.info(f"Deleted {count4} connections between Procedures and LabEvents")
            
            total_deleted = count1 + count2 + count3 + count4
            if total_deleted > 0:
                logger.info(f"Total cross-connections deleted: {total_deleted}")
            else:
                logger.info("No cross-connections found.")
            
            # Fetch all event types: EmergencyDepartment, UnitAdmission, ICUStay, and HospitalAdmission
            query_emergency_dept = """
            MATCH (e:EmergencyDepartment)
            RETURN e.event_id AS event_id, 
                   e.subject_id AS subject_id, 
                   e.hadm_id AS hadm_id,
                   e.intime AS intime, 
                   e.outtime AS outtime,
                   'EmergencyDepartment' AS node_type
            """
            
            query_unit_admission = """
            MATCH (u:UnitAdmission)
            RETURN u.event_id AS event_id, 
                   u.subject_id AS subject_id, 
                   u.hadm_id AS hadm_id,
                   u.intime AS intime, 
                   u.outtime AS outtime,
                   'UnitAdmission' AS node_type
            """
            
            query_icu_stays = """
            MATCH (e:ICUStay)
            RETURN e.event_id AS event_id, 
                   e.subject_id AS subject_id, 
                   e.hadm_id AS hadm_id,
                   e.intime AS intime, 
                   e.outtime AS outtime,
                   'ICUStay' AS node_type
            """
            
            query_hospital_admissions = """
            MATCH (h:HospitalAdmission)
            RETURN h.hadm_id AS event_id,
                   h.subject_id AS subject_id,
                   h.hadm_id AS hadm_id,
                   h.admittime AS intime,
                   h.dischtime AS outtime,
                   'HospitalAdmission' AS node_type
            """
            
            # Combine results from all queries
            ed_results = list(session.run(query_emergency_dept))
            unit_results = list(session.run(query_unit_admission))
            icu_results = list(session.run(query_icu_stays))
            hospital_results = list(session.run(query_hospital_admissions))
            
            # Separate specific event nodes from HospitalAdmission nodes
            # Process specific events first, then handle HospitalAdmission separately
            specific_events = ed_results + unit_results + icu_results
            all_results = specific_events + hospital_results
            
            logger.info(f"Processing {len(ed_results)} ED visits, {len(unit_results)} unit admissions, {len(icu_results)} ICU stays, and {len(hospital_results)} hospital admissions")
            
            # Check for existing procedures (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            events_with_procedures = set()
            with driver.session() as check_session:
                query_existing = """
                MATCH (pb:ProceduresBatch)
                RETURN DISTINCT pb.event_id AS event_id
                """
                result = check_session.run(query_existing)
                events_with_procedures = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(events_with_procedures)} events with existing procedures")
            skipped_events = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()
            
            # Track which procedures (by hadm_id + chartdate) have been connected to specific event nodes
            # This prevents duplicate connections to HospitalAdmission
            connected_procedures = set()  # Set of (hadm_id, chartdate) tuples

            # First, process specific event nodes (ED, UnitAdmission, ICUStay)
            for record in specific_events:
                event_id = str(record["event_id"]).strip() if record["event_id"] is not None else None
                subject_id_raw = record["subject_id"]
                hadm_id_raw = record["hadm_id"]
                node_type = record["node_type"]
                
                if event_id is None or subject_id_raw is None:
                    continue
                
                subject_id = str(subject_id_raw).strip()
                
                try:
                    subject_id_int = int(subject_id)
                except ValueError:
                    logger.warning(f"Skipping event with invalid subject_id: {subject_id}")
                    continue
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if event_id in events_with_procedures:
                        skipped_events += 1
                        if skipped_events == 1 or skipped_events % 100 == 0:
                            logger.info(f"Skipping event {event_id} (patient {subject_id_int} already processed by {SCRIPT_NAME}). Total skipped: {skipped_events}")
                        continue
                
                # Skip if event already has procedures (incremental load)
                if event_id in events_with_procedures:
                    skipped_events += 1
                    if skipped_events == 1 or skipped_events % 100 == 0:
                        logger.info(f"Skipping event {event_id} - already has procedures (incremental load). Total skipped: {skipped_events}")
                    continue
                
                # Determine the node type
                is_emergency_dept = node_type == "EmergencyDepartment"
                is_unit_admission = node_type == "UnitAdmission"
                is_icu_stay = node_type == "ICUStay"
                is_hospital_admission = node_type == "HospitalAdmission"
                
                all_procedures = []
                
                # Process ICU procedure events if this is an ICU stay
                if is_icu_stay and not proc_events_df.empty:
                    # Filter procedure events by stay_id (which matches event_id)
                    stay_id_int = int(event_id)
                    icu_procs_all = proc_events_df[proc_events_df['stay_id'] == stay_id_int].copy()
                    
                    # Filter out ContinuousProcess - only keep Task-based procedures
                    icu_procs = icu_procs_all[icu_procs_all['ordercategorydescription'] == 'Task'].copy()
                    
                    filtered_count = len(icu_procs_all) - len(icu_procs)
                    if filtered_count > 0:
                        logger.info(f"Filtered out {filtered_count} ContinuousProcess items for ICU stay {event_id}, keeping {len(icu_procs)} Task procedures")
                    
                    if not icu_procs.empty:
                        # Group by starttime
                        for starttime, group in icu_procs.groupby('starttime'):
                            procedure_strings = []
                            for _, row in group.iterrows():
                                ordercategoryname = row.get('ordercategoryname', 'Unknown')
                                ordercategorydescription = row.get('ordercategorydescription', '')
                                item_label = row.get('label', 'Unknown')
                                value = row.get('value', '')
                                valueuom = row.get('valueuom', '')
                                lownormal = row.get('lownormalvalue')
                                highnormal = row.get('highnormalvalue')
                                
                                # Format string as requested
                                if pd.notna(ordercategorydescription) and ordercategorydescription:
                                    proc_str = f"{ordercategoryname} ({ordercategorydescription}) - {item_label}"
                                else:
                                    proc_str = f"{ordercategoryname} - {item_label}"
                                
                                if pd.notna(value) and value:
                                    proc_str += f" with value {value}"
                                    if pd.notna(valueuom) and valueuom:
                                        proc_str += f"{valueuom}"
                                
                                # Add reference range if available
                                if pd.notna(lownormal) and pd.notna(highnormal):
                                    proc_str += f" (Ref: {lownormal} - {highnormal})"
                                elif pd.notna(lownormal):
                                    proc_str += f" (Ref: {lownormal} - ∞)"
                                elif pd.notna(highnormal):
                                    proc_str += f" (Ref: 0 - {highnormal})"
                                
                                procedure_strings.append(proc_str)
                            
                            all_procedures.append({
                                'time': starttime,
                                'time_str': starttime.strftime("%Y-%m-%d %H:%M:%S"),
                                'procedures': procedure_strings,
                                'source': 'ICU_EVENTS'
                            })
                
                # Process ICD procedures for both ICUStay and HospitalAdmission nodes
                if hadm_id_raw is not None and not proc_icd_df.empty:
                    try:
                        hadm_id_int = int(str(hadm_id_raw).strip())
                        
                        # Get intime and outtime for filtering if available
                        intime = pd.to_datetime(record["intime"]) if record.get("intime") else None
                        outtime = pd.to_datetime(record["outtime"]) if record.get("outtime") else None
                        
                        # Filter ICD procedures
                        icd_procs = proc_icd_df[
                            (proc_icd_df["subject_id"] == subject_id_int) &
                            (proc_icd_df["hadm_id"] == hadm_id_int)
                        ].copy()
                        
                        # If we have time bounds, filter by them
                        if not icd_procs.empty and intime is not None and outtime is not None:
                            icd_procs['chartdate'] = pd.to_datetime(icd_procs['chartdate'])
                            icd_procs = icd_procs[
                                (icd_procs['chartdate'] >= intime) &
                                (icd_procs['chartdate'] <= outtime)
                            ]
                        
                        if not icd_procs.empty:
                            # Group by chartdate
                            for chartdate, group in icd_procs.groupby('chartdate'):
                                # Track that this procedure has been connected to a specific event
                                chartdate_key = (hadm_id_int, pd.to_datetime(chartdate).date())
                                connected_procedures.add(chartdate_key)
                                
                                procedure_strings = []
                                for _, row in group.iterrows():
                                    title = str(row["long_title"]) if pd.notna(row.get("long_title")) else "Unknown"
                                    procedure_strings.append(title)
                                
                                all_procedures.append({
                                    'time': pd.to_datetime(chartdate),
                                    'time_str': pd.to_datetime(chartdate).strftime("%Y-%m-%d %H:%M:%S"),
                                    'procedures': procedure_strings,
                                    'source': 'ICD'
                                })
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Error processing ICD procedures for event {event_id}: {e}")
                
                # If we have procedures, create the batch structure
                if all_procedures:
                    # Sort by time
                    all_procedures.sort(key=lambda x: x['time'])
                    
                    # Create ProceduresBatch node - match by node type
                    if is_emergency_dept:
                        query_batch = """
                        MATCH (e:EmergencyDepartment {event_id: $event_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (e)-[:INCLUDED_PROCEDURES]->(pb)
                        """
                    elif is_unit_admission:
                        query_batch = """
                        MATCH (u:UnitAdmission {event_id: $event_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (u)-[:INCLUDED_PROCEDURES]->(pb)
                        """
                    elif is_icu_stay:
                        query_batch = """
                        MATCH (e:ICUStay {event_id: $event_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (e)-[:INCLUDED_PROCEDURES]->(pb)
                        """
                    else:  # HospitalAdmission
                        # For HospitalAdmission, event_id is the hadm_id
                        # Convert event_id to int for matching
                        try:
                            hadm_id_for_match = int(event_id)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid event_id for HospitalAdmission: {event_id}. Skipping.")
                            continue
                        
                        query_batch = """
                        MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (h)-[:INCLUDED_PROCEDURES]->(pb)
                        RETURN h.hadm_id AS matched_hadm_id, pb.event_id AS batch_event_id
                        """
                    
                    hadm_id_for_batch = int(hadm_id_raw) if hadm_id_raw is not None else None
                    result = session.run(query_batch, event_id=event_id, hadm_id=hadm_id_for_match if is_hospital_admission else hadm_id_for_batch, subject_id=subject_id_int)
                    
                    # Verify the connection was made
                    if is_hospital_admission:
                        result_record = result.single()
                        if result_record is None:
                            logger.warning(f"Failed to connect ProceduresBatch to HospitalAdmission {event_id}. HospitalAdmission node may not exist.")
                            continue
                        else:
                            logger.debug(f"Successfully connected ProceduresBatch to HospitalAdmission {event_id}")
                    
                    # Create individual Procedures nodes
                    proc_counter = 1
                    for proc_group in all_procedures:
                        procedure_props = {
                            "event_id": event_id,
                            "time": proc_group['time_str'],
                            "procedures": proc_group['procedures'],
                            "procedure_count": len(proc_group['procedures']),
                            "name": "Procedures",
                            "source": proc_group['source']
                        }
                        
                        query_procedures = """
                        MERGE (p:Procedures {
                            event_id: $event_id,
                            time: $time
                        })
                        SET p.procedures = $procedures,
                            p.procedure_count = $procedure_count,
                            p.name = $name,
                            p.source = $source
                        """
                        session.run(query_procedures, **procedure_props)
                        
                        # Link Procedures → ProceduresBatch
                        query_link_procedures = """
                        MATCH (pb:ProceduresBatch {event_id: $event_id})
                        MATCH (p:Procedures {event_id: $event_id, time: $time})
                        MERGE (pb)-[:CONTAINED_PROCEDURE]->(p)
                        """
                        session.run(query_link_procedures, event_id=event_id, time=proc_group['time_str'])
                        
                        proc_counter += 1
                    
                    logger.info(f"Added {len(all_procedures)} procedure groups for {node_type} {event_id}")
                    
                    # Track patient as processed
                    processed_patients.add(subject_id_int)
            
            # Now process HospitalAdmission nodes - only connect procedures that haven't been connected to specific events
            logger.info("Processing HospitalAdmission nodes - only connecting procedures without parent event nodes...")
            hospital_processed_count = 0
            
            for record in hospital_results:
                event_id = str(record["event_id"]).strip() if record["event_id"] is not None else None
                subject_id_raw = record["subject_id"]
                hadm_id_raw = record["hadm_id"]
                
                if event_id is None or subject_id_raw is None or hadm_id_raw is None:
                    continue
                
                subject_id = str(subject_id_raw).strip()
                
                try:
                    subject_id_int = int(subject_id)
                    hadm_id_int = int(str(hadm_id_raw).strip())
                except ValueError:
                    logger.warning(f"Skipping HospitalAdmission with invalid IDs: event_id={event_id}, subject_id={subject_id}, hadm_id={hadm_id_raw}")
                    continue
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if event_id in events_with_procedures:
                        skipped_events += 1
                        if skipped_events == 1 or skipped_events % 100 == 0:
                            logger.info(f"Skipping HospitalAdmission {event_id} (patient {subject_id_int} already processed by {SCRIPT_NAME}). Total skipped: {skipped_events}")
                        continue
                
                # Skip if HospitalAdmission already has procedures (incremental load)
                if event_id in events_with_procedures:
                    skipped_events += 1
                    if skipped_events == 1 or skipped_events % 100 == 0:
                        logger.info(f"Skipping HospitalAdmission {event_id} - already has procedures (incremental load). Total skipped: {skipped_events}")
                    continue
                
                # Process ICD procedures for HospitalAdmission - only those NOT already connected to specific events
                if not proc_icd_df.empty:
                    try:
                        # Get intime and outtime for filtering if available
                        intime = pd.to_datetime(record["intime"]) if record.get("intime") else None
                        outtime = pd.to_datetime(record["outtime"]) if record.get("outtime") else None
                        
                        # Filter ICD procedures
                        icd_procs = proc_icd_df[
                            (proc_icd_df["subject_id"] == subject_id_int) &
                            (proc_icd_df["hadm_id"] == hadm_id_int)
                        ].copy()
                        
                        # Convert chartdate to datetime if not already done
                        if not icd_procs.empty:
                            icd_procs['chartdate'] = pd.to_datetime(icd_procs['chartdate'])
                        
                        # If we have time bounds, filter by them
                        if not icd_procs.empty and intime is not None and outtime is not None:
                            icd_procs = icd_procs[
                                (icd_procs['chartdate'] >= intime) &
                                (icd_procs['chartdate'] <= outtime)
                            ]
                        
                        # Filter out procedures that have already been connected to specific event nodes
                        if not icd_procs.empty:
                            orphaned_procs = []
                            for chartdate, group in icd_procs.groupby('chartdate'):
                                chartdate_key = (hadm_id_int, pd.to_datetime(chartdate).date())
                                # Only process if NOT already connected to a specific event
                                if chartdate_key not in connected_procedures:
                                    procedure_strings = []
                                    for _, row in group.iterrows():
                                        title = str(row["long_title"]) if pd.notna(row.get("long_title")) else "Unknown"
                                        procedure_strings.append(title)
                                    
                                    orphaned_procs.append({
                                        'time': pd.to_datetime(chartdate),
                                        'time_str': pd.to_datetime(chartdate).strftime("%Y-%m-%d %H:%M:%S"),
                                        'procedures': procedure_strings,
                                        'source': 'ICD'
                                    })
                            
                            # Only create ProceduresBatch if there are orphaned procedures
                            if orphaned_procs:
                                # Sort by time
                                orphaned_procs.sort(key=lambda x: x['time'])
                                
                                # Create ProceduresBatch node for HospitalAdmission
                                try:
                                    hadm_id_for_match = int(event_id)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid event_id for HospitalAdmission: {event_id}. Skipping.")
                                    continue
                                
                                query_batch = """
                                MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                                MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                                ON CREATE SET pb.name = "Procedures"
                                MERGE (h)-[:INCLUDED_PROCEDURES]->(pb)
                                RETURN h.hadm_id AS matched_hadm_id, pb.event_id AS batch_event_id
                                """
                                
                                result = session.run(query_batch, event_id=event_id, hadm_id=hadm_id_for_match, subject_id=subject_id_int)
                                
                                # Verify the connection was made
                                result_record = result.single()
                                if result_record is None:
                                    logger.warning(f"Failed to connect ProceduresBatch to HospitalAdmission {event_id}. HospitalAdmission node may not exist.")
                                    continue
                                
                                # Create individual Procedures nodes
                                for proc_group in orphaned_procs:
                                    procedure_props = {
                                        "event_id": event_id,
                                        "time": proc_group['time_str'],
                                        "procedures": proc_group['procedures'],
                                        "procedure_count": len(proc_group['procedures']),
                                        "name": "Procedures",
                                        "source": proc_group['source']
                                    }
                                    
                                    query_procedures = """
                                    MERGE (p:Procedures {
                                        event_id: $event_id,
                                        time: $time
                                    })
                                    SET p.procedures = $procedures,
                                        p.procedure_count = $procedure_count,
                                        p.name = $name,
                                        p.source = $source
                                    """
                                    session.run(query_procedures, **procedure_props)
                                    
                                    # Link Procedures → ProceduresBatch
                                    query_link_procedures = """
                                    MATCH (pb:ProceduresBatch {event_id: $event_id})
                                    MATCH (p:Procedures {event_id: $event_id, time: $time})
                                    MERGE (pb)-[:CONTAINED_PROCEDURE]->(p)
                                    """
                                    session.run(query_link_procedures, event_id=event_id, time=proc_group['time_str'])
                                
                                hospital_processed_count += 1
                                logger.info(f"Added {len(orphaned_procs)} orphaned procedure groups for HospitalAdmission {event_id} (procedures not connected to specific events)")
                                
                                # Track patient as processed
                                processed_patients.add(subject_id_int)
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Error processing ICD procedures for HospitalAdmission {event_id}: {e}")
            
            logger.info(f"Processed {hospital_processed_count} HospitalAdmission nodes with orphaned procedures")
            
            # Log incremental load summary
            if skipped_events > 0:
                logger.info(f"Incremental load summary: Skipped {skipped_events} events that already have procedures")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            # Fix any remaining orphaned ProceduresBatch nodes - connect them to HospitalAdmission if they have a hadm_id
            logger.info("Checking for any remaining orphaned ProceduresBatch nodes...")
            query_orphaned = """
            MATCH (pb:ProceduresBatch)
            WHERE pb.hadm_id IS NOT NULL
            AND NOT EXISTS {
                MATCH ()-[r:INCLUDED_PROCEDURES]->(pb)
            }
            RETURN pb.event_id AS event_id, pb.hadm_id AS hadm_id
            """
            orphaned_results = list(session.run(query_orphaned))
            
            if orphaned_results:
                logger.info(f"Found {len(orphaned_results)} remaining orphaned ProceduresBatch nodes. Attempting to connect them...")
                connected_count = 0
                for record in orphaned_results:
                    event_id = str(record["event_id"]) if record["event_id"] is not None else None
                    hadm_id = record["hadm_id"]
                    
                    if event_id and hadm_id:
                        # Try to connect to HospitalAdmission (event_id for HospitalAdmission is the hadm_id)
                        try:
                            hadm_id_int = int(hadm_id)
                            query_fix = """
                            MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                            MATCH (pb:ProceduresBatch {event_id: $event_id})
                            MERGE (h)-[:INCLUDED_PROCEDURES]->(pb)
                            RETURN h.hadm_id AS matched_hadm_id
                            """
                            fix_result = session.run(query_fix, hadm_id=hadm_id_int, event_id=event_id)
                            if fix_result.single():
                                connected_count += 1
                                logger.info(f"Connected orphaned ProceduresBatch {event_id} to HospitalAdmission {hadm_id}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Could not connect orphaned ProceduresBatch {event_id}: {e}")
                
                if connected_count > 0:
                    logger.info(f"Successfully connected {connected_count} orphaned ProceduresBatch nodes to HospitalAdmission nodes")
                else:
                    logger.warning(f"Could not connect any orphaned ProceduresBatch nodes. They may need manual review.")

        logger.info("All procedures processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_procedure_nodes()
