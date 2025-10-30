# add_procedure_nodes.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_procedure_nodes():
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

    # File paths
    PROCEDURES_ICD_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\hosp\procedures_icd.csv"
    ICD_LOOKUP_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\hosp\d_icd_procedures.csv"
    PROCEDURE_EVENTS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\icu\procedureevents.csv"
    D_ITEMS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\icu\d_items.csv"

    # Load ICD procedures data
    try:
        proc_icd_df = pd.read_csv(PROCEDURES_ICD_CSV)
        icd_lookup = pd.read_csv(ICD_LOOKUP_CSV)
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
            
            # Fetch ICU stays and Hospital Admissions only
            # ICU stays get procedure events, Hospital Admissions get ICD procedures
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
            
            # Combine results from both queries
            icu_results = list(session.run(query_icu_stays))
            hospital_results = list(session.run(query_hospital_admissions))
            all_results = icu_results + hospital_results
            
            logger.info(f"Processing {len(icu_results)} ICU stays and {len(hospital_results)} hospital admissions")

            for record in all_results:
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
                
                # Determine if this is an ICU stay or Hospital Admission
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
                    if is_icu_stay:
                        query_batch = """
                        MATCH (e:ICUStay {event_id: $event_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (e)-[:INCLUDED_PROCEDURES]->(pb)
                        """
                    else:  # HospitalAdmission
                        query_batch = """
                        MATCH (h:HospitalAdmission {hadm_id: $event_id})
                        MERGE (pb:ProceduresBatch {event_id: $event_id, hadm_id: $hadm_id, subject_id: $subject_id})
                        ON CREATE SET pb.name = "Procedures"
                        MERGE (h)-[:INCLUDED_PROCEDURES]->(pb)
                        """
                    
                    hadm_id_for_batch = int(hadm_id_raw) if hadm_id_raw is not None else None
                    session.run(query_batch, event_id=event_id, hadm_id=hadm_id_for_batch, subject_id=subject_id_int)
                    
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

        logger.info("All procedures processed successfully!")

    finally:
        driver.close()


if __name__ == "__main__":
    create_procedure_nodes()
