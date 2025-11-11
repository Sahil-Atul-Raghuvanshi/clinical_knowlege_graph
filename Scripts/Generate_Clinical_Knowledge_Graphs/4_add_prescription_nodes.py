# add_prescription_nodes.py
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

def create_previous_prescription_meds(driver):
    """Create PreviousPrescriptionMeds nodes from medrecon data"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    MEDRECON_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'medrecon.csv')
    
    try:
        # Load medrecon data
        medrecon_df = pd.read_csv(MEDRECON_CSV)
        medrecon_df["charttime"] = pd.to_datetime(medrecon_df["charttime"], errors="coerce")
        
        # Group by stay_id only to create one node per ED visit
        grouped = medrecon_df.groupby('stay_id')
        
        with driver.session() as session:
            for stay_id, group in grouped:
                # Format medications as "[Medication Name] - [ETC Description]"
                med_descriptions = []
                for _, row in group.iterrows():
                    med_name = row['name']
                    etc_desc = row['etcdescription'] if pd.notna(row['etcdescription']) else "No Classification"
                    med_descriptions.append(f"{med_name} - {etc_desc}")
                
                # Get the first charttime for reference
                first_charttime = group['charttime'].iloc[0]
                
                # Create PreviousPrescriptionMeds node and link to EmergencyDepartment
                query = """
                MATCH (ed:EmergencyDepartment {event_id: $stay_id})
                MERGE (prev:PreviousPrescriptionMeds {stay_id: $stay_id})
                SET prev.name = 'PreviousPrescriptionMeds',
                    prev.medications = $medications,
                    prev.medication_count = $count,
                    prev.charttime = $charttime
                MERGE (ed)-[:RECORDED_PREVIOUS_MEDICATIONS]->(prev)
                """
                session.run(query,
                          stay_id=str(stay_id),
                          charttime=first_charttime.strftime('%Y-%m-%d %H:%M:%S'),
                          medications=med_descriptions,
                          count=len(med_descriptions))
                
                logger.info(f"Processed {len(med_descriptions)} previous medications for ED stay {stay_id}")
                
    except Exception as e:
        logger.error(f"Error processing previous prescription meds: {e}")
        raise

def create_administered_meds(driver):
    """Create AdministeredMeds nodes from pyxis data"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    PYXIS_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'pyxis.csv')
    
    try:
        # Load pyxis data
        pyxis_df = pd.read_csv(PYXIS_CSV)
        pyxis_df["charttime"] = pd.to_datetime(pyxis_df["charttime"], errors="coerce")
        
        # Group by stay_id only to create one node per ED visit
        grouped = pyxis_df.groupby('stay_id')
        
        with driver.session() as session:
            for stay_id, group in grouped:
                # Aggregate medication names into array of strings
                med_names = []
                for _, row in group.iterrows():
                    med_name = row['name']
                    if pd.notna(med_name):
                        med_names.append(str(med_name))
                
                # Get the first charttime for reference
                first_charttime = group['charttime'].iloc[0]
                
                # Create AdministeredMeds node and link to EmergencyDepartment
                query = """
                MATCH (ed:EmergencyDepartment {event_id: $stay_id})
                MERGE (admin:AdministeredMeds {stay_id: $stay_id})
                SET admin.name = 'AdministeredMeds',
                    admin.medications = $medications,
                    admin.medication_count = $count,
                    admin.charttime = $charttime
                MERGE (ed)-[:ADMINISTERED_MEDICATIONS]->(admin)
                """
                session.run(query,
                          stay_id=str(stay_id),
                          charttime=first_charttime.strftime('%Y-%m-%d %H:%M:%S'),
                          medications=med_names,
                          count=len(med_names))
                
                logger.info(f"Processed {len(med_names)} administered medications for ED stay {stay_id}")
                
    except Exception as e:
        logger.error(f"Error processing administered meds: {e}")
        raise

def create_prescription_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '4_add_prescription_nodes'

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    PRESCRIPTIONS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'prescriptions.csv')

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load prescriptions
        presc_df = pd.read_csv(PRESCRIPTIONS_CSV)
        presc_df["starttime"] = pd.to_datetime(presc_df["starttime"], errors="coerce")
        presc_df["stoptime"] = pd.to_datetime(presc_df["stoptime"], errors="coerce")
        
        # Remove duplicate prescriptions before processing
        # Note: Only remove duplicates based on poe_id if it's not null
        # For rows with null poe_id, we keep all of them as they represent unique medicines
        initial_count = len(presc_df)
        # Keep rows with null poe_id, only deduplicate non-null poe_id rows
        null_poe = presc_df[presc_df['poe_id'].isna()]
        non_null_poe = presc_df[presc_df['poe_id'].notna()].drop_duplicates(subset=['poe_id'], keep='first')
        presc_df = pd.concat([non_null_poe, null_poe]).sort_index()
        final_count = len(presc_df)
        logger.info(f"Removed {initial_count - final_count} duplicate prescriptions. {final_count} unique prescriptions remaining.")
        
        # Sort by starttime for proper ordering
        presc_df = presc_df.sort_values(by='starttime')

        with driver.session() as session:
            # Delete any existing cross-connections before processing
            logger.info("Checking for and deleting cross-connections...")
            
            # Delete HAS_LAB_EVENTS relationships from Prescription nodes
            query1 = """
            MATCH (p)-[r:HAS_LAB_EVENTS]->()
            WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result1 = session.run(query1)
            count1 = result1.single()["deleted_count"]
            if count1 > 0:
                logger.info(f"Deleted {count1} HAS_LAB_EVENTS from Prescription nodes")
            
            # Delete HAS_PROCEDURES relationships from Prescription nodes
            query2 = """
            MATCH (p)-[r:HAS_PROCEDURES]->()
            WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result2 = session.run(query2)
            count2 = result2.single()["deleted_count"]
            if count2 > 0:
                logger.info(f"Deleted {count2} HAS_PROCEDURES from Prescription nodes")
            
            # Delete ANY remaining relationships between Prescription and Procedure nodes
            query3 = """
            MATCH (p)-[r]-(proc)
            WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
              AND (proc:Procedure OR proc:ProceduresBatch)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result3 = session.run(query3)
            count3 = result3.single()["deleted_count"]
            if count3 > 0:
                logger.info(f"Deleted {count3} connections between Prescription and Procedures")
            
            # Delete ANY remaining relationships between Prescription and LabEvents nodes
            query4 = """
            MATCH (p)-[r]-(lab)
            WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
              AND (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result4 = session.run(query4)
            count4 = result4.single()["deleted_count"]
            if count4 > 0:
                logger.info(f"Deleted {count4} connections between Prescription and LabEvents")
            
            # Delete incorrect ISSUED_PRESCRIPTIONS relationships from non-event nodes
            query5 = """
            MATCH (n)-[r:ISSUED_PRESCRIPTIONS]->(pb:PrescriptionsBatch)
            WHERE NOT (n:UnitAdmission OR n:EmergencyDepartment OR n:Discharge OR n:ICUStay OR n:HospitalAdmission)
            DELETE r
            RETURN count(r) as deleted_count
            """
            result5 = session.run(query5)
            count5 = result5.single()["deleted_count"]
            if count5 > 0:
                logger.info(f"Deleted {count5} incorrect HAS_PRESCRIPTIONS relationships from non-event nodes")
            
            total_deleted = count1 + count2 + count3 + count4 + count5
            if total_deleted > 0:
                logger.info(f"Total cross-connections deleted: {total_deleted}")
            else:
                logger.info("No cross-connections found.")
            
            # Fetch all event nodes with intime/outtime and subject_id
            query_events = """
            MATCH (e)
            WHERE e.intime IS NOT NULL AND e.outtime IS NOT NULL AND e.subject_id IS NOT NULL
            RETURN e.event_id AS event_id, e.subject_id AS subject_id, e.intime AS intime, e.outtime AS outtime
            """
            events = session.run(query_events)

            event_list = [
                {
                    "event_id": record["event_id"],
                    "subject_id": record["subject_id"],
                    "intime": pd.to_datetime(record["intime"]),
                    "outtime": pd.to_datetime(record["outtime"]),
                }
                for record in events
            ]

            logger.info(f"Found {len(event_list)} events with intime/outtime")
            
            # Check for existing prescriptions (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            events_with_prescriptions = checker.get_events_with_prescriptions()
            skipped_events = 0
            processed_events = []

            # Track which prescription rows have been assigned to events
            assigned_prescription_indices = set()
            
            # Track created prescription batches and nodes for reporting
            created_batches = []
            created_prescriptions = []
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()

            # Iterate over events and associate prescriptions
            for event in event_list:
                event_id = str(event["event_id"])
                subject_id = event.get("subject_id")
                intime = event["intime"]
                outtime = event["outtime"]
                
                # Extract subject_id and check per-patient, per-script tracking
                subject_id_int = None
                if subject_id is not None:
                    try:
                        subject_id_int = int(subject_id)
                        # Check if this patient was already processed by this script
                        if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                            skipped_patients.add(subject_id_int)
                            # Still check event-level to avoid duplicate work
                            if event_id in events_with_prescriptions:
                                skipped_events += 1
                                if skipped_events == 1 or skipped_events % 100 == 0:
                                    logger.info(f"Skipping event {event_id} (patient {subject_id_int} already processed by {SCRIPT_NAME}). Total skipped: {skipped_events}")
                                continue
                    except (ValueError, TypeError):
                        pass
                
                # Skip if event already has prescriptions (incremental load)
                if event_id in events_with_prescriptions:
                    skipped_events += 1
                    if skipped_events == 1 or skipped_events % 100 == 0:
                        logger.info(f"Skipping event {event_id} - already has prescriptions (incremental load). Total skipped: {skipped_events}")
                    continue
                
                processed_events.append(event_id)

                # Filter prescriptions within the event period
                presc_for_event = presc_df[
                    (presc_df["starttime"] >= intime) & (presc_df["starttime"] <= outtime)
                ].sort_values(by="starttime")
                
                # Track indices of prescriptions that were assigned
                for idx in presc_for_event.index:
                    assigned_prescription_indices.add(idx)

                if presc_for_event.empty:
                    continue

                # Create PrescriptionsBatch node (central container) and link it to the Event
                query_prescriptions_batch = """
                MATCH (e {event_id: $event_id})
                WHERE e:UnitAdmission OR e:EmergencyDepartment OR e:Discharge OR e:ICUStay OR e:HospitalAdmission
                MERGE (pb:PrescriptionsBatch {event_id: $event_id})
                ON CREATE SET pb.name = "PrescriptionsBatch"
                MERGE (e)-[:ISSUED_PRESCRIPTIONS]->(pb)
                RETURN pb.event_id AS batch_id, COUNT(e) AS event_matched
                """
                result = session.run(query_prescriptions_batch, event_id=event_id)
                batch_record = result.single()
                if batch_record:
                    created_batches.append({
                        'event_id': event_id,
                        'matched': batch_record['event_matched'] > 0
                    })

                # Group prescriptions by starttime to create separate Prescription nodes
                prescription_groups = presc_for_event.groupby('starttime')
                prescription_counter = 1
                
                for starttime, prescription_medicines in prescription_groups:
                    # Build array of formatted medicine strings for this starttime
                    medicines = []
                    for _, row in prescription_medicines.iterrows():
                        # Format: "drug dose_val_rx dose_unit_rx route doses_per_24_hrs"
                        # Example: "GuaiFENesin 5-10 mL PO/NG 6x/day"
                        
                        drug = str(row.get("drug")) if pd.notna(row.get("drug")) else "Unknown"
                        dose_val_rx = str(row.get("dose_val_rx")) if pd.notna(row.get("dose_val_rx")) else ""
                        dose_unit_rx = str(row.get("dose_unit_rx")) if pd.notna(row.get("dose_unit_rx")) else ""
                        route = str(row.get("route")) if pd.notna(row.get("route")) else ""
                        doses_per_24_hrs = row.get("doses_per_24_hrs") if pd.notna(row.get("doses_per_24_hrs")) else ""
                        
                        # Build dose part (e.g., "5-10 mL")
                        dose_part = f"{dose_val_rx} {dose_unit_rx}".strip() if dose_val_rx or dose_unit_rx else ""
                        
                        # Build frequency part (e.g., "6x/day")
                        frequency_part = f"{doses_per_24_hrs}x/day" if doses_per_24_hrs else ""
                        
                        # Combine all parts
                        parts = [drug]
                        if dose_part:
                            parts.append(dose_part)
                        if route:
                            parts.append(route)
                        if frequency_part:
                            parts.append(frequency_part)
                        
                        medicine_str = " ".join(parts)
                        medicines.append(medicine_str)

                    # Create Prescription node with array of medicine strings for this starttime
                    query_prescription = """
                    MERGE (p:Prescription {event_id: $event_id, starttime: $starttime})
                    SET p.medicines = $medicines,
                        p.medicine_count = $count,
                        p.name = 'Prescription'
                    """
                    session.run(query_prescription, event_id=event_id, 
                               starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'),
                               medicines=medicines, count=len(medicines))
                    
                    created_prescriptions.append({
                        'event_id': event_id,
                        'starttime': starttime,
                        'medicine_count': len(medicines)
                    })
                    
                    # Link Prescription → PrescriptionsBatch
                    query_link_prescription = """
                    MATCH (pb:PrescriptionsBatch {event_id: $event_id})
                    MATCH (p:Prescription {event_id: $event_id, starttime: $starttime})
                    MERGE (pb)-[:CONTAINED_PRESCRIPTION]->(p)
                    """
                    session.run(query_link_prescription, event_id=event_id, 
                               starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    prescription_counter += 1
                
                logger.info(f"Processed {len(presc_for_event)} medicines in {len(prescription_groups)} prescriptions for event {event_id}")
                
                # Track patient as processed if we have subject_id
                if subject_id_int is not None:
                    processed_patients.add(subject_id_int)

            # Query database to check actual connectivity
            logger.info("")
            logger.info("Verifying prescription connectivity in database...")
            
            # Count total PrescriptionsBatch nodes
            query_total_batches = """
            MATCH (pb:PrescriptionsBatch)
            RETURN count(pb) as total
            """
            total_batches = session.run(query_total_batches).single()['total']
            
            # Count connected PrescriptionsBatch nodes
            query_connected_batches = """
            MATCH (e)-[:ISSUED_PRESCRIPTIONS]->(pb:PrescriptionsBatch)
            WHERE e:UnitAdmission OR e:EmergencyDepartment OR e:Discharge OR e:ICUStay OR e:HospitalAdmission
            RETURN count(DISTINCT pb) as connected
            """
            connected_batches = session.run(query_connected_batches).single()['connected']
            
            # Count total Prescription nodes  
            query_total_prescriptions = """
            MATCH (p:Prescription)
            RETURN count(p) as total
            """
            total_prescription_nodes = session.run(query_total_prescriptions).single()['total']
            
            # Count connected Prescription nodes (connected to batch which is connected to event)
            query_connected_prescriptions = """
            MATCH (e)-[:ISSUED_PRESCRIPTIONS]->(pb:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(p:Prescription)
            WHERE e:UnitAdmission OR e:EmergencyDepartment OR e:Discharge OR e:ICUStay OR e:HospitalAdmission
            RETURN count(DISTINCT p) as connected
            """
            connected_prescription_nodes = session.run(query_connected_prescriptions).single()['connected']
            
            # Find orphaned batches
            query_orphaned_batches = """
            MATCH (pb:PrescriptionsBatch)
            WHERE NOT exists((pb)<-[:ISSUED_PRESCRIPTIONS]-())
            RETURN pb.event_id AS event_id
            LIMIT 10
            """
            orphaned_batches = list(session.run(query_orphaned_batches))
            
            logger.info(f"")
            logger.info(f"=" * 80)
            logger.info(f"PRESCRIPTION CONNECTIVITY REPORT")
            logger.info(f"=" * 80)
            logger.info(f"PrescriptionsBatch nodes:")
            logger.info(f"  - Total created: {total_batches}")
            logger.info(f"  - Connected to events: {connected_batches}")
            logger.info(f"  - Orphaned (not connected): {total_batches - connected_batches}")
            logger.info(f"")
            logger.info(f"Prescription nodes:")
            logger.info(f"  - Total created: {total_prescription_nodes}")
            logger.info(f"  - Connected to knowledge graph: {connected_prescription_nodes}")
            logger.info(f"  - Orphaned (not connected): {total_prescription_nodes - connected_prescription_nodes}")
            logger.info(f"")
            
            if orphaned_batches:
                logger.warning(f"Sample of orphaned PrescriptionsBatch nodes (first 10):")
                for record in orphaned_batches:
                    logger.warning(f"  - event_id: {record['event_id']}")
                logger.warning(f"")
                logger.warning(f"These batches were created but failed to connect to their event nodes.")
                logger.warning(f"This usually means the event_id doesn't match any existing event in the graph.")
            
            logger.info(f"=" * 80)
            logger.info(f"")

            # Report on unassigned prescriptions (medicine rows)
            unassigned_prescriptions = presc_df[~presc_df.index.isin(assigned_prescription_indices)]
            
            if not unassigned_prescriptions.empty:
                logger.warning(f"=" * 80)
                logger.warning(f"UNASSIGNED PRESCRIPTIONS REPORT")
                logger.warning(f"=" * 80)
                logger.warning(f"Total prescriptions not connected to any event: {len(unassigned_prescriptions)}")
                logger.warning(f"Total prescriptions processed: {len(presc_df)}")
                logger.warning(f"Percentage unassigned: {(len(unassigned_prescriptions)/len(presc_df)*100):.2f}%")
                logger.warning("")
                
                # Group unassigned by hadm_id
                unassigned_by_admission = unassigned_prescriptions.groupby('hadm_id').size().reset_index(name='count')
                logger.warning(f"Unassigned prescriptions by hospital admission:")
                for _, row in unassigned_by_admission.iterrows():
                    hadm_id = row['hadm_id']
                    count = row['count']
                    if pd.notna(hadm_id):
                        logger.warning(f"  - Hospital Admission {int(hadm_id)}: {count} unassigned prescription(s)")
                    else:
                        logger.warning(f"  - No Hospital Admission (NULL): {count} unassigned prescription(s)")
                
                logger.warning("")
                logger.warning("Sample of unassigned prescriptions (first 10):")
                sample = unassigned_prescriptions.head(10)
                for idx, row in sample.iterrows():
                    hadm_id = f"{int(row['hadm_id'])}" if pd.notna(row['hadm_id']) else "NULL"
                    drug = row['drug'] if pd.notna(row['drug']) else "Unknown"
                    starttime = row['starttime'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['starttime']) else "NULL"
                    logger.warning(f"  - hadm_id={hadm_id}, drug='{drug}', starttime={starttime}")
                
                logger.warning("")
                logger.warning("NOTE: These prescriptions have starttime values that fall outside all event time windows.")
                logger.warning("They may represent prescriptions ordered before admission, after discharge, or during")
                logger.warning("time periods not covered by event nodes (UnitAdmission, ED, Discharge, ICUStay).")
                logger.warning("Consider linking them directly to HospitalAdmission nodes if needed.")
                logger.warning(f"=" * 80)
            else:
                logger.info("✓ All prescriptions successfully connected to events!")
            
            # Log incremental load summary
            if skipped_events > 0:
                logger.info(f"Incremental load summary: Skipped {skipped_events} events that already have prescriptions")
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")

        logger.info("All prescriptions processed successfully!")

        # Process previous prescription meds
        create_previous_prescription_meds(driver)
        logger.info("Previous prescription meds processed successfully!")

        # Process administered meds
        create_administered_meds(driver)
        logger.info("Administered meds processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()


if __name__ == "__main__":
    create_prescription_nodes()
