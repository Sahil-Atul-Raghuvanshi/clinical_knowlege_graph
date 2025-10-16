# add_prescription_nodes.py
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

def create_previous_prescription_meds(driver, folder_name):
    """Create PreviousPrescriptionMeds nodes from medrecon data"""
    MEDRECON_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\medrecon.csv"
    
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
                SET prev.medications = $medications,
                    prev.medication_count = $count,
                    prev.charttime = $charttime
                MERGE (ed)-[:HAS_PREVIOUS_MEDS]->(prev)
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

def create_administered_meds(driver, folder_name):
    """Create AdministeredMeds nodes from pyxis data"""
    PYXIS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\pyxis.csv"
    
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
                SET admin.medications = $medications,
                    admin.medication_count = $count,
                    admin.charttime = $charttime
                MERGE (ed)-[:HAS_ADMINISTERED_MEDS]->(admin)
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

def create_prescription_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File paths - dynamically constructed
    PRESCRIPTIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\prescriptions.csv"
    MEDRECON_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\medrecon.csv"

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
            
            # Delete incorrect HAS_PRESCRIPTIONS relationships from non-event nodes
            query5 = """
            MATCH (n)-[r:HAS_PRESCRIPTIONS]->(pb:PrescriptionsBatch)
            WHERE NOT (n:UnitAdmission OR n:EmergencyDepartment OR n:Discharge)
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
            
            # Fetch all event nodes with intime/outtime
            query_events = """
            MATCH (e)
            WHERE e.intime IS NOT NULL AND e.outtime IS NOT NULL
            RETURN e.event_id AS event_id, e.intime AS intime, e.outtime AS outtime
            """
            events = session.run(query_events)

            event_list = [
                {
                    "event_id": record["event_id"],
                    "intime": pd.to_datetime(record["intime"]),
                    "outtime": pd.to_datetime(record["outtime"]),
                }
                for record in events
            ]

            logger.info(f"Found {len(event_list)} events with intime/outtime")

            # Iterate over events and associate prescriptions
            for event in event_list:
                event_id = event["event_id"]
                intime = event["intime"]
                outtime = event["outtime"]

                # Filter prescriptions within the event period
                presc_for_event = presc_df[
                    (presc_df["starttime"] >= intime) & (presc_df["starttime"] <= outtime)
                ].sort_values(by="starttime")

                if presc_for_event.empty:
                    continue

                # Create PrescriptionsBatch node (central container) and link it to the Event
                query_prescriptions_batch = """
                MATCH (e {event_id: $event_id})
                WHERE e:UnitAdmission OR e:EmergencyDepartment OR e:Discharge
                MERGE (pb:PrescriptionsBatch {event_id: $event_id})
                ON CREATE SET pb.name = "PrescriptionsBatch"
                MERGE (e)-[:HAS_PRESCRIPTIONS]->(pb)
                """
                session.run(query_prescriptions_batch, event_id=event_id)

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
                        p.name = $name
                    """
                    session.run(query_prescription, event_id=event_id, 
                               starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'),
                               medicines=medicines, count=len(medicines),
                               name=f"Prescription_{prescription_counter}")
                    
                    # Link Prescription → PrescriptionsBatch
                    query_link_prescription = """
                    MATCH (pb:PrescriptionsBatch {event_id: $event_id})
                    MATCH (p:Prescription {event_id: $event_id, starttime: $starttime})
                    MERGE (pb)-[:HAS_PRESCRIPTION]->(p)
                    """
                    session.run(query_link_prescription, event_id=event_id, 
                               starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    prescription_counter += 1
                
                logger.info(f"Processed {len(presc_for_event)} medicines in {len(prescription_groups)} prescriptions for event {event_id}")

        logger.info("All prescriptions processed successfully!")

        # Process previous prescription meds
        create_previous_prescription_meds(driver, folder_name)
        logger.info("Previous prescription meds processed successfully!")

        # Process administered meds
        create_administered_meds(driver, folder_name)
        logger.info("Administered meds processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()


if __name__ == "__main__":
    create_prescription_nodes()
