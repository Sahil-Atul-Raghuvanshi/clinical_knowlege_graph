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

def create_prescription_nodes():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File path - dynamically constructed
    PRESCRIPTIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\prescriptions.csv"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load prescriptions
        presc_df = pd.read_csv(PRESCRIPTIONS_CSV)
        presc_df["starttime"] = pd.to_datetime(presc_df["starttime"], errors="coerce")
        presc_df["stoptime"] = pd.to_datetime(presc_df["stoptime"], errors="coerce")
        
        # Remove duplicate prescriptions before processing
        initial_count = len(presc_df)
        presc_df = presc_df.drop_duplicates(subset=['poe_id'], keep='first')
        final_count = len(presc_df)
        logger.info(f"Removed {initial_count - final_count} duplicate prescriptions. {final_count} unique prescriptions remaining.")
        
        # Sort by starttime for proper ordering
        presc_df = presc_df.sort_values(by='starttime')

        with driver.session() as session:
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

                # Create PrescriptionsBatch node (central node)
                query_prescriptions_batch = """
                MATCH (e {event_id: $event_id})
                WHERE NOT e:PrescriptionBatch AND NOT e:PrescriptionsBatch AND NOT e:ProceduresBatch AND NOT e:LabEventsBatch AND NOT e:LabEvents
                MERGE (pb:PrescriptionsBatch {event_id: $event_id})
                ON CREATE SET pb.name = "PrescriptionsBatch"
                MERGE (e)-[:HAS_PRESCRIPTIONS]->(pb)
                """
                session.run(query_prescriptions_batch, event_id=event_id)

                # Group prescriptions by starttime to create Prescription nodes
                prescription_groups = presc_for_event.groupby('starttime')
                prescription_counter = 1
                
                for starttime, prescription_medicines in prescription_groups:
                    # Create Prescription node
                    prescription_props = {
                        "event_id": event_id,
                        "starttime": starttime.strftime('%Y-%m-%d %H:%M:%S'),
                        "name": f"Prescription_{prescription_counter}",
                        "medicine_count": len(prescription_medicines)
                    }
                    
                    query_prescription = """
                    MERGE (p:Prescription {
                        event_id: $event_id,
                        starttime: $starttime
                    })
                    ON CREATE SET p.name = $name, p.medicine_count = $medicine_count
                    ON MATCH SET p.name = $name, p.medicine_count = $medicine_count
                    """
                    session.run(query_prescription, **prescription_props)
                    
                    # Link Prescription → PrescriptionsBatch
                    query_link_prescription = """
                    MATCH (pb:PrescriptionsBatch {event_id: $event_id})
                    MATCH (p:Prescription {event_id: $event_id, starttime: $starttime})
                    MERGE (pb)-[:HAS_PRESCRIPTION]->(p)
                    """
                    session.run(query_link_prescription, event_id=event_id, 
                               starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    # Create individual Medicine nodes
                    medicine_counter = 1
                    
                    for _, row in prescription_medicines.iterrows():
                        prescription_id = str(row["poe_id"]).strip() if pd.notna(row["poe_id"]) else None
                        if not prescription_id:
                            continue

                        raw_subject_id = str(row["subject_id"]).split("-")[0]
                        try:
                            subject_id = int(raw_subject_id)
                        except ValueError:
                            logger.warning(f"Skipping medicine with invalid subject_id: {row['subject_id']}")
                            continue

                        hadm_id = str(row["hadm_id"]).strip() if pd.notna(row["hadm_id"]) else None

                        # Prepare all properties for Medicine node
                        medicine_props = {
                            "poe_id": prescription_id,
                            "subject_id": subject_id,
                            "hadm_id": hadm_id,
                            "pharmacy_id": str(row.get("pharmacy_id")) if pd.notna(row.get("pharmacy_id")) else None,
                            "poe_seq": int(row.get("poe_seq")) if pd.notna(row.get("poe_seq")) else None,
                            "order_provider_id": str(row.get("order_provider_id")) if pd.notna(row.get("order_provider_id")) else None,
                            "starttime": row["starttime"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row["starttime"]) else None,
                            "stoptime": row["stoptime"].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row["stoptime"]) else None,
                            "drug_type": str(row.get("drug_type")) if pd.notna(row.get("drug_type")) else None,
                            "drug": str(row.get("drug")) if pd.notna(row.get("drug")) else None,
                            "formulary_drug_cd": str(row.get("formulary_drug_cd")) if pd.notna(row.get("formulary_drug_cd")) else None,
                            "gsn": str(row.get("gsn")) if pd.notna(row.get("gsn")) else None,
                            "ndc": str(row.get("ndc")) if pd.notna(row.get("ndc")) else None,
                            "prod_strength": str(row.get("prod_strength")) if pd.notna(row.get("prod_strength")) else None,
                            "form_rx": str(row.get("form_rx")) if pd.notna(row.get("form_rx")) else None,
                            "dose_val_rx": str(row.get("dose_val_rx")) if pd.notna(row.get("dose_val_rx")) else None,
                            "dose_unit_rx": str(row.get("dose_unit_rx")) if pd.notna(row.get("dose_unit_rx")) else None,
                            "form_val_disp": str(row.get("form_val_disp")) if pd.notna(row.get("form_val_disp")) else None,
                            "form_unit_disp": str(row.get("form_unit_disp")) if pd.notna(row.get("form_unit_disp")) else None,
                            "doses_per_24_hrs": float(row.get("doses_per_24_hrs")) if pd.notna(row.get("doses_per_24_hrs")) else None,
                            "route": str(row.get("route")) if pd.notna(row.get("route")) else None,
                            "name": f"Medicine_{medicine_counter}",
                        }

                        # Create/update Medicine node
                        query_medicine = """
                        MERGE (m:Medicine {poe_id: $poe_id})
                        ON CREATE SET m.subject_id = $subject_id, m.hadm_id = $hadm_id,
                                      m.pharmacy_id = $pharmacy_id, m.poe_seq = $poe_seq,
                                      m.order_provider_id = $order_provider_id,
                                      m.starttime = $starttime, m.stoptime = $stoptime,
                                      m.drug_type = $drug_type, m.drug = $drug,
                                      m.formulary_drug_cd = $formulary_drug_cd, m.gsn = $gsn,
                                      m.ndc = $ndc, m.prod_strength = $prod_strength,
                                      m.form_rx = $form_rx, m.dose_val_rx = $dose_val_rx,
                                      m.dose_unit_rx = $dose_unit_rx, m.form_val_disp = $form_val_disp,
                                      m.form_unit_disp = $form_unit_disp, m.doses_per_24_hrs = $doses_per_24_hrs,
                                      m.route = $route, m.name = $name
                        ON MATCH SET  m.subject_id = $subject_id, m.hadm_id = $hadm_id,
                                      m.pharmacy_id = $pharmacy_id, m.poe_seq = $poe_seq,
                                      m.order_provider_id = $order_provider_id,
                                      m.starttime = $starttime, m.stoptime = $stoptime,
                                      m.drug_type = $drug_type, m.drug = $drug,
                                      m.formulary_drug_cd = $formulary_drug_cd, m.gsn = $gsn,
                                      m.ndc = $ndc, m.prod_strength = $prod_strength,
                                      m.form_rx = $form_rx, m.dose_val_rx = $dose_val_rx,
                                      m.dose_unit_rx = $dose_unit_rx, m.form_val_disp = $form_val_disp,
                                      m.form_unit_disp = $form_unit_disp, m.doses_per_24_hrs = $doses_per_24_hrs,
                                      m.route = $route, m.name = $name
                        """
                        session.run(query_medicine, **medicine_props)

                        # Link Medicine → Prescription
                        query_link_medicine = """
                        MATCH (p:Prescription {event_id: $event_id, starttime: $starttime})
                        MATCH (m:Medicine {poe_id: $poe_id})
                        MERGE (p)-[:HAS_MEDICINE]->(m)
                        """
                        session.run(query_link_medicine, event_id=event_id, 
                                   starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'),
                                   poe_id=prescription_id)

                        medicine_counter += 1
                    
                    prescription_counter += 1
                
                logger.info(f"Processed {len(presc_for_event)} medicines in {len(prescription_groups)} prescriptions for event {event_id}")

        logger.info("All prescriptions processed successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()


if __name__ == "__main__":
    create_prescription_nodes()
