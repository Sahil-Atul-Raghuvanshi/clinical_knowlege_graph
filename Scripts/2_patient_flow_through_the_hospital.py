# patient_flow_through_the_hospital.py
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

def event_label(event_type):
    if event_type == "ed":
        return "EmergencyDepartment"
    elif event_type == "discharge":
        return "Discharge"
    elif event_type == "admit":
        return "UnitAdmission"
    elif event_type == "transfer":
        return "Transfer"
    return event_type.capitalize()

def event_relationship(prev_type, next_type):
    if next_type is None:
        next_type = "unknown"
    return f"LEADS_TO_{next_type.upper()}"

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

def create_patient_flow():
    # Get dynamic folder name
    folder_name = get_folder_name()
    
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File paths - dynamically constructed
    ADMISSIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\admissions.csv"
    TRANSFERS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\{folder_name}\transfers.csv"

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    try:
        # Load data
        admissions_df = pd.read_csv(ADMISSIONS_CSV)
        admissions_df["admittime"] = pd.to_datetime(admissions_df["admittime"], errors="coerce")
        admissions_df["dischtime"] = pd.to_datetime(admissions_df["dischtime"], errors="coerce")

        transfers_df = pd.read_csv(TRANSFERS_CSV)
        transfers_df["intime"] = pd.to_datetime(transfers_df["intime"], errors="coerce")
        transfers_df["outtime"] = pd.to_datetime(transfers_df["outtime"], errors="coerce")

        # Sort transfers
        transfers_df = transfers_df.sort_values(by=["subject_id", "hadm_id", "intime"]).reset_index(drop=True)

        with driver.session() as session:
            for subject_id, patient_admissions in admissions_df.groupby("subject_id"):
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

                last_discharge_id = None
                last_discharge_time = None

                for seq_num, adm_row in patient_admissions_sorted.iterrows():
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
                                name=str(hadm_id),
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
                                seq_num=seq_num + 1)  # sequence starts at 1

                    # Link previous discharge → current admission with gap
                    if last_discharge_id:
                        gap = calculate_gap(last_discharge_time, adm_intime)
                        query_gap = """
                        MATCH (d:Discharge {event_id: $discharge_id})
                        MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                        MERGE (d)-[r:LEADS_TO_HOSPITAL_ADMISSION]->(h)
                        ON CREATE SET r.gap = $gap
                        ON MATCH SET r.gap = $gap
                        """
                        session.run(query_gap,
                                    discharge_id=last_discharge_id,
                                    hadm_id=hadm_id,
                                    gap=gap)

                    # Handle transfers (unchanged from your code)
                    admission_transfers = patient_transfers[patient_transfers["hadm_id"] == hadm_id]
                    admission_transfers = admission_transfers.sort_values(by="intime").reset_index(drop=True)

                    previous_event_id = None
                    previous_event_type = None
                    first_event_id = None
                    first_event_type = None

                    for _, row in admission_transfers.iterrows():
                        event_id = str(int(row["transfer_id"]))
                        event_type = str(row["eventtype"]).lower() if pd.notna(row["eventtype"]) else "unknown"
                        careunit = row["careunit"] if pd.notna(row["careunit"]) else "Unknown"
                        intime = row["intime"]
                        outtime = row["outtime"]

                        intime_str = intime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(intime) else None
                        outtime_str = outtime.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(outtime) else None
                        period = human_readable_period(intime, outtime)
                        label = event_label(event_type)
                        
                        # Set name based on event type
                        if event_type in ["admit", "transfer", "ed"]:
                            name_value = careunit
                        elif event_type == "discharge":
                            name_value = "discharge"
                        else:
                            name_value = event_type

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
                            e.period = $period
                        ON MATCH SET
                            e.name = $name_value,
                            e.subject_id = $subject_id,
                            e.hadm_id = $hadm_id,
                            e.transfer_id = $transfer_id,
                            e.eventtype = $eventtype,
                            e.careunit = $careunit,
                            e.intime = $intime,
                            e.outtime = $outtime,
                            e.period = $period
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
                                    period=period)

                        if first_event_id is None:
                            first_event_id = event_id
                            first_event_type = event_type

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

                        if event_type == "discharge":
                            last_discharge_id = event_id
                            last_discharge_time = intime

                        previous_event_id = event_id
                        previous_event_type = event_type

                    if first_event_id:
                        query_link = """
                        MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                        MATCH (first_unit {event_id: $first_event_id})
                        MERGE (h)-[:HAS_UNIT_ADMISSION]->(first_unit)
                        """
                        session.run(query_link, hadm_id=hadm_id, first_event_id=first_event_id)

                    query_patient = """
                    MATCH (p:Patient {subject_id: $subject_id})
                    MATCH (h:HospitalAdmission {hadm_id: $hadm_id})
                    MERGE (p)-[:HAS_VISIT]->(h)
                    """
                    session.run(query_patient, subject_id=int(subject_id), hadm_id=hadm_id)

                    logger.info(f"Processed hospital admission {hadm_id} (Seq {seq_num+1}) for subject {subject_id}")

        logger.info("Patient flows with hospital admits, inter-admission links, sequence numbers, and total counts created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    create_patient_flow()
