# patient_flow_through_the_hospital.py
import pandas as pd
from neo4j import GraphDatabase
import logging
import os

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
        return "Transfer"
    return event_type.capitalize()

def event_relationship(prev_type, next_type):
    """Generate relationship name based on next event type"""
    if next_type is None:
        next_type = "unknown"
    
    # Map event types to appropriate relationship names
    relationship_map = {
        "discharge": "LED_TO_DISCHARGE",
        "transfer": "LED_TO_TRANSFER",
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
    """Process ED stays for a patient, including those without hospital admissions"""
    # Filter ED stays for this patient
    patient_edstays = edstays_df[edstays_df["subject_id"] == subject_id].copy()
    patient_edstays = patient_edstays.sort_values(by="intime").reset_index(drop=True)
    
    for seq_num, stay in patient_edstays.iterrows():
        stay_id = str(stay["stay_id"])
        hadm_id = stay["hadm_id"] if pd.notna(stay["hadm_id"]) else None
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
                   ed_seq_num=seq_num + 1)
        
        # Always link Patient to ED (both standalone and admission-linked)
        query_patient = """
        MATCH (p:Patient {subject_id: $subject_id})
        MATCH (ed:EmergencyDepartment {event_id: $stay_id})
        MERGE (p)-[:VISITED_ED]->(ed)
        """
        session.run(query_patient, subject_id=int(subject_id), stay_id=stay_id)
        
        if pd.isna(hadm_id):
            logger.info(f"Processed standalone ED visit {stay_id} (Seq {seq_num+1}) for subject {subject_id}")
        else:
            logger.info(f"Processed ED visit {stay_id} (Seq {seq_num+1}) with hospital admission {hadm_id} for subject {subject_id}")

def create_patient_flow():
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "10016742"

    # File paths
    ADMISSIONS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\hosp\admissions.csv"
    TRANSFERS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\hosp\transfers.csv"
    SERVICES_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\hosp\services.csv"
    EDSTAYS_CSV = rf"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data\ed\edstays.csv"

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

        # Load ED stays data
        edstays_df = pd.read_csv(EDSTAYS_CSV)
        edstays_df["intime"] = pd.to_datetime(edstays_df["intime"], errors="coerce")
        edstays_df["outtime"] = pd.to_datetime(edstays_df["outtime"], errors="coerce")
        
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

            for subject_id in all_subjects:
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
                                seq_num=seq_num + 1)  # sequence starts at 1

                    # Link EmergencyDepartment to HospitalAdmission based on timing
                    # Check if this admission has ED registration time (meaning it came through ED)
                    if pd.notna(edregtime) and pd.notna(edouttime):
                        edregtime_dt = pd.to_datetime(edregtime)
                        edouttime_dt = pd.to_datetime(edouttime)
                        
                        # Find the ED visit for this admission
                        # ED visits have hadm_id that links to hospital admission
                        patient_ed = edstays_df[
                            (edstays_df["subject_id"] == subject_id) &
                            (edstays_df["hadm_id"] == hadm_id)
                        ]
                        
                        if not patient_ed.empty:
                            ed_stay_id = str(patient_ed.iloc[0]["stay_id"])
                            
                            # Determine relationship based on timing
                            if adm_intime >= edregtime_dt and adm_intime <= edouttime_dt:
                                # Admission happened DURING ED stay
                                relationship = "LED_TO_ADMISSION_DURING_STAY"
                                logger.info(f"Admission {hadm_id} occurred during ED stay {ed_stay_id}")
                            elif adm_intime > edouttime_dt:
                                # Admission happened AFTER ED discharge
                                relationship = "LED_TO_ADMISSION_AFTER_DISCHARGE"
                                logger.info(f"Admission {hadm_id} occurred after ED discharge from {ed_stay_id}")
                            else:
                                # Edge case: admission before ED out (shouldn't normally happen)
                                relationship = "LED_TO_ADMISSION"
                                logger.warning(f"Unusual timing: Admission {hadm_id} before ED out for {ed_stay_id}")
                            
                            # Create the relationship between ED and HospitalAdmission
                            query_ed_to_admission = f"""
                            MATCH (ed:EmergencyDepartment {{event_id: $ed_stay_id}})
                            MATCH (h:HospitalAdmission {{hadm_id: $hadm_id}})
                            MERGE (ed)-[:{relationship}]->(h)
                            """
                            session.run(query_ed_to_admission, ed_stay_id=ed_stay_id, hadm_id=hadm_id)

                    # Handle transfers (unchanged from your code)
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
                        
                        # Set name to label
                        name_value = label

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

                    logger.info(f"Processed hospital admission {hadm_id} (Seq {seq_num+1}) for subject {subject_id}")

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
                
                # Get all ED visits for this subject with their times
                patient_ed_stays = edstays_df[edstays_df["subject_id"] == subject_id].sort_values(by="intime")
                
                for discharge_record in discharge_list:
                    discharge_id = discharge_record["discharge_id"]
                    discharge_time = pd.to_datetime(discharge_record["discharge_time"])
                    
                    # Find the next ED visit after this discharge
                    subsequent_eds = patient_ed_stays[patient_ed_stays["intime"] > discharge_time]
                    
                    if not subsequent_eds.empty:
                        # Get the first ED visit after the discharge
                        next_ed = subsequent_eds.iloc[0]
                        next_ed_stay_id = str(next_ed["stay_id"])
                        next_ed_time = next_ed["intime"]
                        
                        # Calculate gap between discharge and ED visit
                        gap = calculate_gap(discharge_time, next_ed_time)
                        
                        # Create relationship from Discharge to EmergencyDepartment
                        query_discharge_to_ed = """
                        MATCH (d:Discharge {event_id: $discharge_id})
                        MATCH (ed:EmergencyDepartment {event_id: $ed_stay_id})
                        MERGE (d)-[r:LED_TO_ED_VISIT]->(ed)
                        ON CREATE SET r.gap = $gap
                        ON MATCH SET r.gap = $gap
                        """
                        session.run(query_discharge_to_ed, 
                                  discharge_id=discharge_id, 
                                  ed_stay_id=next_ed_stay_id,
                                  gap=gap)
                        
                        logger.info(f"Linked Discharge {discharge_id} to ED {next_ed_stay_id} with gap: {gap}")

        logger.info("Patient flows with hospital admits, inter-admission links, sequence numbers, and total counts created successfully!")

    except Exception as e:
        logger.error(f"An error occurred: {e}")

    finally:
        driver.close()

if __name__ == "__main__":
    create_patient_flow()
