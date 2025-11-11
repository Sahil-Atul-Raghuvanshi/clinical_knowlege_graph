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

def parse_diagnoses(diagnosis_string):
    """Parse comma-separated diagnoses and return a list of clean diagnosis names"""
    if pd.isna(diagnosis_string) or not diagnosis_string:
        return []
    
    # Split by comma and clean up each diagnosis
    diagnoses = [d.strip() for d in str(diagnosis_string).split(',')]
    # Filter out empty strings
    diagnoses = [d for d in diagnoses if d]
    return diagnoses

def create_ed_diagnosis_nodes(driver):
    """Create diagnosis nodes for Emergency Department visits"""
    logger.info("Processing ED diagnoses...")
    
    # File path for ED diagnosis (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    ED_DIAGNOSIS_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'diagnosis.csv')
    
    try:
        # Load ED diagnosis data
        ed_diag_df = pd.read_csv(ED_DIAGNOSIS_CSV)
        
        # Group diagnoses by stay_id
        grouped_diagnoses = ed_diag_df.groupby('stay_id').agg({
            'icd_title': lambda x: list(x),
            'subject_id': 'first'  # Take the first subject_id for each stay
        }).reset_index()
        
        with driver.session() as session:
            for _, row in grouped_diagnoses.iterrows():
                stay_id = str(row['stay_id'])
                subject_id = str(row['subject_id'])
                diagnosis_titles = row['icd_title']  # Already a list from groupby
                
                # Create Diagnosis node and link it to EmergencyDepartment
                query_ed_diagnosis = """
                MATCH (ed:EmergencyDepartment {event_id:$stay_id})
                MERGE (diag:Diagnosis {
                    event_id:$stay_id,
                    subject_id:$subject_id,
                    ed_diagnosis:true
                })
                SET diag.name = 'Diagnosis',
                    diag.complete_diagnosis = $titles,
                    diag.diagnosis_count = $count
                MERGE (ed)-[:RECORDED_DIAGNOSES]->(diag)
                """
                session.run(query_ed_diagnosis,
                          stay_id=stay_id,
                          subject_id=subject_id,
                          titles=diagnosis_titles,
                          count=len(diagnosis_titles))
                
                logger.info(f"Added {len(diagnosis_titles)} ED diagnoses for stay_id {stay_id}")
                
    except Exception as e:
        logger.error(f"Error processing ED diagnoses: {e}")
        raise

def add_primary_secondary_diagnoses(driver):
    """Add primary and secondary diagnoses as arrays to Diagnosis nodes from clinical notes"""
    logger.info("Processing primary and secondary diagnoses from clinical notes...")
    
    # File path (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    CLINICAL_NOTES_CSV = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_flattened.csv')
    
    try:
        # Check if file exists
        if not os.path.exists(CLINICAL_NOTES_CSV):
            logger.warning(f"Clinical notes file not found: {CLINICAL_NOTES_CSV}")
            logger.warning("Skipping primary and secondary diagnoses processing")
            return
        
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(CLINICAL_NOTES_CSV)
        
        # Filter to records with hadm_id
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} clinical note records with hadm_id")
        
        with driver.session() as session:
            updated_count = 0
            skipped_count = 0
            total_primary = 0
            total_secondary = 0
            
            for _, row in clinical_notes_df.iterrows():
                hadm_id = int(row['hadm_id'])
                
                # Parse primary and secondary diagnoses
                primary_diagnoses = parse_diagnoses(row.get('primary_diagnoses'))
                secondary_diagnoses = parse_diagnoses(row.get('secondary_diagnoses'))
                
                # Skip if both are empty
                if not primary_diagnoses and not secondary_diagnoses:
                    skipped_count += 1
                    continue
                
                # Update Diagnosis node with primary and secondary diagnoses arrays
                update_query = """
                MATCH (d:Discharge {hadm_id: $hadm_id})-[:RECORDED_DIAGNOSES]->(diag:Diagnosis)
                SET diag.primary_diagnoses = $primary_diagnoses,
                    diag.secondary_diagnoses = $secondary_diagnoses,
                    diag.primary_count = $primary_count,
                    diag.secondary_count = $secondary_count
                RETURN diag
                """
                
                result = session.run(update_query,
                                   hadm_id=hadm_id,
                                   primary_diagnoses=primary_diagnoses,
                                   secondary_diagnoses=secondary_diagnoses,
                                   primary_count=len(primary_diagnoses),
                                   secondary_count=len(secondary_diagnoses))
                
                if result.single():
                    updated_count += 1
                    total_primary += len(primary_diagnoses)
                    total_secondary += len(secondary_diagnoses)
                    logger.info(f"Updated Diagnosis for hadm_id {hadm_id}: {len(primary_diagnoses)} primary, {len(secondary_diagnoses)} secondary")
                else:
                    logger.warning(f"No Diagnosis node found for hadm_id {hadm_id}")
                    skipped_count += 1
            
            logger.info(f"Primary/Secondary Diagnoses Summary:")
            logger.info(f"  Diagnosis nodes updated: {updated_count}")
            logger.info(f"  Total primary diagnoses: {total_primary}")
            logger.info(f"  Total secondary diagnoses: {total_secondary}")
            logger.info(f"  Skipped: {skipped_count}")
                
    except Exception as e:
        logger.error(f"Error processing primary/secondary diagnoses: {e}")
        raise

def create_diagnosis_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j configuration
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    SCRIPT_NAME = '6_add_diagnosis_nodes'

    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

    # File paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    DIAGNOSES_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'diagnoses_icd.csv')
    ICD_LOOKUP_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'd_icd_diagnoses.csv')

    # Load CSVs
    diag_df = pd.read_csv(DIAGNOSES_CSV)
    icd_lookup = pd.read_csv(ICD_LOOKUP_CSV)

    # Merge to add long_title
    diag_df = diag_df.merge(icd_lookup, on=["icd_code", "icd_version"], how="left")

    try:
        with driver.session() as session:
            # Check for existing diagnoses (incremental load support)
            checker = IncrementalLoadChecker(driver, tracker=tracker)
            discharges_with_diagnoses = set()
            
            # Get discharge events that already have Diagnosis nodes
            query_existing = """
            MATCH (d:Discharge)-[:RECORDED_DIAGNOSES]->(diag:Diagnosis)
            RETURN DISTINCT d.event_id AS event_id
            """
            result = session.run(query_existing)
            discharges_with_diagnoses = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(discharges_with_diagnoses)} discharge events with existing diagnoses")
            
            # Fetch all discharge nodes with their associated hadm_id
            query_discharges = """
            MATCH (d:Discharge)
            RETURN d.event_id AS event_id, d.hadm_id AS hadm_id, d.subject_id AS subject_id
            """
            discharges = session.run(query_discharges)
            
            skipped_count = 0
            processed_count = 0
            
            # Track processed patients for this script (per-patient, per-script tracking)
            processed_patients = set()
            skipped_patients = set()

            for record in discharges:
                event_id = record["event_id"]
                hadm_id_raw = record["hadm_id"]
                subject_id_raw = record["subject_id"]
                
                if event_id is None or hadm_id_raw is None or subject_id_raw is None:
                    logger.warning(f"Skipping discharge with missing IDs: event_id={event_id}, hadm_id={hadm_id_raw}, subject_id={subject_id_raw}")
                    continue
                
                hadm_id = str(hadm_id_raw).strip()
                subject_id = str(subject_id_raw).strip()
                
                try:
                    hadm_id_int = int(hadm_id)
                    subject_id_int = int(subject_id)
                except ValueError:
                    logger.warning(f"Skipping discharge with invalid ID format: hadm_id={hadm_id}, subject_id={subject_id}")
                    continue
                
                # Check per-patient, per-script tracking first
                if tracker and tracker.is_patient_processed(subject_id_int, SCRIPT_NAME):
                    skipped_patients.add(subject_id_int)
                    # Still check event-level to avoid duplicate work
                    if str(event_id) in discharges_with_diagnoses:
                        skipped_count += 1
                        if skipped_count == 1 or skipped_count % 100 == 0:
                            logger.info(f"Skipping discharge {event_id} (patient {subject_id_int} already processed by {SCRIPT_NAME}). Total skipped: {skipped_count}")
                        continue
                
                # Skip if discharge already has diagnoses (incremental load)
                if str(event_id) in discharges_with_diagnoses:
                    skipped_count += 1
                    if skipped_count == 1 or skipped_count % 100 == 0:
                        logger.info(f"Skipping discharge {event_id} - already has diagnoses (incremental load). Total skipped: {skipped_count}")
                    continue
                
                processed_count += 1

                # Filter diagnoses for this admission (diagnoses are linked to admissions, not events)
                diags_for_admission = diag_df[
                    (diag_df["subject_id"] == subject_id_int) &
                    (diag_df["hadm_id"] == hadm_id_int)
                ].sort_values(by="seq_num")

                if diags_for_admission.empty:
                    continue

                # Build array of diagnosis titles
                diagnosis_titles = []
                for _, row in diags_for_admission.iterrows():
                    title = str(row["long_title"]) if pd.notna(row["long_title"]) else "Unknown"
                    diagnosis_titles.append(title)

                # Create Diagnosis node with array of titles and link it to the Discharge
                query_diagnosis = """
                MATCH (d:Discharge {event_id:$event_id})
                MERGE (diag:Diagnosis {event_id:$event_id, hadm_id:$hadm_id, subject_id:$subject_id})
                SET diag.name = 'Diagnosis',
                    diag.complete_diagnosis = $titles,
                    diag.diagnosis_count = $count
                MERGE (d)-[:RECORDED_DIAGNOSES]->(diag)
                """
                session.run(query_diagnosis, event_id=event_id, hadm_id=hadm_id_int, 
                           subject_id=subject_id_int, titles=diagnosis_titles, count=len(diags_for_admission))

                logger.info(f"Added {len(diags_for_admission)} diagnoses for discharge event {event_id} (admission {hadm_id})")
                
                # Track patient as processed
                processed_patients.add(subject_id_int)
            
            # Mark processed patients in tracker (per-patient, per-script tracking)
            if tracker and processed_patients:
                tracker.mark_patients_processed_batch(list(processed_patients), SCRIPT_NAME, status='success')
                logger.info(f"Marked {len(processed_patients)} patients as processed in tracker for script '{SCRIPT_NAME}' (incremental load: will skip these patients on next run)")
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            # Log incremental load summary
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {processed_count} discharge events, skipped {skipped_count} discharge events with existing diagnoses")

        logger.info("All diagnoses processed successfully!")

    finally:
        # Don't close the driver here as we need it for ED diagnoses
        pass

    try:
        # Process ED diagnoses
        create_ed_diagnosis_nodes(driver)
        
        # Process primary and secondary diagnoses from clinical notes
        add_primary_secondary_diagnoses(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    create_diagnosis_nodes()
