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

def parse_diagnoses(diagnosis_string):
    """Parse comma-separated diagnoses and return a list of clean diagnosis names"""
    if pd.isna(diagnosis_string) or not diagnosis_string:
        return []
    
    # Split by comma and clean up each diagnosis
    diagnoses = [d.strip() for d in str(diagnosis_string).split(',')]
    # Filter out empty strings
    diagnoses = [d for d in diagnoses if d]
    return diagnoses

def create_ed_diagnosis_nodes(neo4j_conn):
    """Create diagnosis nodes for Emergency Department visits"""
    logger.info("Processing ED diagnoses...")
    
    # File path for ED diagnosis (relative to script location)
    project_root = script_dir.parent.parent.parent
    ED_DIAGNOSIS_CSV = project_root / 'Filtered_Data' / 'ed' / 'diagnosis.csv'
    
    try:
        # Load ED diagnosis data
        ed_diag_df = pd.read_csv(str(ED_DIAGNOSIS_CSV))
        
        # Group diagnoses by stay_id
        grouped_diagnoses = ed_diag_df.groupby('stay_id').agg({
            'icd_title': lambda x: list(x),
            'subject_id': 'first'  # Take the first subject_id for each stay
        }).reset_index()
        
        with neo4j_conn.session() as session:
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

def add_primary_secondary_diagnoses(neo4j_conn):
    """Add primary and secondary diagnoses as arrays to Diagnosis nodes from clinical notes"""
    logger.info("Processing primary and secondary diagnoses from clinical notes...")
    
    # File path (relative to script location)
    project_root = script_dir.parent.parent.parent
    CLINICAL_NOTES_CSV = project_root / 'Filtered_Data' / 'note' / 'discharge_clinical_note_flattened.csv'
    
    try:
        # Check if file exists
        if not CLINICAL_NOTES_CSV.exists():
            logger.warning(f"Clinical notes file not found: {CLINICAL_NOTES_CSV}")
            logger.warning("Skipping primary and secondary diagnoses processing")
            return
        
        # Load clinical notes data
        clinical_notes_df = pd.read_csv(str(CLINICAL_NOTES_CSV))
        
        # Filter to records with hadm_id
        clinical_notes_df = clinical_notes_df[clinical_notes_df['hadm_id'].notna()]
        
        logger.info(f"Found {len(clinical_notes_df)} clinical note records with hadm_id")
        
        with neo4j_conn.session() as session:
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

def aggregate_patient_diagnoses(neo4j_conn):
    """Aggregate all diagnoses for each patient and update Patient node with all_diagnoses attribute"""
    logger.info("Aggregating all diagnoses for each patient...")
    
    try:
        with neo4j_conn.session() as session:
            # Get all unique patients who have Diagnosis nodes (query by subject_id on Diagnosis nodes)
            query_patients = """
            MATCH (diag:Diagnosis)
            WHERE diag.subject_id IS NOT NULL
            RETURN DISTINCT diag.subject_id AS subject_id
            ORDER BY diag.subject_id
            """
            
            result = session.run(query_patients)
            patient_ids = [record["subject_id"] for record in result if record["subject_id"] is not None]
            
            logger.info(f"Found {len(patient_ids)} patients with diagnoses to process")
            
            if not patient_ids:
                logger.info("No patients with diagnoses found. Skipping aggregation.")
                return
            
            updated_count = 0
            skipped_count = 0
            total_diagnoses = 0
            
            pbar = tqdm(total=len(patient_ids), desc="Aggregating patient diagnoses", unit="patient")
            
            for subject_id in patient_ids:
                try:
                    # Query all Diagnosis nodes for this patient (from both Discharge and EmergencyDepartment)
                    # Diagnosis nodes have subject_id attribute, so we can query directly
                    query_diagnoses = """
                    MATCH (diag:Diagnosis {subject_id: $subject_id})
                    RETURN diag.complete_diagnosis AS complete_diagnosis,
                           diag.primary_diagnoses AS primary_diagnoses,
                           diag.secondary_diagnoses AS secondary_diagnoses
                    """
                    
                    result = session.run(query_diagnoses, subject_id=subject_id)
                    records = list(result)
                    
                    if not records:
                        skipped_count += 1
                        pbar.update(1)
                        continue
                    
                    # Collect all diagnosis strings
                    all_diagnoses = []
                    
                    for record in records:
                        # Process complete_diagnosis (array)
                        complete_diag = record["complete_diagnosis"]
                        if complete_diag:
                            if isinstance(complete_diag, list):
                                all_diagnoses.extend([str(d).strip() for d in complete_diag if d and str(d).strip()])
                            else:
                                diag_str = str(complete_diag).strip()
                                if diag_str:
                                    all_diagnoses.append(diag_str)
                        
                        # Process primary_diagnoses (array)
                        primary_diag = record["primary_diagnoses"]
                        if primary_diag:
                            if isinstance(primary_diag, list):
                                all_diagnoses.extend([str(d).strip() for d in primary_diag if d and str(d).strip()])
                            else:
                                diag_str = str(primary_diag).strip()
                                if diag_str:
                                    all_diagnoses.append(diag_str)
                        
                        # Process secondary_diagnoses (array)
                        secondary_diag = record["secondary_diagnoses"]
                        if secondary_diag:
                            if isinstance(secondary_diag, list):
                                all_diagnoses.extend([str(d).strip() for d in secondary_diag if d and str(d).strip()])
                            else:
                                diag_str = str(secondary_diag).strip()
                                if diag_str:
                                    all_diagnoses.append(diag_str)
                    
                    # Remove duplicates while preserving order (case-insensitive)
                    seen = set()
                    unique_diagnoses = []
                    for diag in all_diagnoses:
                        diag_lower = diag.lower()
                        if diag_lower not in seen and diag:  # Also filter out empty strings
                            seen.add(diag_lower)
                            unique_diagnoses.append(diag)
                    
                    # Update Patient node with aggregated diagnoses
                    if unique_diagnoses:
                        update_query = """
                        MATCH (p:Patient {subject_id: $subject_id})
                        SET p.all_diagnoses = $all_diagnoses
                        RETURN p
                        """
                        
                        session.run(update_query, 
                                   subject_id=subject_id,
                                   all_diagnoses=unique_diagnoses)
                        
                        updated_count += 1
                        total_diagnoses += len(unique_diagnoses)
                        logger.debug(f"Updated Patient {subject_id} with {len(unique_diagnoses)} unique diagnoses")
                    else:
                        skipped_count += 1
                        logger.debug(f"No diagnoses found for Patient {subject_id}")
                    
                except Exception as e:
                    logger.error(f"Error aggregating diagnoses for patient {subject_id}: {e}")
                    skipped_count += 1
                
                pbar.update(1)
            
            pbar.close()
            
            logger.info(f"Patient Diagnosis Aggregation Summary:")
            logger.info(f"  Patients updated: {updated_count}")
            logger.info(f"  Total unique diagnoses across all patients: {total_diagnoses}")
            logger.info(f"  Patients skipped: {skipped_count}")
            
    except Exception as e:
        logger.error(f"Error aggregating patient diagnoses: {e}")
        raise

def create_diagnosis_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_diagnosis_nodes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    SCRIPT_NAME = '6_add_diagnosis_nodes'

    # File paths (relative to script location)
    project_root = script_dir.parent.parent.parent
    DIAGNOSES_CSV = project_root / 'Filtered_Data' / 'hosp' / 'diagnoses_icd.csv'
    ICD_LOOKUP_CSV = project_root / 'Filtered_Data' / 'hosp' / 'd_icd_diagnoses.csv'

    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()

    # Load CSVs
    diag_df = pd.read_csv(str(DIAGNOSES_CSV))
    icd_lookup = pd.read_csv(str(ICD_LOOKUP_CSV))

    # Merge to add long_title
    diag_df = diag_df.merge(icd_lookup, on=["icd_code", "icd_version"], how="left")

    try:
        with neo4j_conn.session() as session:
            # Check for existing diagnoses (incremental load support)
            checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
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
            failed_patients = []
            skipped_patients = set()
            
            # Convert to list to get length for progress bar
            discharge_list = list(discharges)
            pbar = tqdm(total=len(discharge_list), desc="Adding diagnosis nodes", unit="discharge")
            
            for record in discharge_list:
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
                        pbar.update(1)
                        pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                        continue
                
                # Skip if discharge already has diagnoses (incremental load)
                if str(event_id) in discharges_with_diagnoses:
                    skipped_count += 1
                    pbar.update(1)
                    pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
                    continue
                
                processed_count += 1
                try:
                    # Filter diagnoses for this admission (diagnoses are linked to admissions, not events)
                    diags_for_admission = diag_df[
                        (diag_df["subject_id"] == subject_id_int) &
                        (diag_df["hadm_id"] == hadm_id_int)
                    ].sort_values(by="seq_num")

                    if diags_for_admission.empty:
                        pbar.update(1)
                        pbar.set_postfix({'Processed': processed_count, 'Skipped': skipped_count, 'Failed': len(failed_patients)})
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

                    # Mark patient as processed immediately after successful processing
                    if tracker:
                        try:
                            tracker.mark_patient_processed(subject_id_int, SCRIPT_NAME, status='success')
                            processed_patients.add(subject_id_int)
                        except Exception as e:
                            logger.error(f"Error marking patient {subject_id_int} as processed in tracker: {e}")
                except Exception as e:
                    logger.error(f"Error processing discharge {event_id} for patient {subject_id_int}: {e}")
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
            
            if skipped_patients:
                logger.info(f"Skipped {len(skipped_patients)} patients that were already processed by {SCRIPT_NAME} (tracker)")
            
            # Log incremental load summary
            if skipped_count > 0:
                logger.info(f"Incremental load summary: Processed {processed_count} discharge events, skipped {skipped_count} discharge events with existing diagnoses")

        logger.info("All diagnoses processed successfully!")

    finally:
        # Don't close the connection here as we need it for ED diagnoses
        pass

    try:
        # Process ED diagnoses
        create_ed_diagnosis_nodes(neo4j_conn)
        
        # Process primary and secondary diagnoses from clinical notes
        add_primary_secondary_diagnoses(neo4j_conn)
        
        # Aggregate all diagnoses for each patient and update Patient nodes
        aggregate_patient_diagnoses(neo4j_conn)
    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    create_diagnosis_nodes()
