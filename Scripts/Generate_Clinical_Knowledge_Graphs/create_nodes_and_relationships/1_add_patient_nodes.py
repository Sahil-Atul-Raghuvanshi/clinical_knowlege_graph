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

def add_patient_nodes(tracker: Optional[ETLTracker] = None, pipeline_log_file: Optional[str] = None):
    # Setup logging based on whether pipeline_log_file is provided
    # Remove any existing handlers to avoid duplicates
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create temp_ prefixed log file
        log_file = logs_dir / 'temp_add_patient_nodes.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    # Load configuration
    config = Config()
    
    # Path to patient CSV file (relative to script location)
    project_root = script_dir.parent.parent.parent
    PATIENTS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'patients.csv'
    ADMISSIONS_CSV = project_root / 'Filtered_Data' / 'hosp' / 'admissions.csv'
    EDSTAYS_CSV = project_root / 'Filtered_Data' / 'ed' / 'edstays.csv'
    SCRIPT_NAME = '1_add_patient_nodes'
    
    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()
    
    try:
        # Test connection
        neo4j_conn.execute_query("RETURN 1 as test")
        logger.info(f"Connection successful!")
        
        # Load patient data
        df = pd.read_csv(str(PATIENTS_CSV))
        logger.info(f"Found {len(df)} patient records")
        
        # Load admissions and ED stays to filter patients
        # Only include patients who have at least one admission OR one ED visit
        patients_with_data = set()
        
        try:
            # Load admissions
            admissions_df = pd.read_csv(str(ADMISSIONS_CSV))
            if 'subject_id' in admissions_df.columns:
                admissions_df['subject_id'] = admissions_df['subject_id'].astype(int)
                patients_with_data.update(admissions_df['subject_id'].unique())
                logger.info(f"Found {len(admissions_df['subject_id'].unique())} unique patients with admissions")
        except FileNotFoundError:
            logger.warning(f"Admissions file not found: {ADMISSIONS_CSV}")
        except Exception as e:
            logger.warning(f"Error loading admissions file: {e}")
        
        try:
            # Load ED stays
            edstays_df = pd.read_csv(str(EDSTAYS_CSV))
            if 'subject_id' in edstays_df.columns:
                edstays_df['subject_id'] = edstays_df['subject_id'].astype(int)
                patients_with_data.update(edstays_df['subject_id'].unique())
                logger.info(f"Found {len(edstays_df['subject_id'].unique())} unique patients with ED visits")
        except FileNotFoundError:
            logger.warning(f"ED stays file not found: {EDSTAYS_CSV}")
        except Exception as e:
            logger.warning(f"Error loading ED stays file: {e}")
        
        # Filter patients to only include those with admissions or ED visits
        df['subject_id'] = df['subject_id'].astype(int)
        df_filtered = df[df['subject_id'].isin(patients_with_data)]
        excluded_count = len(df) - len(df_filtered)
        
        if excluded_count > 0:
            logger.info(f"Excluding {excluded_count} patients with no admissions and no ED visits")
        
        if len(df_filtered) == 0:
            logger.warning("No patients found with admissions or ED visits. Nothing to process.")
            return
        
        logger.info(f"Processing {len(df_filtered)} patients with admissions or ED visits")
        df = df_filtered
        
        # Check for existing patients (incremental load support)
        checker = IncrementalLoadChecker(neo4j_conn.driver, tracker=tracker, database=config.neo4j.database)
        existing_patients = checker.get_existing_patients()
        
        # Sync tracker: Mark patients as processed if they exist in Neo4j but tracker is missing entries
        if tracker and existing_patients:
            checker.sync_tracker_for_existing_patients(SCRIPT_NAME, existing_patients)
        
        # Filter out existing patients
        df_to_process = df[~df['subject_id'].isin(existing_patients)]
        skipped_count = len(df) - len(df_to_process)
        
        if skipped_count > 0:
            logger.info(f"Skipping {skipped_count} patients that already exist (incremental load)")
        if len(df_to_process) == 0:
            logger.info("All patients already exist. Nothing to process.")
            return
        
        logger.info(f"Processing {len(df_to_process)} new patients")
        
        # Track processed patients for logging
        processed_patients = []
        failed_patients = []
        
        # Create patient nodes
        with neo4j_conn.session() as session:
            pbar = tqdm(total=len(df_to_process), desc="Creating patient nodes", unit="patient")
            for _, row in df_to_process.iterrows():
                subject_id = int(row['subject_id'])
                try:
                    query = """
                    MERGE (p:Patient {subject_id: $subject_id})
                    ON CREATE SET 
                        p.name = 'Patient',
                        p.gender = $gender,
                        p.anchor_age = $anchor_age,
                        p.anchor_year = $anchor_year,
                        p.anchor_year_group = $anchor_year_group,
                        p.dod = $dod
                    ON MATCH SET
                        p.name = 'Patient'
                    """
                    
                    # Handle null values
                    dod = row['dod'] if pd.notna(row['dod']) else None
                    
                    session.run(query, 
                               subject_id=subject_id,
                               gender=row['gender'],
                               anchor_age=int(row['anchor_age']),
                               anchor_year=int(row['anchor_year']),
                               anchor_year_group=row['anchor_year_group'],
                               dod=dod)
                    
                    # Mark patient as processed immediately after successful creation
                    if tracker:
                        tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='success')
                    processed_patients.append(subject_id)
                except Exception as e:
                    logger.error(f"Error processing patient {subject_id}: {e}")
                    # Mark patient as failed immediately
                    if tracker:
                        tracker.mark_patient_processed(subject_id, SCRIPT_NAME, status='failed')
                    failed_patients.append(subject_id)
                
                pbar.update(1)
                pbar.set_postfix({'Processed': len(processed_patients), 'Failed': len(failed_patients)})
            
            pbar.close()
        
        # Log summary
        if processed_patients:
            logger.info(f"Successfully processed and tracked {len(processed_patients)} patients in tracker")
        if failed_patients:
            logger.warning(f"Failed to process {len(failed_patients)} patients (marked as failed in tracker)")
        
        logger.info("Patient nodes created successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        neo4j_conn.close()

if __name__ == "__main__":
    add_patient_nodes()