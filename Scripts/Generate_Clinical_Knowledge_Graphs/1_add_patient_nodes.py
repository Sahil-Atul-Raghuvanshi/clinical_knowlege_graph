import pandas as pd
from neo4j import GraphDatabase
import logging
import os
from typing import Optional
from incremental_load_utils import IncrementalLoadChecker
from etl_tracker import ETLTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_patient_nodes(tracker: Optional[ETLTracker] = None):
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    
    # Path to patient CSV file (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    PATIENTS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'patients.csv')
    ADMISSIONS_CSV = os.path.join(project_root, 'Filtered_Data', 'hosp', 'admissions.csv')
    EDSTAYS_CSV = os.path.join(project_root, 'Filtered_Data', 'ed', 'edstays.csv')
    SCRIPT_NAME = '1_add_patient_nodes'
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            logger.info(f"Connection successful!")
        
        # Load patient data
        df = pd.read_csv(PATIENTS_CSV)
        logger.info(f"Found {len(df)} patient records")
        
        # Load admissions and ED stays to filter patients
        # Only include patients who have at least one admission OR one ED visit
        patients_with_data = set()
        
        try:
            # Load admissions
            admissions_df = pd.read_csv(ADMISSIONS_CSV)
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
            edstays_df = pd.read_csv(EDSTAYS_CSV)
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
        checker = IncrementalLoadChecker(driver, tracker=tracker)
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
        
        # Track processed patients
        processed_patients = []
        
        # Create patient nodes
        with driver.session() as session:
            for _, row in df_to_process.iterrows():
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
                               subject_id=int(row['subject_id']),
                               gender=row['gender'],
                               anchor_age=int(row['anchor_age']),
                               anchor_year=int(row['anchor_year']),
                               anchor_year_group=row['anchor_year_group'],
                               dod=dod)
                    
                    processed_patients.append(int(row['subject_id']))
                    logger.info(f"Created patient node for subject_id: {row['subject_id']}")
                except Exception as e:
                    logger.error(f"Error processing patient {row['subject_id']}: {e}")
                    if tracker:
                        tracker.mark_patient_processed(int(row['subject_id']), SCRIPT_NAME, status='failed')
        
        # Mark all successfully processed patients in tracker
        if tracker and processed_patients:
            tracker.mark_patients_processed_batch(processed_patients, SCRIPT_NAME, status='success')
            logger.info(f"Marked {len(processed_patients)} patients as processed in tracker")
        
        logger.info("Patient nodes created successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    add_patient_nodes()