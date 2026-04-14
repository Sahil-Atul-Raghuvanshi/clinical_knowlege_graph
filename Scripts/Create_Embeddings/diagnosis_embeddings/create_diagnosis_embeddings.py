"""
Diagnosis embedding generator
Generates embeddings for patient diagnoses and stores them in Patient nodes
Uses the all_diagnoses attribute from Patient nodes
Supports incremental loading using ETL tracker
"""
import logging
import sys
import time
from pathlib import Path
from typing import List, Optional
import numpy as np
from tqdm import tqdm

# Add Scripts directory to path for utils imports
# This file is at: Scripts/Create_Embeddings/diagnosis_embeddings/create_diagnosis_embeddings.py
# So parent.parent.parent is Scripts/
scripts_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(scripts_dir))

# Add parent directory (Create_Embeddings) to path for imports
create_embeddings_dir = Path(__file__).parent.parent
sys.path.insert(0, str(create_embeddings_dir))

# Add full_patient_embeddings directory to path for text_embeddings import
full_patient_embeddings_dir = create_embeddings_dir / 'full_patient_embeddings'
sys.path.insert(0, str(full_patient_embeddings_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection
from text_embeddings import TextEmbeddingGenerator
from neo4j_storage import Neo4jEmbeddingStorage

# Import ETL tracker for incremental loading
try:
    from utils.etl_tracker import ETLTracker
except ImportError:
    ETLTracker = None

# Configure logging
project_root = scripts_dir.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prevent propagation to root logger (which would print to console)
logger.propagate = False

# Script name for tracking
SCRIPT_NAME = 'create_diagnosis_embeddings'


def format_diagnoses_for_embedding(all_diagnoses: List[str]) -> str:
    """
    Format diagnosis list into a single string for embedding
    
    Args:
        all_diagnoses: List of diagnosis strings
        
    Returns:
        Formatted text string for embedding
    """
    if not all_diagnoses:
        return ""
    
    # Join all diagnoses with separators
    # Use a clear separator to maintain structure
    formatted = " | ".join([str(diag).strip() for diag in all_diagnoses if diag and str(diag).strip()])
    
    return formatted


def create_diagnosis_embeddings(
    tracker: Optional[ETLTracker] = None,
    pipeline_log_file: Optional[str] = None,
    patient_ids: Optional[List[str]] = None,
    force: bool = False
):
    """
    Generate diagnosis embeddings for patients and store in Patient nodes
    
    Args:
        tracker: Optional ETLTracker instance for incremental loading
        pipeline_log_file: Optional path to pipeline log file
        patient_ids: Optional list of patient IDs to process (None = all patients)
        force: If True, regenerate embeddings even if they exist
    """
    # Setup logging
    logger.handlers = []
    
    if pipeline_log_file:
        # Pipeline mode: append to the pipeline log file
        file_handler = logging.FileHandler(pipeline_log_file, encoding='utf-8', mode='a')
    else:
        # Standalone mode: create log file
        log_file = logs_dir / 'create_diagnosis_embeddings.log'
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
    
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    
    logger.info("=" * 80)
    logger.info("GENERATING DIAGNOSIS EMBEDDINGS")
    if tracker:
        logger.info("Incremental load mode: ENABLED")
    else:
        logger.info("Incremental load mode: DISABLED (full load)")
    logger.info("=" * 80)
    
    # Load configuration
    config = Config()
    
    # Connect to Neo4j
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()
    
    try:
        # Initialize embedding generator
        text_generator = TextEmbeddingGenerator(
            model_name=config.embedding.text_model_name,
            use_openai=config.embedding.use_openai if hasattr(config.embedding, 'use_openai') else False,
            use_gemini=config.embedding.use_gemini if hasattr(config.embedding, 'use_gemini') else False
        )
        
        # Initialize storage
        storage = Neo4jEmbeddingStorage(neo4j_conn)
        
        # Get patient IDs
        if patient_ids is None:
            # Get all patients with all_diagnoses attribute
            query_all_patients = """
            MATCH (p:Patient)
            WHERE p.all_diagnoses IS NOT NULL AND size(p.all_diagnoses) > 0
            RETURN p.subject_id AS subject_id
            ORDER BY p.subject_id
            """
            result = neo4j_conn.execute_query(query_all_patients)
            all_patient_ids = [str(record['subject_id']) for record in result if record.get('subject_id') is not None]
        else:
            all_patient_ids = patient_ids
        
        logger.info(f"Total patients with diagnoses: {len(all_patient_ids)}")
        
        # Filter patients based on incremental load logic
        if force:
            patients_to_process = all_patient_ids
            skipped_count = 0
            logger.info("Force mode: Processing all patients (ignoring tracker)")
        else:
            # Check which patients need embeddings
            patients_to_process = []
            skipped_count = 0
            
            if tracker:
                # Reload tracker to ensure we have the latest data
                try:
                    tracker_file = getattr(tracker, 'tracker_file', 'unknown')
                    logger.info(f"Using tracker file: {tracker_file}")
                    tracker._load_tracker()
                except Exception as e:
                    logger.warning(f"Could not reload tracker: {e}")
            
            # Batch check Neo4j for actual embeddings
            patient_ids_int = [int(pid) for pid in all_patient_ids]
            query_check = """
            MATCH (p:Patient)
            WHERE p.subject_id IN $patient_ids
            RETURN p.subject_id AS subject_id,
                   p.diagnosis_embeddings IS NOT NULL AS has_diagnosis_embedding
            """
            neo4j_embedding_status = {}
            try:
                result = neo4j_conn.execute_query(query_check, {'patient_ids': patient_ids_int})
                for record in result:
                    subject_id = str(record['subject_id'])
                    has_embedding = record.get('has_diagnosis_embedding', False)
                    neo4j_embedding_status[subject_id] = has_embedding
            except Exception as e:
                logger.error(f"Could not check Neo4j for embeddings: {e}")
                neo4j_embedding_status = {}
            
            # Filter patients
            for patient_id_str in all_patient_ids:
                try:
                    patient_id_int = int(patient_id_str)
                    
                    # PRIMARY CHECK: Verify embeddings actually exist in Neo4j
                    has_embeddings_in_neo4j = neo4j_embedding_status.get(patient_id_str, False)
                    
                    if has_embeddings_in_neo4j:
                        skipped_count += 1
                        logger.debug(f"Skipping patient {patient_id_int} - diagnosis embedding already exists in Neo4j")
                        continue
                    
                    # SECONDARY CHECK: Verify tracker state
                    if tracker:
                        tracker_says_processed = tracker.is_patient_processed(patient_id_int, SCRIPT_NAME)
                        if tracker_says_processed:
                            # Tracker says processed but embeddings don't exist - regenerate
                            logger.warning(
                                f"Tracker says patient {patient_id_int} is processed for '{SCRIPT_NAME}', "
                                f"but embeddings are missing in Neo4j. Will regenerate embeddings."
                            )
                    
                    patients_to_process.append(patient_id_str)
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid patient_id '{patient_id_str}': {e}")
                    continue
            
            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} patients (already have embeddings)")
        
        if not patients_to_process:
            logger.info("No patients need diagnosis embedding generation. All patients are up to date.")
            return
        
        logger.info(f"Processing {len(patients_to_process)} patients")
        
        # Process patients one at a time: extract, generate, store immediately
        total_patients = len(patients_to_process)
        stored_count = 0
        processed_patient_ids = []
        failed_patient_ids = []
        
        with tqdm(total=total_patients, desc="Processing patients", unit="patient", ncols=100) as pbar:
            for patient_id in patients_to_process:
                try:
                    # Get all_diagnoses from Patient node
                    query_diagnoses = """
                    MATCH (p:Patient {subject_id: toInteger($patient_id)})
                    RETURN p.all_diagnoses AS all_diagnoses
                    """
                    result = neo4j_conn.execute_query(query_diagnoses, {'patient_id': patient_id})
                    
                    if not result or not result[0].get('all_diagnoses'):
                        logger.warning(f"Patient {patient_id} has no all_diagnoses attribute or it's empty")
                        pbar.update(1)
                        continue
                    
                    all_diagnoses = result[0]['all_diagnoses']
                    
                    # Format diagnoses for embedding
                    diagnosis_text = format_diagnoses_for_embedding(all_diagnoses)
                    
                    if not diagnosis_text or not diagnosis_text.strip():
                        logger.warning(f"Patient {patient_id} has empty diagnosis text after formatting")
                        pbar.update(1)
                        continue
                    
                    # Generate embedding
                    diagnosis_embedding = text_generator.generate_embedding(diagnosis_text)
                    
                    # Store in Neo4j
                    query_store = """
                    MATCH (p:Patient {subject_id: toInteger($patient_id)})
                    SET p.diagnosis_embeddings = $diagnosis_embedding
                    RETURN count(p) AS count
                    """
                    
                    embedding_list = diagnosis_embedding.tolist() if isinstance(diagnosis_embedding, np.ndarray) else diagnosis_embedding
                    
                    result = neo4j_conn.execute_query(query_store, {
                        'patient_id': patient_id,
                        'diagnosis_embedding': embedding_list
                    })
                    
                    if result and result[0]['count'] > 0:
                        stored_count += 1
                        processed_patient_ids.append(int(patient_id))
                        logger.debug(f"Stored diagnosis embedding for patient {patient_id}")
                    else:
                        logger.warning(f"Failed to store embedding for patient {patient_id}")
                        failed_patient_ids.append(int(patient_id))
                    
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Error processing patient {patient_id}: {e}", exc_info=True)
                    failed_patient_ids.append(int(patient_id))
                    pbar.update(1)
                    continue
        
        logger.info(f"Stored {stored_count} diagnosis embeddings in Neo4j")
        
        # Mark processed patients in tracker (incremental load)
        if tracker and processed_patient_ids:
            tracker.mark_patients_processed_batch(
                processed_patient_ids,
                SCRIPT_NAME,
                status='success'
            )
            logger.info(f"Marked {len(processed_patient_ids)} patients as processed in tracker for '{SCRIPT_NAME}'")
        
        if failed_patient_ids and tracker:
            tracker.mark_patients_processed_batch(
                failed_patient_ids,
                SCRIPT_NAME,
                status='failed'
            )
            logger.warning(f"Marked {len(failed_patient_ids)} patients as failed in tracker")
        
        # Create vector index
        logger.info("\nCreating vector index for diagnosis embeddings...")
        storage.create_neo4j_vector_indexes(
            index_name="patient_diagnosis_index",
            node_label="Patient",
            property_name="diagnosis_embeddings",
            dimension=config.embedding.text_dimension,
            similarity_function=config.vector_search.similarity_function if hasattr(config, 'vector_search') else "cosine"
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("DIAGNOSIS EMBEDDING GENERATION COMPLETE")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {stored_count} patients, skipped {skipped_count} patients")
        if failed_patient_ids:
            logger.warning(f"Failed to process {len(failed_patient_ids)} patients")
        logger.info("=" * 80)
    
    finally:
        neo4j_conn.close()
        logger.info("Pipeline cleanup complete")


if __name__ == "__main__":
    create_diagnosis_embeddings()

