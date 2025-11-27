"""
Main embedding pipeline for patient embeddings
Generates patient node embeddings in Neo4j
Supports incremental loading using ETL tracker
"""
import logging
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Any
import numpy as np
from tqdm import tqdm

# Add Scripts directory to path for utils imports
# This file is at: Scripts/Create_Embeddings/full_patient_embeddings/embedding_pipeline.py
# So parent.parent.parent is Scripts/
scripts_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(scripts_dir))

# Add parent directory (Create_Embeddings) to path for neo4j_storage import
create_embeddings_dir = Path(__file__).parent.parent
sys.path.insert(0, str(create_embeddings_dir))

# Add current directory to path for same-folder imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection
from enhanced_text_extractor import EnhancedTextExtractor
from text_embeddings import TextEmbeddingGenerator
from neo4j_storage import Neo4jEmbeddingStorage

# Import ETL tracker for incremental loading
try:
    from utils.etl_tracker import ETLTracker
except ImportError:
    ETLTracker = None

logger = logging.getLogger(__name__)

# Log ETL tracker availability after logger is initialized
if ETLTracker is None:
    logger.warning("ETLTracker not found. Incremental loading will be disabled.")

# Script names for tracking
SCRIPT_NAME_PATIENT_EMBEDDINGS = 'generate_patient_embeddings'


class PatientEmbeddingPipeline:
    """Main pipeline for generating and storing embeddings"""
    
    def __init__(self, config: Config, tracker: Optional[ETLTracker] = None, tracker_file: Optional[str] = None):
        """
        Initialize pipeline
        
        Args:
            config: Configuration object
            tracker: Optional ETLTracker instance for incremental loading
            tracker_file: Optional path to tracker file (will create ETLTracker if provided)
        """
        self.config = config
        
        # Initialize ETL tracker for incremental loading
        if tracker is not None:
            self.tracker = tracker
        elif tracker_file is not None and ETLTracker is not None:
            self.tracker = ETLTracker(tracker_file)
            logger.info(f"Initialized ETL tracker from file: {tracker_file}")
        else:
            self.tracker = None
            if ETLTracker is None:
                logger.warning("ETLTracker not available. Incremental loading disabled.")
            else:
                logger.info("ETL tracker not provided. Running in full-load mode.")
        
        # Initialize connections
        self.neo4j = Neo4jConnection(
            config.neo4j.uri,
            config.neo4j.username,
            config.neo4j.password,
            config.neo4j.database
        )
        
        # Initialize generators
        self.text_extractor = EnhancedTextExtractor(self.neo4j)
        self.text_generator = TextEmbeddingGenerator(
            model_name=config.embedding.text_model_name,
            use_openai=config.embedding.use_openai if hasattr(config.embedding, 'use_openai') else False,
            use_gemini=config.embedding.use_gemini if hasattr(config.embedding, 'use_gemini') else False
        )
        
        # Initialize storage
        self.storage = Neo4jEmbeddingStorage(self.neo4j)
    
    def setup(self):
        """Setup connections"""
        logger.info("Setting up pipeline...")
        self.neo4j.connect()
        logger.info("Pipeline setup complete")
    
    def _get_patients_needing_embeddings(
        self,
        all_patient_ids: List[str],
        require_complete_kg: bool = True
    ) -> tuple[List[str], int]:
        """
        Filter patients to only those that need embeddings (incremental load support)
        
        Args:
            all_patient_ids: List of all patient IDs
            require_complete_kg: If True, only process patients with complete KG data
            
        Returns:
            Tuple of (patient_ids_to_process, skipped_count)
        """
        if not self.tracker:
            # No tracker: check Neo4j directly for missing embeddings
            logger.info("No tracker available. Checking Neo4j directly for missing embeddings...")
            query = """
            MATCH (p:Patient)
            WHERE p.subject_id IN $patient_ids
              AND p.textEmbedding IS NULL
            RETURN p.subject_id AS subject_id
            """
            patient_ids_int = [int(pid) for pid in all_patient_ids]
            result = self.neo4j.execute_query(query, {'patient_ids': patient_ids_int})
            missing_embeddings = [str(r['subject_id']) for r in result]
            logger.info(f"Found {len(missing_embeddings)} patients without embeddings in Neo4j")
            return missing_embeddings, len(all_patient_ids) - len(missing_embeddings)
        
        patients_to_process = []
        skipped_count = 0
        tracker_mismatch_count = 0
        
        # Required KG scripts that must have run successfully for a patient
        # These are the core scripts that create the patient flow and data
        required_kg_scripts = [
            '1_add_patient_nodes',
            '2_patient_flow_through_the_hospital'
        ]
        
        # Reload tracker to ensure we have the latest data
        if self.tracker:
            try:
                tracker_file = getattr(self.tracker, 'tracker_file', 'unknown')
                logger.info(f"Using tracker file: {tracker_file}")
                self.tracker._load_tracker()
                logger.debug(f"Reloaded tracker. Total entries: {len(self.tracker.tracker_df) if hasattr(self.tracker, 'tracker_df') and not self.tracker.tracker_df.empty else 0}")
            except Exception as e:
                logger.warning(f"Could not reload tracker: {e}")
        
        # Batch check Neo4j for actual embeddings to verify tracker state
        # This ensures data consistency between tracker and database
        patient_ids_int = [int(pid) for pid in all_patient_ids]
        query = """
        MATCH (p:Patient)
        WHERE p.subject_id IN $patient_ids
        RETURN p.subject_id AS subject_id,
               p.textEmbedding IS NOT NULL AS has_text_embedding
        """
        neo4j_embedding_status = {}
        try:
            result = self.neo4j.execute_query(query, {'patient_ids': patient_ids_int})
            logger.info(f"Neo4j embedding check: Found {len(result)} patient records")
            for record in result:
                subject_id = str(record['subject_id'])
                has_text = record.get('has_text_embedding', False)
                neo4j_embedding_status[subject_id] = has_text
                logger.debug(f"  Patient {subject_id}: textEmbedding={has_text}")
            
            # Log patients not found in Neo4j
            found_ids = set(neo4j_embedding_status.keys())
            missing_ids = set(str(pid) for pid in patient_ids_int) - found_ids
            if missing_ids:
                logger.warning(f"Neo4j embedding check: {len(missing_ids)} patients not found in Neo4j: {missing_ids}")
        except Exception as e:
            logger.error(f"Could not check Neo4j for embeddings: {e}. Will rely on tracker only.", exc_info=True)
            neo4j_embedding_status = {}
        
        # Track patients that have embeddings in Neo4j but missing from tracker (sync tracker)
        patients_to_sync_in_tracker = []
        
        for patient_id_str in all_patient_ids:
            try:
                patient_id_int = int(patient_id_str)
                
                # PRIMARY CHECK: Verify embeddings actually exist in Neo4j (source of truth)
                has_embeddings_in_neo4j = neo4j_embedding_status.get(patient_id_str, False)
                
                # If embeddings exist in Neo4j, skip regardless of tracker state
                if has_embeddings_in_neo4j:
                    skipped_count += 1
                    logger.info(f"Skipping patient {patient_id_int} - text embedding already exists in Neo4j")
                    
                    # Check if tracker needs to be synced (embeddings exist but tracker doesn't know)
                    if self.tracker:
                        tracker_says_processed = self.tracker.is_patient_processed(patient_id_int, SCRIPT_NAME_PATIENT_EMBEDDINGS)
                        if not tracker_says_processed:
                            # Embeddings exist in Neo4j but tracker doesn't have entry - sync tracker
                            patients_to_sync_in_tracker.append(patient_id_int)
                            logger.info(f"  → Will sync tracker: patient {patient_id_int} has embeddings in Neo4j but missing from tracker")
                    
                    continue
                
                # SECONDARY CHECK: Verify tracker state (for logging/debugging only)
                tracker_says_processed = self.tracker.is_patient_processed(patient_id_int, SCRIPT_NAME_PATIENT_EMBEDDINGS)
                if tracker_says_processed:
                    # Tracker says processed but embeddings don't exist - this is a mismatch
                    tracker_mismatch_count += 1
                    logger.warning(
                        f"Tracker says patient {patient_id_int} is processed for '{SCRIPT_NAME_PATIENT_EMBEDDINGS}', "
                        f"but embeddings are missing in Neo4j. Will regenerate embeddings."
                    )
                else:
                    logger.debug(f"Patient {patient_id_int} not in tracker for '{SCRIPT_NAME_PATIENT_EMBEDDINGS}' - will process")
                
                # TERTIARY CHECK: If require_complete_kg, check if patient has complete KG data
                if require_complete_kg:
                    kg_status = {}
                    for script_name in required_kg_scripts:
                        is_processed = self.tracker.is_patient_processed(patient_id_int, script_name)
                        kg_status[script_name] = is_processed
                        logger.debug(
                            f"Patient {patient_id_int} - Script '{script_name}': "
                            f"is_processed={is_processed}"
                        )
                    
                    has_complete_kg = all(kg_status.values())
                    if not has_complete_kg:
                        skipped_count += 1
                        missing_scripts = [script for script, status in kg_status.items() if not status]
                        logger.warning(
                            f"Skipping patient {patient_id_int} - KG data not complete yet. "
                            f"Missing scripts: {missing_scripts}. "
                            f"Tracker file: {self.tracker.tracker_file if hasattr(self.tracker, 'tracker_file') else 'unknown'}"
                        )
                        # Debug: Check what's actually in the tracker
                        if hasattr(self.tracker, 'tracker_df') and not self.tracker.tracker_df.empty:
                            patient_rows = self.tracker.tracker_df[
                                self.tracker.tracker_df['subject_id'] == patient_id_int
                            ]
                            logger.debug(f"Tracker entries for patient {patient_id_int}:\n{patient_rows.to_string()}")
                        continue
                    else:
                        logger.info(f"Patient {patient_id_int} has complete KG data - proceeding with embedding generation")
                
                # Patient needs embeddings - add to processing list
                logger.info(f"Patient {patient_id_int} will be processed - no embeddings in Neo4j, KG complete")
                patients_to_process.append(patient_id_str)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid patient_id '{patient_id_str}': {e}")
                continue
        
        if tracker_mismatch_count > 0:
            logger.warning(
                f"Found {tracker_mismatch_count} patients where tracker says processed but embeddings are missing in Neo4j. "
                f"These will be regenerated."
            )
        
        # Sync tracker: Mark patients as processed if they have embeddings in Neo4j but tracker is missing entries
        if self.tracker and patients_to_sync_in_tracker:
            logger.info(f"\nSyncing tracker: Marking {len(patients_to_sync_in_tracker)} patients as processed (embeddings exist in Neo4j but missing from tracker)")
            self.tracker.mark_patients_processed_batch(
                patients_to_sync_in_tracker,
                SCRIPT_NAME_PATIENT_EMBEDDINGS,
                status='success'
            )
            logger.info(f"✓ Synced tracker: Marked {len(patients_to_sync_in_tracker)} patients as processed for '{SCRIPT_NAME_PATIENT_EMBEDDINGS}'")
        
        return patients_to_process, skipped_count
    
    def generate_patient_embeddings(
        self,
        patient_ids: Optional[List[str]] = None,
        batch_size: int = 2000,
        force: bool = False,
        require_complete_kg: bool = True
    ):
        """
        Generate embeddings for patients (node-level) with incremental load support
        
        Args:
            patient_ids: List of patient IDs (None = all patients)
            batch_size: Batch size for processing
            force: If True, regenerate embeddings even if they exist
            require_complete_kg: If True, only process patients with complete KG data
        """
        logger.info("=" * 80)
        logger.info("GENERATING PATIENT EMBEDDINGS (NODE-LEVEL)")
        if self.tracker:
            logger.info("Incremental load mode: ENABLED")
        else:
            logger.info("Incremental load mode: DISABLED (full load)")
        logger.info("=" * 80)
        
        # Get patient IDs
        if patient_ids is None:
            all_patient_ids = self.neo4j.get_all_patient_ids()
        else:
            all_patient_ids = patient_ids
        
        logger.info(f"Total patients in database: {len(all_patient_ids)}")
        
        # Filter patients based on incremental load logic
        if force:
            patients_to_process = all_patient_ids
            skipped_count = 0
            logger.info("Force mode: Processing all patients (ignoring tracker)")
        else:
            patients_to_process, skipped_count = self._get_patients_needing_embeddings(
                all_patient_ids,
                require_complete_kg=require_complete_kg
            )
            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} patients (already processed or incomplete KG data)")
        
        if not patients_to_process:
            logger.info("No patients need embedding generation. All patients are up to date.")
            return
        
        logger.info(f"Processing {len(patients_to_process)} patients")
        logger.info("Processing patients individually and storing embeddings immediately in Neo4j...")
        
        # Process patients one at a time: extract, generate, store immediately
        total_patients = len(patients_to_process)
        stored_count = 0
        processed_patient_ids = []
        
        with tqdm(total=total_patients, desc="Processing patients", unit="patient", ncols=100) as pbar:
            for patient_id in patients_to_process:
                try:
                    # Extract text data for single patient
                    patient_text_data = self.text_extractor.extract_patient_text_data(patient_id)
                    
                    # Generate embedding for single patient
                    patient_embedding = self.text_generator.generate_patient_text_embedding(patient_text_data)
                    
                    # Store immediately in Neo4j
                    if self.storage.store_single_patient_embedding(patient_id, patient_embedding):
                        stored_count += 1
                        processed_patient_ids.append(int(patient_id))
                    
                    # Update progress bar after each patient
                    pbar.update(1)
                    
                except Exception as e:
                    logger.error(f"Error processing patient {patient_id}: {e}", exc_info=True)
                    # Update progress bar even on error
                    pbar.update(1)
                    continue
        
        logger.info(f"Stored {stored_count} patient embeddings in Neo4j")
        
        # Mark processed patients in tracker (incremental load)
        if self.tracker and processed_patient_ids:
            self.tracker.mark_patients_processed_batch(
                processed_patient_ids,
                SCRIPT_NAME_PATIENT_EMBEDDINGS,
                status='success'
            )
            logger.info(f"Marked {len(processed_patient_ids)} patients as processed in tracker for '{SCRIPT_NAME_PATIENT_EMBEDDINGS}'")
        
        # Step 4: Create vector index
        logger.info("\n[3/3] Creating vector index...")
        self.storage.create_neo4j_vector_indexes(
            index_name="patient_text_index",
            node_label="Patient",
            property_name="textEmbedding",
            dimension=self.config.embedding.text_dimension,
            similarity_function=self.config.vector_search.similarity_function if hasattr(self.config, 'vector_search') else "cosine"
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("PATIENT EMBEDDING GENERATION COMPLETE")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {stored_count} patients, skipped {skipped_count} patients")
        logger.info("=" * 80)
    
    def cleanup(self):
        """Cleanup resources"""
        if self.neo4j:
            self.neo4j.close()
        logger.info("Pipeline cleanup complete")

