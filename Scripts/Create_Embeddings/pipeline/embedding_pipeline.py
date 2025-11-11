"""
Main embedding pipeline for hybrid system
Generates node-level embeddings (Neo4j) and item-level embeddings (Milvus)
Supports incremental loading using ETL tracker
"""
import logging
import sys
import time
import os
from pathlib import Path
from typing import List, Dict, Optional
import numpy as np
from tqdm import tqdm

# Add Create_Embeddings directory to path for relative imports
# This file is at: Scripts/Create_Embeddings/pipeline/embedding_pipeline.py
# So parent.parent is Scripts/, and we need Create_Embeddings/
create_embeddings_dir = Path(__file__).parent.parent
sys.path.insert(0, str(create_embeddings_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection
from utils.milvus_connection import MilvusConnection
from generators.enhanced_text_extractor import EnhancedTextExtractor
from generators.text_embeddings import TextEmbeddingGenerator
from generators.structural_embeddings import StructuralEmbeddingGenerator
from generators.combined_embeddings import CombinedEmbeddingGenerator
from generators.item_embeddings import ItemEmbeddingGenerator
from storage.hybrid_storage import HybridEmbeddingStorage

# Import ETL tracker for incremental loading
script_dir = Path(__file__).parent.parent.parent
kg_scripts_dir = script_dir / 'Generate_Clinical_Knowledge_Graphs'
sys.path.insert(0, str(kg_scripts_dir))
try:
    from etl_tracker import ETLTracker
except ImportError:
    ETLTracker = None

logger = logging.getLogger(__name__)

# Log ETL tracker availability after logger is initialized
if ETLTracker is None:
    logger.warning("ETLTracker not found. Incremental loading will be disabled.")

# Script names for tracking
SCRIPT_NAME_PATIENT_EMBEDDINGS = 'generate_patient_embeddings'
SCRIPT_NAME_ITEM_EMBEDDINGS = 'generate_item_embeddings'


class HybridEmbeddingPipeline:
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
        
        self.milvus = MilvusConnection(
            config.milvus.host,
            config.milvus.port,
            config.milvus.alias
        )
        
        # Initialize generators
        self.text_extractor = EnhancedTextExtractor(self.neo4j)
        self.text_generator = TextEmbeddingGenerator(
            model_name=config.embedding.text_model_name,
            use_openai=config.embedding.use_openai if hasattr(config.embedding, 'use_openai') else False,
            use_gemini=config.embedding.use_gemini if hasattr(config.embedding, 'use_gemini') else False
        )
        self.structural_generator = StructuralEmbeddingGenerator(
            self.neo4j,
            config.graph.graph_name
        )
        self.combined_generator = CombinedEmbeddingGenerator(
            combine_method=config.embedding.combine_method,
            structural_weight=config.embedding.structural_weight,
            textual_weight=config.embedding.textual_weight
        )
        self.item_generator = ItemEmbeddingGenerator(
            model_name=config.embedding.text_model_name
        )
        
        # Initialize storage
        self.storage = HybridEmbeddingStorage(
            self.neo4j,
            self.milvus,
            config.embedding.text_dimension
        )
    
    def setup(self):
        """Setup connections"""
        logger.info("Setting up pipeline...")
        self.neo4j.connect()
        self.milvus.connect()  # Milvus is required
        
        # Check GDS availability
        if not self.neo4j.check_gds_availability():
            logger.error("Neo4j GDS is not available! Please install GDS plugin.")
            raise RuntimeError("Neo4j GDS is required")
        
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
              AND (p.textEmbedding IS NULL OR p.combinedEmbedding IS NULL)
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
        
        # Reload tracker to ensure we have the latest data (in case CSV was updated)
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
               p.textEmbedding IS NOT NULL AS has_text_embedding,
               p.combinedEmbedding IS NOT NULL AS has_combined_embedding
        """
        neo4j_embedding_status = {}
        try:
            result = self.neo4j.execute_query(query, {'patient_ids': patient_ids_int})
            logger.info(f"Neo4j embedding check: Found {len(result)} patient records")
            for record in result:
                subject_id = str(record['subject_id'])
                has_text = record.get('has_text_embedding', False)
                has_combined = record.get('has_combined_embedding', False)
                neo4j_embedding_status[subject_id] = has_text and has_combined
                logger.debug(f"  Patient {subject_id}: textEmbedding={has_text}, combinedEmbedding={has_combined}, has_both={has_text and has_combined}")
            
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
                    logger.info(f"Skipping patient {patient_id_int} - embeddings already exist in Neo4j (textEmbedding AND combinedEmbedding)")
                    
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
        
        # Step 1: Generate structural embeddings using GDS (one-time for all)
        logger.info("\n[1/4] Generating structural embeddings using GDS FastRP...")
        self.structural_generator.create_projection(
            self.config.graph.node_labels,
            self.config.graph.relationship_types
        )
        
        self.structural_generator.generate_fastrp_embeddings(
            embedding_dimension=self.config.embedding.fastrp_dimension,
            iteration_weights=self.config.embedding.fastrp_iteration_weights,
            normalization_strength=self.config.embedding.fastrp_normalization_strength
        )
        
        structural_embeddings = self.structural_generator.get_patient_embeddings()
        logger.info(f"Generated {len(structural_embeddings)} structural embeddings")
        
        # Step 2: Process in batches
        logger.info(f"\n[2/4] Processing text embeddings in batches of {batch_size}...")
        text_embeddings = {}
        combined_embeddings = {}
        
        for i in tqdm(range(0, len(patients_to_process), batch_size), desc="Processing batches"):
            batch_ids = patients_to_process[i:i + batch_size]
            
            # Extract text data
            batch_text_data = self.text_extractor.batch_extract_patient_text_data(batch_ids)
            
            # Generate text embeddings
            batch_text_emb = self.text_generator.generate_patient_embeddings_batch(batch_text_data)
            text_embeddings.update(batch_text_emb)
            
            # Get structural embeddings for batch
            batch_structural = {
                pid: structural_embeddings[pid]
                for pid in batch_ids
                if pid in structural_embeddings
            }
            
            # Combine embeddings
            batch_combined = self.combined_generator.combine_batch(
                batch_structural,
                batch_text_emb
            )
            combined_embeddings.update(batch_combined)
        
        # Step 3: Store in Neo4j
        logger.info("\n[3/4] Storing patient embeddings in Neo4j...")
        stored_count = self.storage.store_patient_embeddings(
            text_embeddings,
            combined_embeddings,
            batch_size=100
        )
        logger.info(f"Stored {stored_count} patient embeddings")
        
        # Step 4: Mark processed patients in tracker (incremental load)
        if self.tracker and stored_count > 0:
            # Get list of patient IDs that were successfully stored
            processed_patient_ids = [
                int(pid) for pid in patients_to_process
                if pid in combined_embeddings
            ]
            if processed_patient_ids:
                self.tracker.mark_patients_processed_batch(
                    processed_patient_ids,
                    SCRIPT_NAME_PATIENT_EMBEDDINGS,
                    status='success'
                )
                logger.info(f"Marked {len(processed_patient_ids)} patients as processed in tracker for '{SCRIPT_NAME_PATIENT_EMBEDDINGS}'")
        
        # Step 5: Create vector indexes
        logger.info("\n[4/5] Creating vector indexes...")
        self.storage.create_neo4j_vector_indexes(
            "patient_text_index",
            "textEmbedding",
            self.config.embedding.text_dimension
        )
        self.storage.create_neo4j_vector_indexes(
            "patient_journey_index",
            "combinedEmbedding",
            self.config.embedding.combined_dimension
        )
        
        # Step 6: Cleanup temporary structural embeddings from ALL nodes
        logger.info("\n[5/5] Cleaning up temporary structural embeddings...")
        self._cleanup_structural_embeddings()
        
        # Cleanup projection
        self.structural_generator.drop_projection()
        
        logger.info("\n" + "=" * 80)
        logger.info("PATIENT EMBEDDING GENERATION COMPLETE")
        if skipped_count > 0:
            logger.info(f"Incremental load summary: Processed {stored_count} patients, skipped {skipped_count} patients")
        logger.info("=" * 80)
    
    def generate_item_embeddings(
        self,
        limit: Optional[int] = None,
        force: bool = False
    ):
        """
        Generate item-level embeddings and store in Milvus
        Note: Item embeddings are not tracked per-patient in the tracker
        as they are item-level (not patient-level) embeddings
        
        Args:
            limit: Limit number of nodes to process per type (None = all)
            force: Force regeneration even if items exist
        """
        logger.info("=" * 80)
        logger.info("GENERATING ITEM EMBEDDINGS (MILVUS)")
        if self.tracker:
            logger.info("Note: Item embeddings are not tracked per-patient (item-level, not patient-level)")
        logger.info("=" * 80)
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # Process Prescriptions
        logger.info("\n[1/4] Processing Prescription items...")
        try:
            query_prescriptions = f"""
            MATCH (p:Prescription)
            WHERE p.medicines IS NOT NULL
            RETURN p.event_id AS event_id, 
                   COALESCE(p.starttime, '') AS starttime,
                   p.medicines AS medicines
            {limit_clause}
            """
            prescription_nodes = self.neo4j.execute_query(query_prescriptions)
            if prescription_nodes:
                prescription_items = self.item_generator.process_prescription_items(prescription_nodes)
                if prescription_items:
                    self.storage.store_item_embeddings(
                        prescription_items,
                        self.config.milvus.prescription_collection,
                        batch_size=self.config.batch_processing.item_batch_size,
                        force=force
                    )
        except Exception as e:
            logger.error(f"Error processing Prescription items: {e}", exc_info=True)
            logger.info("Continuing with other item types...")
        
        # Process Microbiology Events
        logger.info("\n[2/4] Processing Microbiology items...")
        try:
            query_micro = f"""
            MATCH (me:MicrobiologyEvent)
            WHERE me.micro_results IS NOT NULL
            RETURN COALESCE(me.event_id, '') AS event_id,
                   COALESCE(me.subject_id, '') AS subject_id,
                   COALESCE(me.hadm_id, '') AS hadm_id,
                   me.micro_results AS micro_results
            {limit_clause}
            """
            micro_nodes = self.neo4j.execute_query(query_micro)
            if micro_nodes:
                micro_items = self.item_generator.process_microbiology_items(micro_nodes)
                if micro_items:
                    self.storage.store_item_embeddings(
                        micro_items,
                        self.config.milvus.microbiology_collection,
                        batch_size=self.config.batch_processing.item_batch_size,
                        force=force
                    )
        except Exception as e:
            logger.error(f"Error processing Microbiology items: {e}", exc_info=True)
            logger.info("Continuing with other item types...")
        
        # Process Lab Events
        logger.info("\n[3/4] Processing Lab Result items...")
        try:
            query_labs = f"""
            MATCH (le:LabEvent)
            WHERE le.lab_results IS NOT NULL
            RETURN COALESCE(le.event_id, '') AS event_id,
                   COALESCE(le.subject_id, '') AS subject_id,
                   COALESCE(le.hadm_id, '') AS hadm_id,
                   COALESCE(le.charttime, '') AS charttime,
                   le.lab_results AS lab_results
            {limit_clause}
            """
            lab_nodes = self.neo4j.execute_query(query_labs)
            if lab_nodes:
                lab_items = self.item_generator.process_lab_result_items(lab_nodes)
                if lab_items:
                    self.storage.store_item_embeddings(
                        lab_items,
                        self.config.milvus.lab_result_collection,
                        batch_size=self.config.batch_processing.item_batch_size,
                        force=force
                    )
        except Exception as e:
            logger.error(f"Error processing Lab Result items: {e}", exc_info=True)
            logger.info("Continuing with other item types...")
        
        # Process Diagnoses
        logger.info("\n[4/4] Processing Diagnosis items...")
        try:
            query_diag = f"""
            MATCH (d:Diagnosis)
            WHERE d.primary_diagnoses IS NOT NULL OR d.secondary_diagnoses IS NOT NULL
            RETURN COALESCE(d.event_id, '') AS event_id,
                   COALESCE(d.subject_id, '') AS subject_id,
                   COALESCE(d.hadm_id, '') AS hadm_id,
                   d.primary_diagnoses AS primary_diagnoses, 
                   d.secondary_diagnoses AS secondary_diagnoses
            {limit_clause}
            """
            diag_nodes = self.neo4j.execute_query(query_diag)
            if diag_nodes:
                diag_items = self.item_generator.process_diagnosis_items(diag_nodes)
                if diag_items:
                    self.storage.store_item_embeddings(
                        diag_items,
                        self.config.milvus.diagnosis_collection,
                        batch_size=self.config.batch_processing.item_batch_size,
                        force=force
                    )
        except Exception as e:
            logger.error(f"Error processing Diagnosis items: {e}", exc_info=True)
            logger.info("Item embedding generation completed with some errors")
        
        logger.info("\n" + "=" * 80)
        logger.info("ITEM EMBEDDING GENERATION COMPLETE")
        logger.info("=" * 80)
    
    def _cleanup_structural_embeddings(self):
        """
        Remove temporary structuralEmbedding properties from all nodes
        This is called after embeddings are retrieved and used
        """
        logger.info("Removing temporary structural embeddings from all node types...")
        
        # Use a simpler, more robust cleanup query
        # Process in batches to avoid memory issues with large graphs
        batch_size = 10000
        
        # First, count how many nodes have structuralEmbedding
        count_query = """
        MATCH (n)
        WHERE n.structuralEmbedding IS NOT NULL
        RETURN count(n) AS total_count
        """
        result = self.neo4j.execute_query(count_query)
        total_count = result[0]['total_count'] if result else 0
        
        if total_count == 0:
            logger.info("No structural embeddings to clean up")
            return
        
        logger.info(f"Found {total_count} nodes with structuralEmbedding to clean up")
        
        # Clean up in batches
        cleaned_total = 0
        while True:
            cleanup_query = """
            MATCH (n)
            WHERE n.structuralEmbedding IS NOT NULL
            WITH n
            LIMIT $batch_size
            REMOVE n.structuralEmbedding
            RETURN count(n) AS cleaned
            """
            
            try:
                result = self.neo4j.execute_query(cleanup_query, {'batch_size': batch_size})
                if result:
                    cleaned = result[0].get('cleaned', 0)
                    cleaned_total += cleaned
                    logger.info(f"Cleaned {cleaned_total}/{total_count} structural embeddings...")
                    
                    if cleaned == 0:
                        break  # No more nodes to clean
                else:
                    break
            except Exception as e:
                logger.error(f"Error during cleanup batch: {e}")
                break
        
        # Final cleanup query to get statistics by node type
        stats_query = """
        MATCH (n)
        WHERE n.structuralEmbedding IS NOT NULL
        RETURN labels(n)[0] AS node_type, count(n) AS remaining_count
        ORDER BY remaining_count DESC
        """
        
        try:
            remaining = self.neo4j.execute_query(stats_query)
            if remaining:
                total_remaining = sum(r.get('remaining_count', 0) for r in remaining)
                if total_remaining > 0:
                    logger.warning(f"Warning: {total_remaining} nodes still have structuralEmbedding:")
                    for record in remaining:
                        logger.warning(f"  - {record.get('node_type', 'Unknown')}: {record.get('remaining_count', 0)} nodes")
                else:
                    logger.info(f"[OK] Successfully cleaned all {cleaned_total} structural embeddings")
        except Exception as e:
            logger.warning(f"Could not get cleanup statistics: {e}")
            logger.info(f"Cleaned approximately {cleaned_total} structural embeddings")
    
    def cleanup(self):
        """Cleanup resources"""
        if self.neo4j:
            self.neo4j.close()
        if self.milvus:
            self.milvus.disconnect()
        logger.info("Pipeline cleanup complete")

