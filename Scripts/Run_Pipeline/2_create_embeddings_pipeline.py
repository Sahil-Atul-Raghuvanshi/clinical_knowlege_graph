"""
Batch processing pipeline for large-scale patient embedding generation
Optimized for datasets with 100K+ patients (e.g., 364K patients)
"""
import logging
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import time
from tqdm import tqdm
import numpy as np

# Add Embeddings directory to path for imports
# This file is now at: Scripts/Run_Pipeline/2_create_embeddings_pipeline.py
# Need to access modules from: Scripts/Embeddings/
embeddings_path = Path(__file__).parent.parent / 'Create_Embeddings'
sys.path.insert(0, str(embeddings_path))

# Import ETL tracker for incremental loading
kg_scripts_dir = Path(__file__).parent.parent / 'Generate_Clinical_Knowledge_Graphs'
sys.path.insert(0, str(kg_scripts_dir))
try:
    from etl_tracker import ETLTracker
except ImportError:
    ETLTracker = None

# Import new embedding system modules
from pipeline.embedding_pipeline import HybridEmbeddingPipeline
from utils.config import Config

# Setup logging
# Get project root and create logs directory
# This file is at: Scripts/Run_Pipeline/2_create_embeddings_pipeline.py
# Need to go up 3 levels to reach Phase1/ (Run_Pipeline -> Scripts -> Phase1)
project_root = Path(__file__).parent.parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)

# Configure logging to save in logs directory
log_file = logs_dir / f'embedding_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Log file: {log_file}")

# Log ETL tracker availability
if ETLTracker is None:
    logger.warning("ETLTracker not found. Incremental loading will be disabled.")
else:
    logger.info("ETLTracker available. Incremental loading enabled.")


class BatchProgress:
    """Track and persist batch processing progress"""
    
    def __init__(self, progress_file: str):
        self.progress_file = Path(progress_file)
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
    
    def _load(self) -> Dict:
        """Load progress from file"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'completed_batches': [],
            'failed_batches': [],
            'last_batch_index': -1,
            'total_processed': 0,
            'start_time': None,
            'last_update': None
        }
    
    def save(self):
        """Save progress to file"""
        self.data['last_update'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_batch_completed(self, batch_index: int, patient_count: int):
        """Mark a batch as completed"""
        if batch_index not in self.data['completed_batches']:
            self.data['completed_batches'].append(batch_index)
        self.data['last_batch_index'] = batch_index
        self.data['total_processed'] += patient_count
        self.save()
    
    def mark_batch_failed(self, batch_index: int, error: str):
        """Mark a batch as failed"""
        self.data['failed_batches'].append({
            'batch_index': batch_index,
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        })
        self.save()
    
    def is_batch_completed(self, batch_index: int) -> bool:
        """Check if batch is already completed"""
        return batch_index in self.data['completed_batches']
    
    def get_resume_index(self) -> int:
        """Get index to resume from"""
        return self.data['last_batch_index'] + 1
    
    def reset(self):
        """Reset progress"""
        self.data = {
            'completed_batches': [],
            'failed_batches': [],
            'last_batch_index': -1,
            'total_processed': 0,
            'start_time': datetime.now().isoformat(),
            'last_update': None
        }
        self.save()


class LargeScaleBatchPipeline:
    """Pipeline optimized for large-scale patient datasets (100K+) using hybrid storage"""
    
    def __init__(self, config: Config, tracker: Optional[ETLTracker] = None, tracker_file: Optional[str] = None):
        self.config = config
        self.pipeline = None
        self.progress = BatchProgress(config.progress_file)
        self.tracker = tracker
        self.tracker_file = tracker_file
        
        logger.info("Initializing Large-Scale Hybrid Embedding Pipeline")
        logger.info(f"Batch size: {config.batch_processing.batch_size}")
        logger.info("Using Neo4j for node-level embeddings")
        logger.info("Using Milvus for item-level embeddings")
        if tracker or tracker_file:
            logger.info("Incremental load mode: ENABLED (using ETL tracker)")
        else:
            logger.info("Incremental load mode: DISABLED (full load)")
    
    def setup(self):
        """Setup all components"""
        logger.info("Setting up hybrid embedding pipeline...")
        
        # Initialize the new hybrid pipeline with tracker support
        self.pipeline = HybridEmbeddingPipeline(
            self.config,
            tracker=self.tracker,
            tracker_file=self.tracker_file
        )
        self.pipeline.setup()  # Milvus is required
        
        logger.info("Pipeline setup complete [OK]")
    
    def get_unprocessed_patients(self) -> List[str]:
        """Get list of patients without embeddings"""
        logger.info("Querying for patients without embeddings...")
        
        query = """
        MATCH (p:Patient)
        WHERE p.embedding IS NULL
        RETURN p.subject_id AS subject_id
        ORDER BY p.subject_id
        """
        
        result = self.connection.execute_query(query)
        patient_ids = [str(r['subject_id']) for r in result]
        
        logger.info(f"Found {len(patient_ids)} patients without embeddings")
        return patient_ids
    
    def check_existing_item_embeddings(self) -> Dict[str, int]:
        """
        Check what item embeddings already exist in the database
        
        Returns:
            Dictionary with counts of existing item embeddings
        """
        logger.info("Checking for existing item embeddings...")
        
        counts = {}
        item_types = [
            ('DiagnosisItem', 'diagnosis'),
            ('MedicationItem', 'medication'),
            ('LabResultItem', 'lab_result'),
            ('MicrobiologyResultItem', 'microbiology_result')
        ]
        
        for node_label, item_name in item_types:
            query = f"""
            MATCH (n:{node_label})
            WHERE n.embedding IS NOT NULL
            RETURN count(n) AS count
            """
            try:
                result = self.connection.execute_query(query)
                count = result[0]['count'] if result else 0
                counts[item_name] = count
                if count > 0:
                    logger.info(f"  Found {count} existing {item_name} embeddings")
            except Exception as e:
                logger.debug(f"  {node_label} nodes don't exist yet: {e}")
                counts[item_name] = 0
        
        return counts
    
    def check_vector_indexes(self) -> Dict[str, bool]:
        """
        Check which vector indexes already exist
        
        Returns:
            Dictionary mapping index names to existence status
        """
        logger.info("Checking for existing vector indexes...")
        
        indexes = {
            'patient_journey_index': False,
            'diagnosis_item_embedding_index': False,
            'medication_item_embedding_index': False,
            'lab_result_item_embedding_index': False,
            'microbiology_result_item_embedding_index': False
        }
        
        query = """
        SHOW INDEXES
        YIELD name, type
        WHERE type = 'VECTOR'
        RETURN name
        """
        
        try:
            result = self.connection.execute_query(query)
            existing_indexes = {r['name'] for r in result}
            
            for index_name in indexes.keys():
                if index_name in existing_indexes:
                    indexes[index_name] = True
                    logger.info(f"  [OK] {index_name} exists")
                else:
                    logger.info(f"  [MISSING] {index_name} missing")
        except Exception as e:
            logger.warning(f"Could not check indexes: {e}")
        
        return indexes
    
    def generate_structural_embeddings_gds(self) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Generate structural embeddings using Neo4j GDS FastRP
        This is done once for ALL nodes (patients and multi-node types)
        
        Returns:
            Dictionary with keys: 'Patient', 'Diagnosis', 'Prescription', 'DischargeClinicalNote'
            Each value is a dict mapping node_id -> embedding
        """
        logger.info("=" * 80)
        logger.info("GENERATING STRUCTURAL EMBEDDINGS USING NEO4J GDS")
        logger.info("=" * 80)
        
        gds = Neo4jGDSEmbedding(self.connection, self.config.graph.graph_name)
        all_structural_embeddings = {}
        
        try:
            # Create projection
            logger.info("Creating GDS graph projection...")
            gds.create_projection(
                self.config.graph.node_labels,
                self.config.graph.relationship_types
            )
            
            # Generate FastRP embeddings
            logger.info("Computing FastRP embeddings (this may take 10-20 hours for 364K patients)...")
            start_time = time.time()
            
            gds.generate_fastrp_embeddings(
                embedding_dimension=self.config.embedding.fastrp_dimension,
                iteration_weights=self.config.embedding.fastrp_iteration_weights,
                normalization_strength=self.config.embedding.fastrp_normalization_strength,
                property_name="structuralEmbedding"
            )
            
            elapsed = time.time() - start_time
            logger.info(f"FastRP computation completed in {elapsed/3600:.2f} hours")
            
            # Get embeddings for all node types
            logger.info("Retrieving structural embeddings from Neo4j...")
            
            # Patient embeddings
            all_structural_embeddings['Patient'] = gds.get_patient_embeddings("structuralEmbedding")
            logger.info(f"Retrieved {len(all_structural_embeddings['Patient'])} Patient structural embeddings")
            
            # Multi-node embeddings
            for node_label in ['Diagnosis', 'Prescription', 'DischargeClinicalNote']:
                try:
                    embeddings = gds.get_node_embeddings(node_label, "structuralEmbedding")
                    all_structural_embeddings[node_label] = embeddings
                    logger.info(f"Retrieved {len(embeddings)} {node_label} structural embeddings")
                except Exception as e:
                    logger.warning(f"Could not retrieve {node_label} embeddings: {e}")
                    all_structural_embeddings[node_label] = {}
            
            # Clean up temporary structural embeddings from ALL nodes
            logger.info("Removing temporary structural embeddings from all node types...")
            
            # Count first
            count_query = """
            MATCH (n)
            WHERE n.structuralEmbedding IS NOT NULL
            RETURN count(n) AS total_count
            """
            count_result = self.connection.execute_query(count_query)
            total_count = count_result[0]['count'] if count_result else 0
            
            if total_count == 0:
                logger.info("No structural embeddings to clean up")
            else:
                logger.info(f"Found {total_count} nodes with structuralEmbedding to clean up")
                
                # Clean up in batches to avoid memory issues
                batch_size = 10000
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
                        cleanup_result = self.connection.execute_query(cleanup_query, {'batch_size': batch_size})
                        if cleanup_result:
                            cleaned = cleanup_result[0].get('cleaned', 0)
                            cleaned_total += cleaned
                            logger.info(f"Cleaned {cleaned_total}/{total_count} structural embeddings...")
                            
                            if cleaned == 0:
                                break  # No more nodes to clean
                        else:
                            break
                    except Exception as e:
                        logger.error(f"Error during cleanup batch: {e}")
                        break
                
                # Get final statistics by node type
                stats_query = """
                MATCH (n)
                WHERE n.structuralEmbedding IS NOT NULL
                RETURN labels(n)[0] AS node_type, count(n) AS remaining_count
                ORDER BY remaining_count DESC
                """
                
                try:
                    remaining = self.connection.execute_query(stats_query)
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
            
            return all_structural_embeddings
            
        finally:
            # Cleanup projection
            try:
                logger.info("Cleaning up GDS projection...")
                gds.drop_projection()
            except Exception as cleanup_error:
                logger.warning(f"Could not cleanup GDS projection (may not exist or GDS not available): {cleanup_error}")
    
    def process_batch(self, 
                     batch_index: int,
                     patient_ids: List[str],
                     patient_structural_embeddings: Dict[str, np.ndarray]) -> bool:
        """
        Process a single batch of patients
        
        Returns:
            True if successful, False otherwise
        """
        batch_start = time.time()
        logger.info(f"\n{'='*80}")
        logger.info(f"PROCESSING BATCH {batch_index + 1}")
        logger.info(f"Patients: {len(patient_ids)}")
        logger.info(f"{'='*80}")
        
        try:
            # Step 1: Extract text data
            logger.info("Extracting text data...")
            patient_data = self.subgraph_extractor.batch_extract_patient_subgraphs(
                patient_ids,
                include_text=True
            )
            
            # Step 2: Generate text embeddings
            logger.info("Generating text embeddings...")
            patient_text_data = {
                patient_id: data['text_data']
                for patient_id, data in patient_data.items()
            }
            text_embeddings = self.text_generator.generate_patient_embeddings_batch(
                patient_text_data
            )
            
            # Step 3: Get structural embeddings for this batch
            logger.info("Retrieving structural embeddings...")
            batch_structural = {
                pid: patient_structural_embeddings[pid]
                for pid in patient_ids
                if pid in patient_structural_embeddings
            }
            
            # Step 4: Combine embeddings
            logger.info("Combining embeddings...")
            combined_embeddings = self.combiner.combine_batch(
                batch_structural,
                text_embeddings
            )
            
            # Normalize
            combined_embeddings = self.combiner.normalize_embeddings(combined_embeddings)
            
            # Validate
            validator = EmbeddingValidator()
            expected_dim = self.config.embedding.combined_dimension
            
            valid_count = 0
            for patient_id, embedding in combined_embeddings.items():
                if validator.validate_embedding(embedding, expected_dim):
                    valid_count += 1
            
            logger.info(f"Validated {valid_count}/{len(combined_embeddings)} embeddings")
            
            # Step 5: Store both text and combined embeddings (ROOT CAUSE FIX)
            logger.info("Storing dual embeddings in Neo4j (text + combined)...")
            count = self.storage.store_patient_embeddings_dual(
                text_embeddings=text_embeddings,
                combined_embeddings=combined_embeddings,
                batch_size=100
            )
            
            batch_elapsed = time.time() - batch_start
            logger.info(f"[OK] Batch {batch_index + 1} completed in {batch_elapsed:.2f}s")
            logger.info(f"  Processed: {count} patients")
            logger.info(f"  Rate: {count/batch_elapsed:.2f} patients/sec")
            
            return True
            
        except Exception as e:
            logger.error(f"[FAILED] Batch {batch_index + 1} failed: {e}", exc_info=True)
            return False
    
    def process_multinode_embeddings(self, structural_embeddings: Dict[str, Dict[str, np.ndarray]] = None, 
                                     test_mode: bool = False, limit: Optional[int] = None, force: bool = False):
        """
        Process embeddings for multi-node types using PER-ITEM approach
        Creates separate item nodes for array fields (diagnoses, medications, labs, etc.)
        
        Args:
            structural_embeddings: Dict with keys (not used for per-item approach, can be None)
            test_mode: If True, only process a small sample for testing
            limit: Maximum number of nodes to process per type (None = all)
            force: If True, regenerate even if embeddings exist
        """
        logger.info("\n" + "=" * 80)
        logger.info("PROCESSING PER-ITEM EMBEDDINGS FOR ALL NODE TYPES")
        if test_mode or limit:
            logger.info(f"MODE: {'TEST' if test_mode else 'LIMITED'} - Processing up to {limit or 10} nodes per type")
        else:
            logger.info("MODE: FULL - Processing ALL nodes")
        if force:
            logger.info("FORCE: Will regenerate existing embeddings")
        logger.info("=" * 80)
        
        # Check what already exists
        if not force:
            existing_counts = self.check_existing_item_embeddings()
            total_existing = sum(existing_counts.values())
            if total_existing > 0:
                logger.info(f"\n[INFO] Found {total_existing} existing item embeddings")
                logger.info("      Item-level search is already functional!")
                logger.info("      Use --force-items to regenerate")
                logger.info("\n" + "=" * 80)
                return
        
        # Set limit based on mode
        if test_mode and limit is None:
            processing_limit = 10  # Super fast test mode
        elif test_mode:
            processing_limit = min(limit, 50)  # Cap test mode at 50
        else:
            processing_limit = limit
        
        # Process Diagnosis items
        try:
            logger.info("\n[1/4] Processing Diagnosis items...")
            self._process_diagnosis_embeddings_per_item(limit=processing_limit, force=force)
        except Exception as e:
            logger.error(f"Error processing Diagnosis item embeddings: {e}", exc_info=True)
        
        # Process Medication items
        try:
            logger.info("\n[2/4] Processing Medication items...")
            self._process_medication_embeddings_per_item(limit=processing_limit, force=force)
        except Exception as e:
            logger.error(f"Error processing Medication item embeddings: {e}", exc_info=True)
        
        # Process Lab Result items
        try:
            logger.info("\n[3/4] Processing Lab Result items...")
            self._process_lab_event_embeddings_per_item(limit=processing_limit, force=force)
        except Exception as e:
            logger.error(f"Error processing Lab Result item embeddings: {e}", exc_info=True)
        
        # Process Microbiology Result items
        try:
            logger.info("\n[4/4] Processing Microbiology Result items...")
            self._process_microbiology_event_embeddings_per_item(limit=processing_limit, force=force)
        except Exception as e:
            logger.error(f"Error processing Microbiology Result item embeddings: {e}", exc_info=True)
        
        logger.info("\n" + "=" * 80)
        logger.info("PER-ITEM EMBEDDING PROCESSING COMPLETE")
        logger.info("=" * 80)
    
    def _process_diagnosis_embeddings_per_item(self, batch_size: int = 1000, limit: Optional[int] = None, force: bool = False):
        """Process Diagnosis embeddings using per-item approach"""
        
        # Check if items already exist
        if not force:
            existing_query = """
            MATCH (di:DiagnosisItem)
            WHERE di.embedding IS NOT NULL
            RETURN count(di) AS count
            """
            try:
                result = self.connection.execute_query(existing_query)
                existing_count = result[0]['count'] if result else 0
                if existing_count > 0:
                    logger.info(f"[SKIP] Found {existing_count} existing diagnosis items. Use --force to regenerate.")
                    return
            except Exception:
                pass  # DiagnosisItem nodes don't exist yet, continue
        
        logger.info("Generating per-item diagnosis embeddings...")
        
        # Fetch diagnosis nodes with optional limit
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f"""
        MATCH (d:Diagnosis)
        RETURN 
            id(d) AS id,
            d.primary_diagnoses AS primary_diagnoses,
            d.secondary_diagnoses AS secondary_diagnoses
        {limit_clause}
        """
        
        diagnosis_nodes = self.connection.execute_query(query)
        if not diagnosis_nodes:
            logger.info("No diagnosis nodes found")
            return
        
        logger.info(f"Found {len(diagnosis_nodes)} diagnosis nodes")
        
        # Initialize per-item generator
        per_item_gen = PerItemEmbeddingGenerator(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        batch_processor = ItemEmbeddingBatchProcessor(per_item_gen)
        storage = ItemEmbeddingStorage(self.connection)
        
        # Process diagnosis items
        logger.info(f"Generating embeddings for {len(diagnosis_nodes)} diagnosis nodes...")
        diagnosis_items = batch_processor.process_diagnosis_nodes(diagnosis_nodes)
        
        if diagnosis_items:
            logger.info(f"Storing {len(diagnosis_items)} diagnosis items...")
            count = storage.store_diagnosis_items(diagnosis_items, batch_size=500)
            logger.info(f"[OK] Stored {count} diagnosis items")
        
        # Create vector index
        logger.info("Creating DiagnosisItem vector index...")
        diagnosis_index_manager = VectorIndexManager(
            self.connection,
            "diagnosis_item_embedding_index"
        )
        diagnosis_index_manager.create_vector_index(
            node_label="DiagnosisItem",
            property_name="embedding",
            dimension=384,  # all-MiniLM-L6-v2 dimension
            similarity_function=self.config.vector_search.similarity_function
        )
    
    def _process_medication_embeddings_per_item(self, batch_size: int = 1000, limit: Optional[int] = None, force: bool = False):
        """Process Medication embeddings using per-item approach"""
        
        # Check if items already exist
        if not force:
            existing_query = """
            MATCH (mi:MedicationItem)
            WHERE mi.embedding IS NOT NULL
            RETURN count(mi) AS count
            """
            try:
                result = self.connection.execute_query(existing_query)
                existing_count = result[0]['count'] if result else 0
                if existing_count > 0:
                    logger.info(f"[SKIP] Found {existing_count} existing medication items. Use --force to regenerate.")
                    return
            except Exception:
                pass  # MedicationItem nodes don't exist yet, continue
        
        logger.info("Generating per-item medication embeddings...")
        
        # Medication node types to process
        medication_node_types = [
            ('Prescription', 'medicines'),
            ('PreviousPrescriptionMeds', 'medications'),
            ('AdministeredMeds', 'medications'),
            ('MedicationStarted', 'medications'),
            ('DischargeMedications', 'medications'),
            ('AdmissionMedications', 'medications')
        ]
        
        limit_clause = f"LIMIT {limit}" if limit else "LIMIT 10000"
        
        # Initialize per-item generator
        per_item_gen = PerItemEmbeddingGenerator(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        batch_processor = ItemEmbeddingBatchProcessor(per_item_gen)
        storage = ItemEmbeddingStorage(self.connection)
        
        total_count = 0
        
        for node_label, field_name in medication_node_types:
            try:
                logger.info(f"\nProcessing {node_label}...")
                
                # Fetch medication nodes
                query = f"""
                MATCH (m:{node_label})
                WHERE m.{field_name} IS NOT NULL
                RETURN 
                    id(m) AS id,
                    m.{field_name} AS {field_name},
                    '{node_label}' AS type
                {limit_clause}
                """
                
                med_nodes = self.connection.execute_query(query)
                
                if not med_nodes:
                    logger.info(f"No {node_label} nodes found")
                    continue
                
                # Rename field to 'medications' for consistency
                for result in med_nodes:
                    result['medications'] = result.pop(field_name)
                
                logger.info(f"Fetched {len(med_nodes)} {node_label} nodes")
                
                # Process medication items
                logger.info(f"Generating embeddings for {node_label}...")
                med_items = batch_processor.process_medication_nodes(med_nodes, 'medications')
                
                if med_items:
                    logger.info(f"Storing {len(med_items)} {node_label} items...")
                    count = storage.store_medication_items(med_items, node_label, batch_size=500)
                    total_count += count
                    logger.info(f"[OK] Stored {count} items from {node_label}")
                    
            except Exception as e:
                logger.error(f"Error processing {node_label}: {e}", exc_info=True)
        
        logger.info(f"\n[OK] Total medication items stored: {total_count}")
        
        # Create vector index
        logger.info("Creating MedicationItem vector index...")
        medication_index_manager = VectorIndexManager(
            self.connection,
            "medication_item_embedding_index"
        )
        medication_index_manager.create_vector_index(
            node_label="MedicationItem",
            property_name="embedding",
            dimension=384,  # all-MiniLM-L6-v2 dimension
            similarity_function=self.config.vector_search.similarity_function
        )
    
    def _process_lab_event_embeddings_per_item(self, batch_size: int = 1000, limit: Optional[int] = None, force: bool = False):
        """Process LabEvent embeddings using per-item approach"""
        
        # Check if items already exist
        if not force:
            existing_query = """
            MATCH (li:LabResultItem)
            WHERE li.embedding IS NOT NULL
            RETURN count(li) AS count
            """
            try:
                result = self.connection.execute_query(existing_query)
                existing_count = result[0]['count'] if result else 0
                if existing_count > 0:
                    logger.info(f"[SKIP] Found {existing_count} existing lab result items. Use --force to regenerate.")
                    return
            except Exception:
                pass  # LabResultItem nodes don't exist yet, continue
        
        logger.info("Generating per-item lab result embeddings...")
        
        # Fetch LabEvent nodes with optional limit
        limit_clause = f"LIMIT {limit}" if limit else "LIMIT 10000"
        query = f"""
        MATCH (le:LabEvent)
        WHERE le.lab_results IS NOT NULL
        RETURN 
            id(le) AS id,
            le.lab_results AS lab_results
        {limit_clause}
        """
        
        lab_event_nodes = self.connection.execute_query(query)
        if not lab_event_nodes:
            logger.info("No LabEvent nodes found")
            return
        
        logger.info(f"Found {len(lab_event_nodes)} LabEvent nodes")
        
        # Initialize per-item generator
        per_item_gen = PerItemEmbeddingGenerator(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        batch_processor = ItemEmbeddingBatchProcessor(per_item_gen)
        storage = ItemEmbeddingStorage(self.connection)
        
        # Process lab result items
        logger.info(f"Generating embeddings for {len(lab_event_nodes)} lab event nodes...")
        lab_result_items = batch_processor.process_lab_event_nodes(lab_event_nodes)
        
        if lab_result_items:
            logger.info(f"Storing {len(lab_result_items)} lab result items...")
            count = storage.store_lab_result_items(lab_result_items, batch_size=500)
            logger.info(f"[OK] Stored {count} lab result items")
        
        # Create vector index
        logger.info("Creating LabResultItem vector index...")
        lab_index_manager = VectorIndexManager(
            self.connection,
            "lab_result_item_embedding_index"
        )
        lab_index_manager.create_vector_index(
            node_label="LabResultItem",
            property_name="embedding",
            dimension=384,  # all-MiniLM-L6-v2 dimension
            similarity_function=self.config.vector_search.similarity_function
        )
    
    def _process_microbiology_event_embeddings_per_item(self, batch_size: int = 1000, limit: Optional[int] = None, force: bool = False):
        """Process MicrobiologyEvent embeddings using per-item approach"""
        
        # Check if items already exist
        if not force:
            existing_query = """
            MATCH (mi:MicrobiologyResultItem)
            WHERE mi.embedding IS NOT NULL
            RETURN count(mi) AS count
            """
            try:
                result = self.connection.execute_query(existing_query)
                existing_count = result[0]['count'] if result else 0
                if existing_count > 0:
                    logger.info(f"[SKIP] Found {existing_count} existing microbiology result items. Use --force to regenerate.")
                    return
            except Exception:
                pass  # MicrobiologyResultItem nodes don't exist yet, continue
        
        logger.info("Generating per-item microbiology result embeddings...")
        
        # Fetch MicrobiologyEvent nodes with optional limit
        limit_clause = f"LIMIT {limit}" if limit else ""
        query = f"""
        MATCH (me:MicrobiologyEvent)
        WHERE me.micro_results IS NOT NULL
        RETURN 
            id(me) AS id,
            me.micro_results AS micro_results
        {limit_clause}
        """
        
        micro_event_nodes = self.connection.execute_query(query)
        if not micro_event_nodes:
            logger.info("No MicrobiologyEvent nodes found")
            return
        
        logger.info(f"Found {len(micro_event_nodes)} MicrobiologyEvent nodes")
        
        # Initialize per-item generator
        per_item_gen = PerItemEmbeddingGenerator(
            model_name="sentence-transformers/all-MiniLM-L6-v2"
        )
        batch_processor = ItemEmbeddingBatchProcessor(per_item_gen)
        storage = ItemEmbeddingStorage(self.connection)
        
        # Process microbiology result items
        logger.info(f"Generating embeddings for {len(micro_event_nodes)} microbiology event nodes...")
        micro_result_items = batch_processor.process_microbiology_nodes(micro_event_nodes)
        
        if micro_result_items:
            logger.info(f"Storing {len(micro_result_items)} microbiology result items...")
            count = storage.store_microbiology_result_items(micro_result_items, batch_size=500)
            logger.info(f"[OK] Stored {count} microbiology result items")
        
        # Create vector index
        logger.info("Creating MicrobiologyResultItem vector index...")
        micro_index_manager = VectorIndexManager(
            self.connection,
            "microbiology_result_item_embedding_index"
        )
        micro_index_manager.create_vector_index(
            node_label="MicrobiologyResultItem",
            property_name="embedding",
            dimension=384,  # all-MiniLM-L6-v2 dimension
            similarity_function=self.config.vector_search.similarity_function
        )
    
    def run_full_batch_pipeline(self, reset_progress: bool = False, skip_items: bool = False, force_items: bool = False, force_patients: bool = False):
        """
        Run complete batch pipeline for all unprocessed patients
        
        Args:
            reset_progress: If True, reset progress and start from beginning
            skip_items: If True, skip item embedding generation
            force_items: If True, regenerate item embeddings even if they exist
            force_patients: If True, regenerate patient embeddings even if they exist
        """
        logger.info("\n" + "=" * 80)
        logger.info("STARTING LARGE-SCALE HYBRID EMBEDDING PIPELINE")
        logger.info("=" * 80)
        
        total_start_time = time.time()
        
        if reset_progress:
            logger.info("Resetting progress...")
            self.progress.reset()
        
        try:
            # Step 1: Generate patient embeddings (node-level in Neo4j)
            logger.info("\n[STEP 1] Generating patient embeddings (Neo4j)...")
            self.pipeline.generate_patient_embeddings(
                patient_ids=None,  # Process all patients
                batch_size=self.config.batch_processing.batch_size,
                force=force_patients
            )
            
            # Step 2: Generate item embeddings (item-level in Milvus)
            if skip_items:
                logger.info("\n[SKIP] Skipping item embeddings (--skip-items flag)")
            else:
                logger.info("\n[STEP 2] Generating item embeddings (Milvus)...")
                self.pipeline.generate_item_embeddings(
                    limit=None,  # Process all items
                    force=force_items
                )
            
            # Final summary
            total_elapsed = time.time() - total_start_time
            
            logger.info("\n" + "=" * 80)
            logger.info("HYBRID PIPELINE COMPLETED")
            logger.info("=" * 80)
            logger.info(f"Total time: {total_elapsed/3600:.2f} hours")
            logger.info("Node-level embeddings stored in Neo4j")
            logger.info("Item-level embeddings stored in Milvus")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def create_vector_index(self):
        """Create vector indices in Neo4j (ROOT CAUSE FIX - creates both text and combined indices)"""
        logger.info("Creating vector search indices...")
        
        # Create combined embedding index (512 dim) for patient-to-patient similarity
        combined_index_manager = VectorIndexManager(
            self.connection,
            "patient_journey_index"
        )
        success1 = combined_index_manager.create_vector_index(
            node_label="Patient",
            property_name="combinedEmbedding",
            dimension=self.config.embedding.combined_dimension,
            similarity_function=self.config.vector_search.similarity_function
        )
        
        # Create text-only index (384 dim) for semantic queries
        text_index_manager = VectorIndexManager(
            self.connection,
            "patient_text_index"
        )
        success2 = text_index_manager.create_vector_index(
            node_label="Patient",
            property_name="textEmbedding",
            dimension=self.config.embedding.text_dimension,
            similarity_function=self.config.vector_search.similarity_function
        )
        
        if success1 and success2:
            logger.info("[OK] Both vector indices created successfully")
            logger.info("  - patient_journey_index (512 dim) for patient similarity")
            logger.info("  - patient_text_index (384 dim) for semantic queries")
        else:
            logger.warning("Some vector index creation failed (may already exist)")
    
    def run_simple_pipeline(self, patient_ids: List[str], skip_items: bool = False, force_items: bool = False):
        """
        Run pipeline for specific patient IDs (for testing/small datasets)
        Also processes multi-node embeddings
        
        Args:
            patient_ids: List of specific patient IDs to process
            skip_items: If True, skip item embedding generation
            force_items: If True, regenerate item embeddings even if they exist
        """
        logger.info("=" * 80)
        logger.info(f"RUNNING PIPELINE FOR {len(patient_ids)} SPECIFIC PATIENTS")
        logger.info("=" * 80)
        
        # Check existing state
        logger.info("\nChecking existing embeddings and indexes...")
        existing_items = self.check_existing_item_embeddings()
        existing_indexes = self.check_vector_indexes()
        
        start_time = time.time()
        
        try:
            # Generate structural embeddings using GDS (for all node types)
            logger.info("\nGenerating structural embeddings using Neo4j GDS...")
            all_structural_embeddings = self.generate_structural_embeddings_gds()
            
            # Filter to only requested patients
            filtered_structural = {pid: all_structural_embeddings['Patient'][pid] 
                                 for pid in patient_ids 
                                 if pid in all_structural_embeddings['Patient']}
            
            # Extract text data
            logger.info("Extracting patient text data...")
            patient_data = self.subgraph_extractor.batch_extract_patient_subgraphs(
                patient_ids,
                include_text=True
            )
            
            # Generate text embeddings
            logger.info("Generating text embeddings...")
            text_data = {pid: data.get('text_data', {}) for pid, data in patient_data.items()}
            text_embeddings = {}
            
            for patient_id, text in text_data.items():
                embedding = self.text_generator.generate_patient_text_embedding(text)
                text_embeddings[patient_id] = embedding
            
            # Combine embeddings
            logger.info("Combining embeddings...")
            combined_embeddings = {}
            for patient_id in patient_ids:
                if patient_id in filtered_structural and patient_id in text_embeddings:
                    combined = self.combiner.combine(
                        filtered_structural[patient_id],
                        text_embeddings[patient_id]
                    )
                    combined_embeddings[patient_id] = combined
            
            # Store both text and combined embeddings (ROOT CAUSE FIX)
            logger.info("Storing dual patient embeddings (text + combined)...")
            self.storage.store_patient_embeddings_dual(
                text_embeddings=text_embeddings,
                combined_embeddings=combined_embeddings,
                batch_size=100
            )
            
            # Create/update Patient vector indices (both text and combined)
            logger.info("Creating Patient vector search indices...")
            
            # Combined index (512 dim)
            combined_index_manager = VectorIndexManager(
                self.connection,
                "patient_journey_index"
            )
            combined_index_manager.create_vector_index(
                node_label="Patient",
                property_name="combinedEmbedding",
                dimension=self.config.embedding.combined_dimension,
                similarity_function=self.config.vector_search.similarity_function
            )
            
            # Text index (384 dim)
            text_index_manager = VectorIndexManager(
                self.connection,
                "patient_text_index"
            )
            text_index_manager.create_vector_index(
                node_label="Patient",
                property_name="textEmbedding",
                dimension=self.config.embedding.text_dimension,
                similarity_function=self.config.vector_search.similarity_function
            )
            
            # Process multi-node embeddings
            if skip_items:
                logger.info("\n[SKIP] Skipping item embeddings (--skip-items flag)")
            else:
                logger.info("\nProcessing multi-node embeddings...")
                test_mode = len(patient_ids) <= 10  # Auto-detect test mode
                self.process_multinode_embeddings(all_structural_embeddings, test_mode=test_mode, force=force_items)
            
            elapsed = time.time() - start_time
            logger.info("\n" + "=" * 80)
            logger.info(f"[OK] PIPELINE COMPLETED")
            logger.info(f"  Processed: {len(combined_embeddings)} patients")
            logger.info(f"  Time: {elapsed:.2f}s")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Simple pipeline failed: {e}", exc_info=True)
            raise
    
    def cleanup_structural_embeddings(self):
        """
        Utility method to clean up existing structuralEmbedding properties
        Useful for cleaning up after incomplete runs or fixing existing data
        """
        logger.info("=" * 80)
        logger.info("CLEANING UP EXISTING STRUCTURAL EMBEDDINGS")
        logger.info("=" * 80)
        
        # Get Neo4j connection from pipeline
        neo4j_conn = self.pipeline.neo4j if self.pipeline else None
        if not neo4j_conn:
            logger.error("Pipeline not initialized. Cannot access Neo4j connection.")
            return
        
        # Count first
        count_query = """
        MATCH (n)
        WHERE n.structuralEmbedding IS NOT NULL
        RETURN count(n) AS total_count
        """
        count_result = neo4j_conn.execute_query(count_query)
        total_count = count_result[0]['count'] if count_result else 0
        
        if total_count == 0:
            logger.info("No structural embeddings found to clean up")
            return
        
        logger.info(f"Found {total_count} nodes with structuralEmbedding to clean up")
        
        # Clean up in batches
        batch_size = 10000
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
                cleanup_result = neo4j_conn.execute_query(cleanup_query, {'batch_size': batch_size})
                if cleanup_result:
                    cleaned = cleanup_result[0].get('cleaned', 0)
                    cleaned_total += cleaned
                    logger.info(f"Cleaned {cleaned_total}/{total_count} structural embeddings...")
                    
                    if cleaned == 0:
                        break
                else:
                    break
            except Exception as e:
                logger.error(f"Error during cleanup batch: {e}")
                break
        
        # Final statistics
        stats_query = """
        MATCH (n)
        WHERE n.structuralEmbedding IS NOT NULL
        RETURN labels(n)[0] AS node_type, count(n) AS remaining_count
        ORDER BY remaining_count DESC
        """
        
        try:
            remaining = neo4j_conn.execute_query(stats_query)
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
        
        logger.info("=" * 80)
    
    def cleanup(self):
        """Cleanup resources"""
        if self.pipeline:
            self.pipeline.cleanup()
        logger.info("Pipeline cleanup complete")


def main():
    """Main entry point - defaults to batch mode for direct execution"""
    parser = argparse.ArgumentParser(
        description="Patient Embedding Pipeline - Production & Testing Modes"
    )
    parser.add_argument(
        '--mode',
        choices=['batch', 'test', 'specific'],
        default='batch',  # Default to batch mode for direct execution (e.g., clicking run button)
        help='Pipeline mode: batch (full dataset), test (5 patients), specific (provide patient IDs). Default: batch'
    )
    parser.add_argument(
        '--patient-ids',
        nargs='+',
        help='Specific patient IDs to process (use with --mode specific)'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        help='Reset progress and start from beginning (batch mode only)'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to config JSON file'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Override batch size from config'
    )
    parser.add_argument(
        '--item-limit',
        type=int,
        help='Limit number of nodes processed per item type (useful for testing)'
    )
    parser.add_argument(
        '--skip-items',
        action='store_true',
        help='Skip per-item embedding generation (only process patient embeddings)'
    )
    parser.add_argument(
        '--force-items',
        action='store_true',
        help='Force regeneration of item embeddings even if they already exist'
    )
    parser.add_argument(
        '--force-patients',
        action='store_true',
        help='Force regeneration of patient embeddings even if they already exist'
    )
    parser.add_argument(
        '--only-items',
        action='store_true',
        help='Only generate item embeddings, skip patient embeddings'
    )
    parser.add_argument(
        '--cleanup-structural',
        action='store_true',
        help='Clean up existing structuralEmbedding properties from all nodes (standalone operation)'
    )
    
    args = parser.parse_args()
    
    # Log execution mode
    logger.info("=" * 80)
    logger.info(f"EXECUTION MODE: {args.mode.upper()}")
    if args.mode == 'batch':
        logger.info("Running in BATCH mode (default) - processing full dataset")
    logger.info("=" * 80)
    
    # Load config
    if args.config:
        with open(args.config, 'r') as f:
            config_dict = json.load(f)
        config = Config(**config_dict)
    else:
        config = Config()
    
    # Override batch size if specified
    if args.batch_size:
        config.batch_processing.batch_size = args.batch_size
        logger.info(f"Using batch size: {args.batch_size}")
    
    # Initialize pipeline
    # Initialize ETL tracker for incremental loading
    tracker_file = project_root / 'logs' / 'etl_tracker.csv'
    tracker = None
    if ETLTracker is not None:
        tracker = ETLTracker(str(tracker_file))
        logger.info(f"Initialized ETL tracker from: {tracker_file}")
    
    pipeline = LargeScaleBatchPipeline(config, tracker=tracker, tracker_file=str(tracker_file) if tracker else None)
    
    try:
        pipeline.setup()  # Milvus is required
        
        # Handle cleanup mode (standalone operation)
        if args.cleanup_structural:
            logger.info("=" * 80)
            logger.info("CLEANUP MODE: Removing structuralEmbedding properties")
            logger.info("=" * 80)
            pipeline.cleanup_structural_embeddings()
            return
        
        # Handle only-items mode (independent item embedding generation)
        if args.only_items:
            logger.info("=" * 80)
            logger.info("ONLY-ITEMS MODE: Generating item embeddings only")
            logger.info("=" * 80)
            pipeline.process_multinode_embeddings(
                structural_embeddings=None,
                test_mode=(args.mode == 'test'),
                limit=args.item_limit,
                force=args.force_items
            )
        
        elif args.mode == 'batch':
            # Full batch processing for large datasets
            pipeline.run_full_batch_pipeline(
                reset_progress=args.reset,
                skip_items=args.skip_items,
                force_items=args.force_items,
                force_patients=args.force_patients
            )
            
        elif args.mode == 'test':
            # Test mode: process first 5 patients
            logger.info("TEST MODE: Processing first 5 patients")
            patient_ids = pipeline.connection.get_all_patient_ids()[:5]
            if not patient_ids:
                logger.error("No patients found in database")
                sys.exit(1)
            logger.info(f"Test patients: {patient_ids}")
            pipeline.run_simple_pipeline(
                patient_ids,
                skip_items=args.skip_items,
                force_items=args.force_items
            )
            
        elif args.mode == 'specific':
            # Process specific patient IDs
            if not args.patient_ids:
                logger.error("--patient-ids required when using --mode specific")
                sys.exit(1)
            logger.info(f"Processing {len(args.patient_ids)} specific patients")
            pipeline.run_simple_pipeline(
                args.patient_ids,
                skip_items=args.skip_items,
                force_items=args.force_items
            )
        
    except KeyboardInterrupt:
        logger.info("\nPipeline interrupted by user")
        if args.mode == 'batch':
            logger.info("Progress has been saved. Run again to resume.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()

