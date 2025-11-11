"""
Main embedding pipeline for hybrid system
Generates node-level embeddings (Neo4j) and item-level embeddings (Milvus)
"""
import logging
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional
import numpy as np
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from Embeddings.utils.config import Config
from Embeddings.utils.neo4j_connection import Neo4jConnection
from Embeddings.utils.milvus_connection import MilvusConnection
from Embeddings.generators.enhanced_text_extractor import EnhancedTextExtractor
from Embeddings.generators.text_embeddings import TextEmbeddingGenerator
from Embeddings.generators.structural_embeddings import StructuralEmbeddingGenerator
from Embeddings.generators.combined_embeddings import CombinedEmbeddingGenerator
from Embeddings.generators.item_embeddings import ItemEmbeddingGenerator
from Embeddings.storage.hybrid_storage import HybridEmbeddingStorage

logger = logging.getLogger(__name__)


class HybridEmbeddingPipeline:
    """Main pipeline for generating and storing embeddings"""
    
    def __init__(self, config: Config):
        """
        Initialize pipeline
        
        Args:
            config: Configuration object
        """
        self.config = config
        
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
    
    def generate_patient_embeddings(
        self,
        patient_ids: Optional[List[str]] = None,
        batch_size: int = 2000
    ):
        """
        Generate embeddings for patients (node-level)
        
        Args:
            patient_ids: List of patient IDs (None = all patients)
            batch_size: Batch size for processing
        """
        logger.info("=" * 80)
        logger.info("GENERATING PATIENT EMBEDDINGS (NODE-LEVEL)")
        logger.info("=" * 80)
        
        # Get patient IDs
        if patient_ids is None:
            patient_ids = self.neo4j.get_all_patient_ids()
        
        logger.info(f"Processing {len(patient_ids)} patients")
        
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
        
        for i in tqdm(range(0, len(patient_ids), batch_size), desc="Processing batches"):
            batch_ids = patient_ids[i:i + batch_size]
            
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
        
        # Step 4: Create vector indexes
        logger.info("\n[4/4] Creating vector indexes...")
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
        
        # Step 5: Cleanup temporary structural embeddings from ALL nodes
        logger.info("\n[5/5] Cleaning up temporary structural embeddings...")
        self._cleanup_structural_embeddings()
        
        # Cleanup projection
        self.structural_generator.drop_projection()
        
        logger.info("\n" + "=" * 80)
        logger.info("PATIENT EMBEDDING GENERATION COMPLETE")
        logger.info("=" * 80)
    
    def generate_item_embeddings(
        self,
        limit: Optional[int] = None,
        force: bool = False
    ):
        """
        Generate item-level embeddings and store in Milvus
        
        Args:
            limit: Limit number of nodes to process per type (None = all)
            force: Force regeneration even if items exist
        """
        logger.info("=" * 80)
        logger.info("GENERATING ITEM EMBEDDINGS (MILVUS)")
        logger.info("=" * 80)
        
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        # Process Prescriptions
        logger.info("\n[1/4] Processing Prescription items...")
        try:
            query_prescriptions = f"""
            MATCH (p:Prescription)
            WHERE p.medicines IS NOT NULL
            RETURN id(p) AS id, p.medicines AS medicines
            {limit_clause}
            """
            prescription_nodes = self.neo4j.execute_query(query_prescriptions)
            if prescription_nodes:
                prescription_items = self.item_generator.process_prescription_items(prescription_nodes)
                if prescription_items:
                    self.storage.store_item_embeddings(
                        prescription_items,
                        self.config.milvus.prescription_collection,
                        batch_size=self.config.batch_processing.item_batch_size
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
            RETURN id(me) AS id, me.micro_results AS micro_results
            {limit_clause}
            """
            micro_nodes = self.neo4j.execute_query(query_micro)
            if micro_nodes:
                micro_items = self.item_generator.process_microbiology_items(micro_nodes)
                if micro_items:
                    self.storage.store_item_embeddings(
                        micro_items,
                        self.config.milvus.microbiology_collection,
                        batch_size=self.config.batch_processing.item_batch_size
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
            RETURN id(le) AS id, le.lab_results AS lab_results
            {limit_clause}
            """
            lab_nodes = self.neo4j.execute_query(query_labs)
            if lab_nodes:
                lab_items = self.item_generator.process_lab_result_items(lab_nodes)
                if lab_items:
                    self.storage.store_item_embeddings(
                        lab_items,
                        self.config.milvus.lab_result_collection,
                        batch_size=self.config.batch_processing.item_batch_size
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
            RETURN id(d) AS id, d.primary_diagnoses AS primary_diagnoses, d.secondary_diagnoses AS secondary_diagnoses
            {limit_clause}
            """
            diag_nodes = self.neo4j.execute_query(query_diag)
            if diag_nodes:
                diag_items = self.item_generator.process_diagnosis_items(diag_nodes)
                if diag_items:
                    self.storage.store_item_embeddings(
                        diag_items,
                        self.config.milvus.diagnosis_collection,
                        batch_size=self.config.batch_processing.item_batch_size
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

