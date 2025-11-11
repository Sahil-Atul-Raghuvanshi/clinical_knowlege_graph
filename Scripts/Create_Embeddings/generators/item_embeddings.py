"""
Item-level embedding generator for storing in Milvus
Generates embeddings for individual items (medications, lab results, etc.)
"""
import logging
from typing import List, Dict, Any
import numpy as np
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class ItemEmbeddingGenerator:
    """Generate embeddings for individual items"""
    
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        """
        Initialize item embedding generator
        
        Args:
            model_name: SentenceTransformer model name
        """
        logger.info(f"Loading model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.dimension = self.model.get_sentence_embedding_dimension()
        logger.info(f"Model loaded. Dimension: {self.dimension}")
    
    def generate_item_embedding(self, text: str) -> np.ndarray:
        """Generate embedding for a single item"""
        if not text or not text.strip():
            return np.zeros(self.dimension)
        
        try:
            return self.model.encode(text, convert_to_numpy=True)
        except Exception as e:
            logger.error(f"Error generating item embedding: {e}")
            return np.zeros(self.dimension)
    
    def generate_item_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        """Generate embeddings for multiple items"""
        if not texts:
            return []
        
        valid_texts = [t for t in texts if t and t.strip()]
        if not valid_texts:
            return [np.zeros(self.dimension) for _ in texts]
        
        try:
            embeddings = self.model.encode(
                valid_texts,
                convert_to_numpy=True,
                show_progress_bar=len(valid_texts) > 100
            )
            return [emb for emb in embeddings]
        except Exception as e:
            logger.error(f"Error generating batch item embeddings: {e}")
            return [np.zeros(self.dimension) for _ in texts]
    
    def process_prescription_items(
        self,
        prescription_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process prescription nodes and extract individual medication items
        
        Args:
            prescription_nodes: List of prescription nodes with 'medicines' array
            
        Returns:
            List of items with embeddings ready for Milvus
        """
        items = []
        
        for node in prescription_nodes:
            medicines = node.get('medicines', [])
            source_node_id = str(node.get('id', ''))
            
            if not medicines:
                continue
            
            # Generate embeddings for each medication
            embeddings = self.generate_item_embeddings_batch(medicines)
            
            for idx, (med_text, embedding) in enumerate(zip(medicines, embeddings)):
                if med_text and med_text.strip():
                    items.append({
                        'item_id': f"presc_{source_node_id}_{idx}",
                        'item_type': 'prescription',
                        'text': med_text,
                        'source_node_id': source_node_id,
                        'embedding': embedding,
                        'metadata': {
                            'index': idx,
                            'node_type': 'Prescription'
                        }
                    })
        
        return items
    
    def process_microbiology_items(
        self,
        microbiology_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process microbiology event nodes"""
        items = []
        
        for node in microbiology_nodes:
            micro_results = node.get('micro_results', [])
            source_node_id = str(node.get('id', ''))
            
            if not micro_results:
                continue
            
            embeddings = self.generate_item_embeddings_batch(micro_results)
            
            for idx, (result_text, embedding) in enumerate(zip(micro_results, embeddings)):
                if result_text and result_text.strip():
                    items.append({
                        'item_id': f"micro_{source_node_id}_{idx}",
                        'item_type': 'microbiology',
                        'text': result_text,
                        'source_node_id': source_node_id,
                        'embedding': embedding,
                        'metadata': {
                            'index': idx,
                            'node_type': 'MicrobiologyEvent'
                        }
                    })
        
        return items
    
    def process_lab_result_items(
        self,
        lab_event_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process lab event nodes"""
        items = []
        
        for node in lab_event_nodes:
            lab_results = node.get('lab_results', [])
            source_node_id = str(node.get('id', ''))
            
            if not lab_results:
                continue
            
            embeddings = self.generate_item_embeddings_batch(lab_results)
            
            for idx, (result_text, embedding) in enumerate(zip(lab_results, embeddings)):
                if result_text and result_text.strip():
                    items.append({
                        'item_id': f"lab_{source_node_id}_{idx}",
                        'item_type': 'lab_result',
                        'text': result_text,
                        'source_node_id': source_node_id,
                        'embedding': embedding,
                        'metadata': {
                            'index': idx,
                            'node_type': 'LabEvent'
                        }
                    })
        
        return items
    
    def process_diagnosis_items(
        self,
        diagnosis_nodes: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process diagnosis nodes"""
        items = []
        
        for node in diagnosis_nodes:
            source_node_id = str(node.get('id', ''))
            
            # Process primary diagnoses
            primary = node.get('primary_diagnoses', [])
            if primary:
                embeddings = self.generate_item_embeddings_batch(primary)
                for idx, (diag_text, embedding) in enumerate(zip(primary, embeddings)):
                    if diag_text and diag_text.strip():
                        items.append({
                            'item_id': f"diag_primary_{source_node_id}_{idx}",
                            'item_type': 'diagnosis',
                            'text': diag_text,
                            'source_node_id': source_node_id,
                            'embedding': embedding,
                            'metadata': {
                                'index': idx,
                                'diagnosis_type': 'primary',
                                'node_type': 'Diagnosis'
                            }
                        })
            
            # Process secondary diagnoses
            secondary = node.get('secondary_diagnoses', [])
            if secondary:
                embeddings = self.generate_item_embeddings_batch(secondary)
                for idx, (diag_text, embedding) in enumerate(zip(secondary, embeddings)):
                    if diag_text and diag_text.strip():
                        items.append({
                            'item_id': f"diag_secondary_{source_node_id}_{idx}",
                            'item_type': 'diagnosis',
                            'text': diag_text,
                            'source_node_id': source_node_id,
                            'embedding': embedding,
                            'metadata': {
                                'index': idx,
                                'diagnosis_type': 'secondary',
                                'node_type': 'Diagnosis'
                            }
                        })
        
        return items

