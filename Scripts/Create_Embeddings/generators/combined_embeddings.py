"""
Combined embedding generator that merges structural and textual embeddings
"""
import logging
from typing import Dict
import numpy as np

logger = logging.getLogger(__name__)


class CombinedEmbeddingGenerator:
    """Combine structural and textual embeddings"""
    
    def __init__(
        self,
        combine_method: str = "concatenate",
        structural_weight: float = 0.5,
        textual_weight: float = 0.5
    ):
        """
        Initialize combined embedding generator
        
        Args:
            combine_method: Method to combine ('concatenate' or 'weighted_sum')
            structural_weight: Weight for structural embedding (weighted_sum only)
            textual_weight: Weight for textual embedding (weighted_sum only)
        """
        self.combine_method = combine_method
        self.structural_weight = structural_weight
        self.textual_weight = textual_weight
    
    def combine(
        self,
        structural_embedding: np.ndarray,
        textual_embedding: np.ndarray
    ) -> np.ndarray:
        """
        Combine structural and textual embeddings
        
        Args:
            structural_embedding: Structural embedding vector
            textual_embedding: Textual embedding vector
            
        Returns:
            Combined embedding vector
        """
        if self.combine_method == "concatenate":
            return np.concatenate([structural_embedding, textual_embedding])
        elif self.combine_method == "weighted_sum":
            # Normalize weights
            total_weight = self.structural_weight + self.textual_weight
            w1 = self.structural_weight / total_weight
            w2 = self.textual_weight / total_weight
            
            # Ensure same dimension
            if len(structural_embedding) != len(textual_embedding):
                min_dim = min(len(structural_embedding), len(textual_embedding))
                structural_embedding = structural_embedding[:min_dim]
                textual_embedding = textual_embedding[:min_dim]
            
            return w1 * structural_embedding + w2 * textual_embedding
        else:
            raise ValueError(f"Unknown combine method: {self.combine_method}")
    
    def combine_batch(
        self,
        structural_embeddings: Dict[str, np.ndarray],
        textual_embeddings: Dict[str, np.ndarray]
    ) -> Dict[str, np.ndarray]:
        """
        Combine embeddings for multiple patients
        
        Args:
            structural_embeddings: Dictionary mapping patient_id to structural embedding
            textual_embeddings: Dictionary mapping patient_id to textual embedding
            
        Returns:
            Dictionary mapping patient_id to combined embedding
        """
        combined = {}
        
        for patient_id in structural_embeddings.keys():
            if patient_id in textual_embeddings:
                try:
                    combined[patient_id] = self.combine(
                        structural_embeddings[patient_id],
                        textual_embeddings[patient_id]
                    )
                except Exception as e:
                    logger.error(f"Error combining embeddings for patient {patient_id}: {e}")
                    continue
        
        logger.info(f"Combined {len(combined)} patient embeddings")
        return combined
    
    def normalize_embeddings(self, embeddings: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """
        Normalize embeddings to unit length
        
        Args:
            embeddings: Dictionary mapping patient_id to embedding
            
        Returns:
            Dictionary with normalized embeddings
        """
        normalized = {}
        for patient_id, embedding in embeddings.items():
            norm = np.linalg.norm(embedding)
            if norm > 0:
                normalized[patient_id] = embedding / norm
            else:
                normalized[patient_id] = embedding
        
        return normalized

