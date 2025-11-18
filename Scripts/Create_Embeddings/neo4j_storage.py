"""
Neo4j storage for patient embeddings
Stores embeddings directly in Neo4j nodes
"""
import logging
import sys
from pathlib import Path
from typing import Dict
import numpy as np

# Add Scripts directory to path for utils imports
scripts_dir = Path(__file__).parent.parent
sys.path.insert(0, str(scripts_dir))

from utils.neo4j_connection import Neo4jConnection

logger = logging.getLogger(__name__)


class Neo4jEmbeddingStorage:
    """Neo4j-only storage for patient embeddings"""
    
    def __init__(self, neo4j_connection: Neo4jConnection):
        """
        Initialize Neo4j storage
        
        Args:
            neo4j_connection: Neo4j connection instance
        """
        self.neo4j = neo4j_connection
    
    def store_patient_embeddings(
        self,
        text_embeddings: Dict[str, np.ndarray],
        batch_size: int = 100
    ) -> int:
        """
        Store patient text embeddings in Neo4j
        
        Args:
            text_embeddings: Dictionary mapping patient_id to text embedding
            batch_size: Batch size for storage
            
        Returns:
            Number of patients stored
        """
        patient_ids = list(text_embeddings.keys())
        stored_count = 0
        
        for i in range(0, len(patient_ids), batch_size):
            batch_ids = patient_ids[i:i + batch_size]
            batch_data = []
            
            for patient_id in batch_ids:
                text_emb = text_embeddings.get(patient_id)
                
                if text_emb is None:
                    continue
                
                batch_data.append({
                    'patient_id': int(patient_id),
                    'text_embedding': text_emb.tolist() if isinstance(text_emb, np.ndarray) else text_emb
                })
            
            if not batch_data:
                continue
            
            # Store in Neo4j
            query = """
            UNWIND $batch AS item
            MATCH (p:Patient {subject_id: item.patient_id})
            SET p.textEmbedding = item.text_embedding
            RETURN count(p) AS count
            """
            
            try:
                result = self.neo4j.execute_query(query, {'batch': batch_data})
                if result:
                    count = result[0]['count']
                    stored_count += count
                    logger.info(f"Stored batch {i//batch_size + 1}: {count} patients")
            except Exception as e:
                logger.error(f"Error storing patient embeddings batch: {e}")
                continue
        
        logger.info(f"Stored {stored_count} patient embeddings in Neo4j")
        return stored_count
    
    def create_neo4j_vector_indexes(
        self,
        index_name: str,
        node_label: str,
        property_name: str,
        dimension: int,
        similarity_function: str = "cosine"
    ):
        """
        Create vector index in Neo4j
        
        Args:
            index_name: Name for the index
            node_label: Node label (e.g., 'Patient')
            property_name: Property containing embeddings
            dimension: Embedding dimension
            similarity_function: Similarity function (cosine or euclidean)
        """
        # Drop if exists
        try:
            self.neo4j.execute_query(f"DROP INDEX {index_name} IF EXISTS")
        except:
            pass
        
        # Create index
        query = f"""
        CREATE VECTOR INDEX {index_name}
        FOR (n:{node_label})
        ON (n.{property_name})
        OPTIONS {{
            indexConfig: {{
                `vector.dimensions`: {dimension},
                `vector.similarity_function`: "{similarity_function}"
            }}
        }}
        """
        
        try:
            self.neo4j.execute_query(query)
            logger.info(f"Created Neo4j vector index '{index_name}' on {node_label}.{property_name}")
        except Exception as e:
            logger.warning(f"Could not create vector index (may already exist): {e}")

