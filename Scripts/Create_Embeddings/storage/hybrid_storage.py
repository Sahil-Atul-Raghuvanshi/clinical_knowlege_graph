"""
Hybrid storage for embeddings: Neo4j for node-level, Milvus for item-level
"""
import logging
from typing import Dict, List, Any, Optional
import numpy as np
from ..utils.neo4j_connection import Neo4jConnection
from ..utils.milvus_connection import MilvusConnection, MilvusCollectionManager

logger = logging.getLogger(__name__)


class HybridEmbeddingStorage:
    """Hybrid storage: Neo4j for nodes, Milvus for items"""
    
    def __init__(
        self,
        neo4j_connection: Neo4jConnection,
        milvus_connection: MilvusConnection,
        embedding_dimension: int = 384
    ):
        """
        Initialize hybrid storage
        
        Args:
            neo4j_connection: Neo4j connection for node-level embeddings
            milvus_connection: Milvus connection for item-level embeddings
            embedding_dimension: Dimension of item embeddings
        """
        self.neo4j = neo4j_connection
        self.milvus_conn = milvus_connection
        self.milvus_manager = MilvusCollectionManager(milvus_connection, embedding_dimension)
        
        # Ensure Milvus is connected (required)
        if not self.milvus_conn.connected:
            self.milvus_conn.connect()
    
    def store_patient_embeddings(
        self,
        text_embeddings: Dict[str, np.ndarray],
        combined_embeddings: Dict[str, np.ndarray],
        batch_size: int = 100
    ) -> int:
        """
        Store patient embeddings in Neo4j (node-level)
        
        Args:
            text_embeddings: Dictionary mapping patient_id to text embedding
            combined_embeddings: Dictionary mapping patient_id to combined embedding
            batch_size: Batch size for storage
            
        Returns:
            Number of patients stored
        """
        patient_ids = list(combined_embeddings.keys())
        stored_count = 0
        
        for i in range(0, len(patient_ids), batch_size):
            batch_ids = patient_ids[i:i + batch_size]
            batch_data = []
            
            for patient_id in batch_ids:
                text_emb = text_embeddings.get(patient_id)
                combined_emb = combined_embeddings.get(patient_id)
                
                if text_emb is None or combined_emb is None:
                    continue
                
                batch_data.append({
                    'patient_id': int(patient_id),
                    'text_embedding': text_emb.tolist() if isinstance(text_emb, np.ndarray) else text_emb,
                    'combined_embedding': combined_emb.tolist() if isinstance(combined_emb, np.ndarray) else combined_emb
                })
            
            if not batch_data:
                continue
            
            # Store in Neo4j
            query = """
            UNWIND $batch AS item
            MATCH (p:Patient {subject_id: item.patient_id})
            SET p.textEmbedding = item.text_embedding,
                p.combinedEmbedding = item.combined_embedding
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
    
    def store_item_embeddings(
        self,
        items: List[Dict[str, Any]],
        collection_name: str,
        batch_size: int = 5000
    ) -> int:
        """
        Store item embeddings in Milvus
        
        Args:
            items: List of items with embeddings
            collection_name: Milvus collection name
            batch_size: Batch size for insertion
            
        Returns:
            Number of items stored
        """
        # Create collection if it doesn't exist
        self.milvus_manager.create_collection(
            collection_name,
            description=f"Item embeddings for {collection_name}",
            index_type="HNSW",
            metric_type="COSINE"
        )
        
        # Insert embeddings
        stored_count = self.milvus_manager.insert_embeddings(
            collection_name,
            items,
            batch_size=batch_size
        )
        
        logger.info(f"Stored {stored_count} items in Milvus collection '{collection_name}'")
        return stored_count
    
    def search_similar_items(
        self,
        query_embedding: np.ndarray,
        collection_name: str,
        top_k: int = 10,
        item_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar items in Milvus
        
        Args:
            query_embedding: Query embedding vector
            collection_name: Milvus collection name
            top_k: Number of results to return
            item_type: Optional filter by item type
            
        Returns:
            List of similar items with scores
        """
        filter_expr = None
        if item_type:
            filter_expr = f'item_type == "{item_type}"'
        
        return self.milvus_manager.search_similar(
            collection_name,
            query_embedding,
            top_k=top_k,
            filter_expr=filter_expr
        )
    
    def create_neo4j_vector_indexes(
        self,
        index_name: str,
        property_name: str,
        dimension: int,
        similarity_function: str = "cosine"
    ):
        """
        Create vector index in Neo4j for patient embeddings
        
        Args:
            index_name: Name for the index
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
        FOR (n:Patient)
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
            logger.info(f"Created Neo4j vector index '{index_name}' on Patient.{property_name}")
        except Exception as e:
            logger.warning(f"Could not create vector index (may already exist): {e}")

