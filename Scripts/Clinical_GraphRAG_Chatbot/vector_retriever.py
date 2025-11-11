"""
Vector Retriever Module
Performs semantic similarity search in Milvus
"""
import logging
import numpy as np
from typing import List, Dict, Any, Optional
from pathlib import Path
from sentence_transformers import SentenceTransformer

# Import from local utils
from utils.milvus_connection import MilvusConnection, MilvusCollectionManager
from utils.config import MilvusConfig

logger = logging.getLogger(__name__)


class VectorRetriever:
    """
    Retrieves semantically similar items from Milvus
    
    Note: This handles ITEM-LEVEL embeddings (diagnoses, medications, lab results, etc.)
    stored in Milvus collections. For NODE-LEVEL patient similarity, use GraphRetriever
    which uses Patient.combinedEmbedding from Neo4j.
    """
    
    def __init__(
        self, 
        config: Optional[MilvusConfig] = None,
        embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    ):
        """
        Initialize vector retriever
        
        Args:
            config: Milvus configuration (uses default if not provided)
            embedding_model: Model name for query embeddings
        """
        if config is None:
            # Try to load from JSON first, fallback to env
            try:
                config = MilvusConfig.from_json()
            except Exception:
                config = MilvusConfig.from_env()
        
        self.config = config
        self.connection = MilvusConnection(
            host=config.host,
            port=config.port,
            alias=config.alias
        )
        self.connection.connect()
        
        # Initialize embedding model
        logger.info(f"Loading embedding model: {embedding_model}")
        self.embedding_model = SentenceTransformer(embedding_model)
        
        # Initialize collection manager
        self.collection_manager = MilvusCollectionManager(
            connection=self.connection,
            dimension=config.dimension
        )
        
        # Collection names
        self.collections = {
            "prescription": config.prescription_collection,
            "microbiology": config.microbiology_collection,
            "lab_result": config.lab_result_collection,
            "diagnosis": config.diagnosis_collection
        }
        
        logger.info("VectorRetriever initialized")
    
    def close(self):
        """Close Milvus connection"""
        if self.connection:
            self.connection.disconnect()
    
    def encode_query(self, query: str) -> np.ndarray:
        """
        Encode query text into embedding vector
        
        Args:
            query: Query text
            
        Returns:
            Embedding vector
        """
        embedding = self.embedding_model.encode(query, convert_to_numpy=True)
        return embedding
    
    def search_collection(
        self,
        collection_name: str,
        query_embedding: np.ndarray,
        top_k: int = 10,
        filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search a single Milvus collection
        
        Args:
            collection_name: Name of the collection
            query_embedding: Query embedding vector
            top_k: Number of results to return
            filter_expr: Optional filter expression
            
        Returns:
            List of similar items with scores
        """
        try:
            results = self.collection_manager.search_similar(
                collection_name=collection_name,
                query_embedding=query_embedding,
                top_k=top_k,
                filter_expr=filter_expr
            )
            logger.info(f"Found {len(results)} results in {collection_name}")
            return results
        except Exception as e:
            logger.error(f"Error searching collection {collection_name}: {e}")
            return []
    
    def search_all_collections(
        self,
        query: str,
        top_k_per_collection: int = 5,
        filter_expr: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Search across all Milvus collections
        
        Args:
            query: Query text
            top_k_per_collection: Number of results per collection
            filter_expr: Optional filter expression
            
        Returns:
            Dictionary mapping collection names to results
        """
        # Encode query
        query_embedding = self.encode_query(query)
        
        all_results = {}
        
        # Search each collection
        for collection_type, collection_name in self.collections.items():
            try:
                results = self.search_collection(
                    collection_name=collection_name,
                    query_embedding=query_embedding,
                    top_k=top_k_per_collection,
                    filter_expr=filter_expr
                )
                all_results[collection_type] = results
            except Exception as e:
                logger.warning(f"Failed to search {collection_name}: {e}")
                all_results[collection_type] = []
        
        return all_results
    
    def search_by_intent(
        self,
        query: str,
        intent: str,
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search collections based on query intent
        
        Args:
            query: Query text
            intent: Query intent (patient_similarity, treatment_recommendation, etc.)
            top_k: Number of results to return
            
        Returns:
            List of relevant results
        """
        query_embedding = self.encode_query(query)
        results = []
        
        # Prioritize collections based on intent
        if intent == "patient_similarity" or intent == "clinical_summary":
            # Focus on diagnoses and medications
            for coll_type in ["diagnosis", "prescription"]:
                coll_name = self.collections[coll_type]
                coll_results = self.search_collection(
                    collection_name=coll_name,
                    query_embedding=query_embedding,
                    top_k=top_k // 2
                )
                results.extend(coll_results)
        
        elif intent == "treatment_recommendation":
            # Focus on medications and lab results
            for coll_type in ["prescription", "lab_result"]:
                coll_name = self.collections[coll_type]
                coll_results = self.search_collection(
                    collection_name=coll_name,
                    query_embedding=query_embedding,
                    top_k=top_k // 2
                )
                results.extend(coll_results)
        
        else:
            # General search across all collections
            all_results = self.search_all_collections(
                query=query,
                top_k_per_collection=top_k // len(self.collections)
            )
            for coll_results in all_results.values():
                results.extend(coll_results)
        
        # Sort by score and return top_k
        results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return results[:top_k]
    
    def format_vector_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format vector search results into structured format
        
        Args:
            results: Raw vector search results
            
        Returns:
            Formatted dictionary
        """
        formatted = {
            "medications": [],
            "lab_results": [],
            "diagnoses": [],
            "microbiology": []
        }
        
        for result in results:
            item_type = result.get('item_type', '').lower()
            item_data = {
                "text": result.get('text'),
                "source_node_id": result.get('source_node_id'),
                "item_id": result.get('item_id'),
                "similarity_score": result.get('score', 0),
                "metadata": result.get('metadata', {})
            }
            
            if 'prescription' in item_type or 'medication' in item_type:
                formatted["medications"].append(item_data)
            elif 'lab' in item_type:
                formatted["lab_results"].append(item_data)
            elif 'diagnosis' in item_type:
                formatted["diagnoses"].append(item_data)
            elif 'microbiology' in item_type:
                formatted["microbiology"].append(item_data)
        
        return formatted

