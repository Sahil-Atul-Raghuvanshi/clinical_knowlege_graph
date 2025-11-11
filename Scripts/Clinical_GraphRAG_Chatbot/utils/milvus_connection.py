"""
Milvus Vector Database Connection and Management
Handles connection to Milvus for storing item-level embeddings
"""
import logging
import socket
from typing import List, Dict, Any, Optional
import numpy as np
from pymilvus import (
    connections,
    Collection,
    FieldSchema,
    CollectionSchema,
    DataType,
    utility,
    MilvusException
)

logger = logging.getLogger(__name__)


class MilvusConnection:
    """Manages connection to Milvus vector database"""
    
    def __init__(self, host: str = "localhost", port: int = 19530, alias: str = "default"):
        """
        Initialize Milvus connection
        
        Args:
            host: Milvus server host
            port: Milvus server port
            alias: Connection alias
        """
        self.host = host
        self.port = port
        self.alias = alias
        self.connected = False
    
    def _check_port_available(self, timeout: float = 2.0) -> bool:
        """
        Check if Milvus port is available by attempting a socket connection
        
        Args:
            timeout: Connection timeout in seconds
            
        Returns:
            True if port is open, False otherwise
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.debug(f"Port check failed: {e}")
            return False
    
    def connect(self):
        """Connect to Milvus"""
        # First check if the port is available
        if not self._check_port_available():
            error_msg = (
                f"Milvus server is not available at {self.host}:{self.port}.\n"
                f"Please ensure Milvus is running. You can start it using:\n"
                f"  docker-compose up -d\n"
                f"Or check if the service is running:\n"
                f"  docker ps | grep milvus\n"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        
        try:
            connections.connect(
                alias=self.alias,
                host=self.host,
                port=self.port
            )
            self.connected = True
            logger.info(f"Connected to Milvus at {self.host}:{self.port}")
        except MilvusException as e:
            error_msg = (
                f"Failed to connect to Milvus at {self.host}:{self.port}\n"
                f"Error: {e}\n"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg) from e
        except Exception as e:
            logger.error(f"Failed to connect to Milvus: {e}")
            raise
    
    def disconnect(self):
        """Disconnect from Milvus"""
        try:
            connections.disconnect(self.alias)
            self.connected = False
            logger.info("Disconnected from Milvus")
        except Exception as e:
            logger.warning(f"Error disconnecting from Milvus: {e}")
    
    def check_connection(self) -> bool:
        """Check if connection is active"""
        try:
            # Try to list collections to verify connection
            utility.list_collections(using=self.alias)
            return True
        except Exception as e:
            logger.debug(f"Connection check failed: {e}")
            self.connected = False
            return False
    
    def list_collections(self) -> List[str]:
        """List all collections in Milvus"""
        try:
            return utility.list_collections()
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []


class MilvusCollectionManager:
    """Manages Milvus collections for different item types"""
    
    def __init__(self, connection: MilvusConnection, dimension: int = 384):
        """
        Initialize collection manager
        
        Args:
            connection: Milvus connection instance
            dimension: Embedding dimension (default 384 for all-MiniLM-L6-v2)
        """
        self.connection = connection
        self.dimension = dimension
        self.collections = {}
    
    def get_collection(self, collection_name: str) -> Optional[Collection]:
        """Get existing collection"""
        if not self.connection.connected:
            self.connection.connect()
        
        if utility.has_collection(collection_name):
            if collection_name not in self.collections:
                self.collections[collection_name] = Collection(collection_name)
            return self.collections[collection_name]
        
        logger.warning(f"Collection {collection_name} does not exist")
        return None
    
    def search_similar(
        self,
        collection_name: str,
        query_embedding: np.ndarray,
        top_k: int = 10,
        filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for similar items
        
        Args:
            collection_name: Name of the collection
            query_embedding: Query embedding vector
            top_k: Number of results to return
            filter_expr: Optional filter expression (e.g., 'item_type == "medication"')
            
        Returns:
            List of similar items with scores
        """
        collection = self.get_collection(collection_name)
        if not collection:
            logger.error(f"Collection {collection_name} not found")
            return []
        
        # Load collection into memory
        collection.load()
        
        # Prepare query
        search_params = {
            "metric_type": "COSINE",
            "params": {"ef": 64}  # HNSW search parameter
        }
        
        query_vector = query_embedding.reshape(1, -1).tolist() if isinstance(query_embedding, np.ndarray) else [query_embedding]
        
        try:
            results = collection.search(
                data=query_vector,
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                expr=filter_expr,
                output_fields=["item_id", "item_type", "text", "source_node_id", "metadata"]
            )
            
            # Format results
            similar_items = []
            for hit in results[0]:
                similar_items.append({
                    'item_id': hit.entity.get('item_id'),
                    'item_type': hit.entity.get('item_type'),
                    'text': hit.entity.get('text'),
                    'source_node_id': hit.entity.get('source_node_id'),
                    'metadata': hit.entity.get('metadata'),
                    'score': hit.score,
                    'distance': hit.distance
                })
            
            return similar_items
            
        except Exception as e:
            logger.error(f"Error searching in {collection_name}: {e}")
            return []

