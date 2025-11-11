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
                f"\n"
                f"If using Docker Compose, make sure you're in the project root directory\n"
                f"and run: docker-compose up -d standalone\n"
                f"\n"
                f"To verify Milvus is ready, wait for the health check to pass:\n"
                f"  docker logs milvus-standalone"
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
                f"\n"
                f"Troubleshooting steps:\n"
                f"1. Verify Milvus is running: docker ps | grep milvus\n"
                f"2. Check Milvus logs: docker logs milvus-standalone\n"
                f"3. Ensure all dependencies are running: docker-compose ps\n"
                f"4. Wait for Milvus to fully start (may take 30-60 seconds after docker-compose up)\n"
                f"5. Check if port {self.port} is already in use by another service"
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
    
    def create_collection(
        self,
        collection_name: str,
        description: str = "",
        index_type: str = "HNSW",
        metric_type: str = "COSINE"
    ) -> Collection:
        """
        Create a new collection in Milvus
        
        Args:
            collection_name: Name of the collection
            description: Collection description
            index_type: Index type (HNSW, IVF_FLAT, etc.)
            metric_type: Similarity metric (COSINE, L2, IP)
            
        Returns:
            Collection object
        """
        if not self.connection.connected:
            self.connection.connect()
        
        # Check if collection already exists
        if utility.has_collection(collection_name):
            logger.info(f"Collection {collection_name} already exists")
            return Collection(collection_name)
        
        # Define schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="item_id", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="item_type", dtype=DataType.VARCHAR, max_length=50),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=10000),
            FieldSchema(name="source_node_id", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="metadata", dtype=DataType.JSON),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=self.dimension)
        ]
        
        schema = CollectionSchema(
            fields=fields,
            description=description
        )
        
        # Create collection
        collection = Collection(
            name=collection_name,
            schema=schema,
            using=self.connection.alias
        )
        
        # Create index on embedding field
        index_params = {
            "metric_type": metric_type,
            "index_type": index_type,
            "params": {"M": 16, "efConstruction": 200} if index_type == "HNSW" else {}
        }
        
        collection.create_index(
            field_name="embedding",
            index_params=index_params
        )
        
        logger.info(f"Created collection {collection_name} with {index_type} index")
        self.collections[collection_name] = collection
        
        return collection
    
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
    
    def insert_embeddings(
        self,
        collection_name: str,
        items: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """
        Insert embeddings into collection
        
        Args:
            collection_name: Name of the collection
            items: List of items with embeddings
            batch_size: Batch size for insertion
            
        Returns:
            Number of items inserted
        """
        collection = self.get_collection(collection_name)
        if not collection:
            logger.error(f"Collection {collection_name} not found")
            return 0
        
        total_inserted = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            # Prepare data for insertion
            item_ids = [item['item_id'] for item in batch]
            item_types = [item.get('item_type', 'unknown') for item in batch]
            texts = [item.get('text', '') for item in batch]
            source_node_ids = [item.get('source_node_id', '') for item in batch]
            metadatas = [item.get('metadata', {}) for item in batch]
            embeddings = [
                item['embedding'].tolist() if isinstance(item['embedding'], np.ndarray) 
                else item['embedding']
                for item in batch
            ]
            
            # Retry logic for batch insertion
            max_retries = 3
            inserted = False
            for attempt in range(max_retries):
                try:
                    # Check connection before each attempt
                    if not self.connection.check_connection():
                        logger.warning(f"Connection lost, reconnecting (attempt {attempt + 1}/{max_retries})...")
                        self.connection.connect()
                        collection = self.get_collection(collection_name)
                        if not collection:
                            raise Exception(f"Could not get collection {collection_name} after reconnection")
                    
                    data = [
                        item_ids,
                        item_types,
                        texts,
                        source_node_ids,
                        metadatas,
                        embeddings
                    ]
                    
                    collection.insert(data)
                    total_inserted += len(batch)
                    inserted = True
                    
                    logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch)} items into {collection_name}")
                    break  # Success, exit retry loop
                    
                except MilvusException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Error inserting batch {i//batch_size + 1} (attempt {attempt + 1}/{max_retries}): {e}")
                        logger.info("Retrying after connection check...")
                        # Mark connection as potentially lost
                        self.connection.connected = False
                    else:
                        logger.error(f"Failed to insert batch {i//batch_size + 1} after {max_retries} attempts: {e}")
                        break
                except Exception as e:
                    logger.error(f"Unexpected error inserting batch {i//batch_size + 1}: {e}")
                    break  # Don't retry for unexpected errors
            
            if not inserted:
                logger.warning(f"Skipping batch {i//batch_size + 1} due to insertion failures")
        
        # Flush to ensure data is written (with error handling)
        try:
            # Check connection before flushing
            if not self.connection.check_connection():
                logger.warning(f"Milvus connection lost, attempting to reconnect...")
                self.connection.connect()
                # Re-get collection after reconnection
                collection = self.get_collection(collection_name)
                if not collection:
                    logger.error(f"Could not re-establish collection {collection_name} after reconnection")
                    return total_inserted
            
            # Attempt flush with timeout
            try:
                collection.flush(timeout=30)  # 30 second timeout
                logger.info(f"Flushed {collection_name} successfully")
            except MilvusException as e:
                # If flush fails but data was inserted, log warning and continue
                logger.warning(f"Flush failed for {collection_name} (data may still be persisted): {e}")
                logger.info(f"Note: Milvus may persist data automatically. Total inserted: {total_inserted} items")
        except Exception as e:
            logger.warning(f"Error during flush for {collection_name}: {e}")
            logger.info(f"Data insertion completed. Total inserted: {total_inserted} items (flush may have failed)")
        
        logger.info(f"Total inserted into {collection_name}: {total_inserted} items")
        
        return total_inserted
    
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
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics about a collection"""
        collection = self.get_collection(collection_name)
        if not collection:
            return {}
        
        try:
            stats = {
                'name': collection_name,
                'num_entities': collection.num_entities,
                'schema': str(collection.schema),
                'indexes': [str(idx) for idx in collection.indexes]
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting stats for {collection_name}: {e}")
            return {}

