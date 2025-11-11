"""
Structural embedding generator using Neo4j GDS FastRP
"""
import logging
from typing import Dict, List, Optional
import numpy as np
from ..utils.neo4j_connection import Neo4jConnection

logger = logging.getLogger(__name__)


class StructuralEmbeddingGenerator:
    """Generate structural embeddings using Neo4j GDS"""
    
    def __init__(self, connection: Neo4jConnection, graph_name: str = "patient_journey_graph"):
        """
        Initialize structural embedding generator
        
        Args:
            connection: Neo4j connection instance
            graph_name: Name for GDS graph projection
        """
        self.connection = connection
        self.graph_name = graph_name
    
    def create_projection(
        self,
        node_labels: List[str],
        relationship_types: List[str]
    ):
        """Create GDS graph projection"""
        # Drop if exists
        try:
            drop_query = "CALL gds.graph.drop($graphName) YIELD graphName"
            self.connection.execute_query(drop_query, {'graphName': self.graph_name})
        except:
            pass
        
        # Get existing labels and relationship types from database
        existing_labels = self.connection.get_existing_node_labels()
        existing_rel_types = self.connection.get_existing_relationship_types()
        
        # Filter node labels to only include those that exist
        valid_node_labels = [label for label in node_labels if label in existing_labels]
        missing_labels = [label for label in node_labels if label not in existing_labels]
        
        if missing_labels:
            logger.warning(f"The following node labels do not exist in the database and will be skipped: {missing_labels}")
        
        if not valid_node_labels:
            raise ValueError("No valid node labels found in the database. Cannot create graph projection.")
        
        logger.info(f"Using {len(valid_node_labels)} node labels for projection: {valid_node_labels}")
        
        # Filter relationship types to only include those that exist
        valid_rel_types = [rel for rel in relationship_types if rel in existing_rel_types]
        missing_rel_types = [rel for rel in relationship_types if rel not in existing_rel_types]
        
        if missing_rel_types:
            logger.warning(f"The following relationship types do not exist in the database and will be skipped: {missing_rel_types}")
        
        if not valid_rel_types:
            logger.warning("No valid relationship types found. Creating projection with nodes only.")
            rel_projection = {}
        else:
            logger.info(f"Using {len(valid_rel_types)} relationship types for projection: {valid_rel_types}")
            # Build relationship projection as a dictionary
            rel_projection = {rel: {"type": rel} for rel in valid_rel_types}
        
        # Use parameterized query with proper Cypher syntax
        query = """
        CALL gds.graph.project(
            $graphName,
            $nodeLabels,
            $relProjection
        )
        YIELD graphName, nodeCount, relationshipCount
        RETURN graphName, nodeCount, relationshipCount
        """
        
        result = self.connection.execute_query(query, {
            'graphName': self.graph_name,
            'nodeLabels': valid_node_labels,
            'relProjection': rel_projection
        })
        
        if result:
            logger.info(f"Created GDS projection: {result[0]}")
    
    def generate_fastrp_embeddings(
        self,
        embedding_dimension: int = 128,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: float = 0.0,
        property_name: str = "structuralEmbedding"
    ) -> int:
        """
        Generate FastRP embeddings using Neo4j GDS
        
        Args:
            embedding_dimension: Dimension of embeddings
            iteration_weights: Weights for each iteration
            normalization_strength: Normalization strength
            property_name: Property name to store embeddings
            
        Returns:
            Number of nodes with embeddings written
        """
        if iteration_weights is None:
            iteration_weights = [0.0, 1.0]
        
        query = """
        CALL gds.fastRP.write(
            $graphName,
            {
                embeddingDimension: $embeddingDimension,
                iterationWeights: $iterationWeights,
                normalizationStrength: $normalizationStrength,
                writeProperty: $propertyName
            }
        )
        YIELD nodePropertiesWritten
        RETURN nodePropertiesWritten
        """
        
        result = self.connection.execute_query(query, {
            'graphName': self.graph_name,
            'embeddingDimension': embedding_dimension,
            'iterationWeights': iteration_weights,
            'normalizationStrength': normalization_strength,
            'propertyName': property_name
        })
        
        if result:
            nodes_written = result[0]['nodePropertiesWritten']
            logger.info(f"Generated FastRP embeddings for {nodes_written} nodes")
            return nodes_written
        
        return 0
    
    def get_patient_embeddings(self, property_name: str = "structuralEmbedding") -> Dict[str, np.ndarray]:
        """Retrieve structural embeddings for all patients"""
        query = f"""
        MATCH (p:Patient)
        WHERE p.{property_name} IS NOT NULL
        RETURN p.subject_id AS subject_id, p.{property_name} AS embedding
        """
        
        results = self.connection.execute_query(query)
        
        embeddings = {}
        for record in results:
            subject_id = record.get('subject_id')
            embedding = record.get('embedding')
            
            if subject_id and embedding:
                embeddings[str(subject_id)] = np.array(embedding)
        
        logger.info(f"Retrieved {len(embeddings)} patient structural embeddings")
        return embeddings
    
    def get_node_embeddings(
        self,
        node_label: str,
        property_name: str = "structuralEmbedding"
    ) -> Dict[str, np.ndarray]:
        """Retrieve embeddings for nodes of a specific label"""
        query = f"""
        MATCH (n:{node_label})
        WHERE n.{property_name} IS NOT NULL
        RETURN elementId(n) AS element_id, n.{property_name} AS embedding
        """
        
        results = self.connection.execute_query(query)
        
        embeddings = {}
        for record in results:
            element_id = record.get('element_id')
            embedding = record.get('embedding')
            
            if element_id and embedding:
                embeddings[element_id] = np.array(embedding)
        
        logger.info(f"Retrieved {len(embeddings)} {node_label} structural embeddings")
        return embeddings
    
    def drop_projection(self):
        """Drop the GDS projection"""
        try:
            query = "CALL gds.graph.drop($graphName) YIELD graphName"
            self.connection.execute_query(query, {'graphName': self.graph_name})
            logger.info(f"Dropped GDS projection '{self.graph_name}'")
        except Exception as e:
            logger.warning(f"Could not drop projection: {e}")

