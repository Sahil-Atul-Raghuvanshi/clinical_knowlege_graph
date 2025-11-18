"""
Neo4j connection handler and utility functions
"""
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)
logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)

class Neo4jConnection:
    """Handles Neo4j database connections and queries"""
    
    def __init__(self, uri: str, username: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.driver = None
        
    def connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.username, self.password)
            )
            with self.driver.session(database=self.database) as session:
                session.run("RETURN 1").single()
            logger.info(f"Connected to Neo4j at {self.uri} (database: {self.database})")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    @contextmanager
    def session(self):
        """Context manager for Neo4j sessions"""
        if not self.driver:
            self.connect()
        session = self.driver.session(database=self.database)
        try:
            yield session
        finally:
            session.close()
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """Execute a Cypher query and return results"""
        if parameters is None:
            parameters = {}
        with self.session() as session:
            result = session.run(query, parameters)
            return [dict(record) for record in result]
    
    def execute_write(self, query: str, parameters: Dict[str, Any] = None) -> Any:
        """Execute a write query"""
        if parameters is None:
            parameters = {}
        with self.session() as session:
            result = session.run(query, parameters)
            return result.single()
    
    def get_all_patient_ids(self) -> List[str]:
        """Get all patient IDs from the database"""
        query = """
        MATCH (p:Patient)
        RETURN p.subject_id AS subject_id
        ORDER BY p.subject_id
        """
        results = self.execute_query(query)
        return [str(r['subject_id']) for r in results if r.get('subject_id')]
    
    def check_gds_availability(self) -> bool:
        """Check if Neo4j Graph Data Science (GDS) plugin is available"""
        try:
            query = "RETURN gds.version() AS version"
            result = self.execute_query(query)
            version = result[0]['version'] if result else None
            if version:
                logger.info(f"Neo4j GDS version {version} is available")
                return True
        except Exception as e:
            logger.warning(f"Neo4j GDS not available: {e}")
        return False
    
    def get_existing_node_labels(self) -> List[str]:
        """Get all node labels that exist in the database"""
        try:
            query = "CALL db.labels() YIELD label RETURN collect(label) AS labels"
            result = self.execute_query(query)
            labels = result[0]['labels'] if result else []
            return labels
        except Exception as e:
            logger.warning(f"Failed to get node labels: {e}")
            return []
    
    def get_existing_relationship_types(self) -> List[str]:
        """Get all relationship types that exist in the database"""
        try:
            query = "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) AS types"
            result = self.execute_query(query)
            types = result[0]['types'] if result else []
            return types
        except Exception as e:
            logger.warning(f"Failed to get relationship types: {e}")
            return []

