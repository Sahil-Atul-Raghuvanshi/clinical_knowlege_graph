"""
Neo4j connection singleton for FastAPI backend.
Imports from the shared utils in Scripts/utils.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_connection = None


def get_connection():
    """Return a cached Neo4j connection, creating one on first call."""
    global _connection
    if _connection is None:
        from utils.config import Config
        from utils.neo4j_connection import Neo4jConnection

        config = Config()
        conn = Neo4jConnection(
            uri=config.neo4j.uri,
            username=config.neo4j.username,
            password=config.neo4j.password,
            database=config.neo4j.database,
        )
        conn.connect()
        _connection = conn
        logger.info("Neo4j connection established")
    return _connection


def execute_query(query: str, params: dict):
    """Execute a Cypher query and return results."""
    conn = get_connection()
    return conn.execute_query(query, params)
