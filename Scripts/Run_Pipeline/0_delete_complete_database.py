# delete_complete_database.py
import logging
import sys
from pathlib import Path

# Add Scripts directory to path for imports
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
sys.path.insert(0, str(scripts_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection

# Configure logging with UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
# Reconfigure stream to use UTF-8 if possible
for handler in logging.root.handlers:
    if isinstance(handler, logging.StreamHandler) and hasattr(handler.stream, 'reconfigure'):
        handler.stream.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

def delete_complete_database():
    """Delete all nodes and relationships from the Neo4j database"""
    
    # Load configuration
    config = Config()
    
    # Connect to Neo4j using centralized config
    neo4j_conn = Neo4jConnection(
        uri=config.neo4j.uri,
        username=config.neo4j.username,
        password=config.neo4j.password,
        database=config.neo4j.database
    )
    neo4j_conn.connect()
    
    try:
        with neo4j_conn.session() as session:
            # Get initial count of nodes and relationships
            count_query = """
            MATCH (n)
            RETURN count(n) as node_count
            """
            result = session.run(count_query)
            initial_count = result.single()['node_count']
            
            logger.info(f"Found {initial_count} nodes in the database")
            
            if initial_count == 0:
                logger.info("Database is already empty!")
                return
            
            logger.warning("=" * 60)
            logger.warning("Deleting ALL data from the database...")
            logger.warning(f"Database: {config.neo4j.database}")
            logger.warning(f"Total nodes to delete: {initial_count}")
            logger.warning("=" * 60)
            
            # Delete all nodes and relationships in batches
            batch_size = 10000
            deleted_total = 0
            
            while True:
                delete_query = f"""
                MATCH (n)
                WITH n LIMIT {batch_size}
                DETACH DELETE n
                RETURN count(n) as deleted
                """
                
                result = session.run(delete_query)
                record = result.single()
                deleted = record['deleted'] if record else 0
                
                if deleted == 0:
                    break
                
                deleted_total += deleted
                logger.info(f"Deleted {deleted} nodes (Total: {deleted_total}/{initial_count})")
            
            # Verify deletion
            result = session.run(count_query)
            final_count = result.single()['node_count']
            
            if final_count == 0:
                logger.info("=" * 60)
                logger.info("✓ Successfully deleted all data from the database!")
                logger.info(f"Total nodes deleted: {deleted_total}")
                logger.info("=" * 60)
            else:
                logger.warning(f"Warning: {final_count} nodes still remain in the database")
                
    except Exception as e:
        logger.error(f"An error occurred during deletion: {e}")
        raise
    
    finally:
        neo4j_conn.close()


if __name__ == "__main__":
    delete_complete_database()

