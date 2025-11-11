# delete_complete_database.py
from neo4j import GraphDatabase
import logging
import sys

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
    
    # Neo4j connection settings
    URI = "neo4j://127.0.0.1:7687"
    AUTH = ("neo4j", "admin123")
    DATABASE = "clinicalknowledgegraph"
    
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        with driver.session() as session:
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
            logger.warning(f"Database: {DATABASE}")
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
        driver.close()


if __name__ == "__main__":
    delete_complete_database()

