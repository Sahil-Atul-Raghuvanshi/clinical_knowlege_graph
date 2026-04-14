import sys
import os
from pathlib import Path
import logging

# Add Scripts directory to path for imports
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent.parent
sys.path.insert(0, str(scripts_dir))

from utils.config import Config
from utils.neo4j_connection import Neo4jConnection

# Configure logging - write only to file, not console
project_root = script_dir.parent.parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(exist_ok=True)

# Configure logger to only use file handler (no console output)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if running via pipeline (pipeline mode) - check environment variable for script 14
# since it runs at module import time
full_load_log_file = os.environ.get('FULL_LOAD_LOG_FILE')
if full_load_log_file:
    # Pipeline mode: append to the pipeline log file
    file_handler = logging.FileHandler(full_load_log_file, encoding='utf-8', mode='a')
else:
    # Standalone mode: create temp_ prefixed log file
    log_file = logs_dir / 'temp_delete_unwanted_connections.log'
    file_handler = logging.FileHandler(log_file, encoding='utf-8')

file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Prevent propagation to root logger (which would print to console)
logger.propagate = False

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

with neo4j_conn.session() as session:
    logger.info("Removing cross-connections between Prescription/Procedure/LabEvents hierarchies...")
    
    # Delete connections FROM PrescriptionsBatch/Prescription TO Procedures
    query1 = """
    MATCH (p)-[r:HAS_PROCEDURES]->(proc)
    WHERE (p:PrescriptionsBatch OR p:Prescription)
      AND (proc:Procedures OR proc:ProceduresBatch)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result1 = session.run(query1)
    count1 = result1.single()["deleted_count"]
    logger.info(f"Deleted {count1} HAS_PROCEDURES from Prescription hierarchy to Procedures")
    
    # Delete connections FROM PrescriptionsBatch/Prescription TO LabEvents
    query2 = """
    MATCH (p)-[r:HAS_LAB_EVENTS]->(lab)
    WHERE (p:PrescriptionsBatch OR p:Prescription)
      AND (lab:LabEvents OR lab:LabEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result2 = session.run(query2)
    count2 = result2.single()["deleted_count"]
    logger.info(f"Deleted {count2} HAS_LAB_EVENTS from Prescription hierarchy to LabEvents")
    
    # Delete connections FROM Procedures TO PrescriptionsBatch/Prescription
    query3 = """
    MATCH (proc)-[r:HAS_PRESCRIPTIONS]->(p)
    WHERE (proc:Procedures OR proc:ProceduresBatch)
      AND (p:PrescriptionsBatch OR p:Prescription)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result3 = session.run(query3)
    count3 = result3.single()["deleted_count"]
    logger.info(f"Deleted {count3} HAS_PRESCRIPTIONS from Procedures to Prescription hierarchy")
    
    # Delete connections FROM LabEvents TO PrescriptionsBatch/Prescription
    query4 = """
    MATCH (lab)-[r:HAS_PRESCRIPTIONS]->(p)
    WHERE (lab:LabEvents OR lab:LabEvent)
      AND (p:PrescriptionsBatch OR p:Prescription)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result4 = session.run(query4)
    count4 = result4.single()["deleted_count"]
    logger.info(f"Deleted {count4} HAS_PRESCRIPTIONS from LabEvents to Prescription hierarchy")
    
    # Delete connections FROM Procedures TO LabEvents
    query5 = """
    MATCH (proc)-[r:HAS_LAB_EVENTS]->(lab)
    WHERE (proc:Procedures OR proc:ProceduresBatch)
      AND (lab:LabEvents OR lab:LabEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result5 = session.run(query5)
    count5 = result5.single()["deleted_count"]
    logger.info(f"Deleted {count5} HAS_LAB_EVENTS from Procedures to LabEvents")
    
    # Delete connections FROM LabEvents TO Procedures
    query6 = """
    MATCH (lab)-[r:HAS_PROCEDURES]->(proc)
    WHERE (lab:LabEvents OR lab:LabEvent)
      AND (proc:Procedures OR proc:ProceduresBatch)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result6 = session.run(query6)
    count6 = result6.single()["deleted_count"]
    logger.info(f"Deleted {count6} HAS_PROCEDURES from LabEvents to Procedures")
    
    # Delete ANY connections FROM Procedures TO LabEvents (bidirectional)
    query7 = """
    MATCH (p:Procedures)-[r]-(lab)
    WHERE (lab:LabEvents OR lab:LabEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result7 = session.run(query7)
    count7 = result7.single()["deleted_count"]
    logger.info(f"Deleted {count7} connections between Procedures and LabEvents")
    
    # Delete ANY connections FROM Procedures TO Prescriptions (bidirectional)
    query8 = """
    MATCH (proc:Procedures)-[r]-(presc)
    WHERE (presc:Prescription OR presc:PrescriptionsBatch)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result8 = session.run(query8)
    count8 = result8.single()["deleted_count"]
    logger.info(f"Deleted {count8} connections between Procedures and Prescriptions")
    
    # Delete ANY connections between LabEvent and Prescription (bidirectional)
    query9 = """
    MATCH (lab:LabEvent)-[r]-(presc:Prescription)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result9 = session.run(query9)
    count9 = result9.single()["deleted_count"]
    logger.info(f"Deleted {count9} connections between LabEvent and Prescription nodes")
    
    # Delete connections FROM Prescriptions TO MicrobiologyEvents
    query10 = """
    MATCH (presc)-[r:HAS_MICROBIOLOGY_EVENTS]->(micro)
    WHERE (presc:Prescription OR presc:PrescriptionsBatch)
      AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result10 = session.run(query10)
    count10 = result10.single()["deleted_count"]
    logger.info(f"Deleted {count10} HAS_MICROBIOLOGY_EVENTS from Prescriptions to MicrobiologyEvents")
    
    # Delete connections FROM Procedures TO MicrobiologyEvents
    query11 = """
    MATCH (proc)-[r:HAS_MICROBIOLOGY_EVENTS]->(micro)
    WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
      AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result11 = session.run(query11)
    count11 = result11.single()["deleted_count"]
    logger.info(f"Deleted {count11} HAS_MICROBIOLOGY_EVENTS from Procedures to MicrobiologyEvents")
    
    # Delete ANY connections between Prescriptions and MicrobiologyEvents (bidirectional)
    query12 = """
    MATCH (presc)-[r]-(micro)
    WHERE (presc:Prescription OR presc:PrescriptionsBatch)
      AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result12 = session.run(query12)
    count12 = result12.single()["deleted_count"]
    logger.info(f"Deleted {count12} connections between Prescriptions and MicrobiologyEvents")
    
    # Delete ANY connections between Procedures and MicrobiologyEvents (bidirectional)
    query13 = """
    MATCH (proc)-[r]-(micro)
    WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
      AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result13 = session.run(query13)
    count13 = result13.single()["deleted_count"]
    logger.info(f"Deleted {count13} connections between Procedures and MicrobiologyEvents")
    
    # Delete ANY connections between LabEvents and MicrobiologyEvents (bidirectional)
    query14 = """
    MATCH (lab)-[r]-(micro)
    WHERE (lab:LabEvents OR lab:LabEvent)
      AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result14 = session.run(query14)
    count14 = result14.single()["deleted_count"]
    logger.info(f"Deleted {count14} connections between LabEvents and MicrobiologyEvents")
    
    # Delete HAS_CHART_EVENTS from non-ICUStay nodes to ChartEventBatch
    query15 = """
    MATCH (n)-[r:HAS_CHART_EVENTS]->(ceb:ChartEventBatch)
    WHERE NOT n:ICUStay
    DELETE r
    RETURN count(r) as deleted_count
    """
    result15 = session.run(query15)
    count15 = result15.single()["deleted_count"]
    logger.info(f"Deleted {count15} HAS_CHART_EVENTS from non-ICUStay nodes")
    
    # Delete ANY other relationships to/from ChartEventBatch except HAS_CHART_EVENTS from ICUStay and HAS_CHART_EVENT to ChartEvent
    query16 = """
    MATCH (n)-[r]-(ceb:ChartEventBatch)
    WHERE NOT (
        (n:ICUStay AND type(r) = 'HAS_CHART_EVENTS') OR
        (n:ChartEvent AND type(r) = 'HAS_CHART_EVENT')
    )
    DELETE r
    RETURN count(r) as deleted_count
    """
    result16 = session.run(query16)
    count16 = result16.single()["deleted_count"]
    logger.info(f"Deleted {count16} unwanted relationships to/from ChartEventBatch")
    
    total = count1 + count2 + count3 + count4 + count5 + count6 + count7 + count8 + count9 + count10 + count11 + count12 + count13 + count14 + count15 + count16
    logger.info(f"\nTotal cross-connections deleted: {total}")

neo4j_conn.close()
