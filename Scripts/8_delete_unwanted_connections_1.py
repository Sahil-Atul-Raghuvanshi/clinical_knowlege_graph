from neo4j import GraphDatabase
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "10016742"

driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)

with driver.session() as session:
    logger.info("Removing cross-connections between Prescription/Procedure/LabEvents hierarchies...")
    
    # Delete connections FROM PrescriptionsBatch/Prescription TO Procedures
    query1 = """
    MATCH (p)-[r:HAS_PROCEDURES]->(proc)
    WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
      AND (proc:Procedure OR proc:ProceduresBatch)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result1 = session.run(query1)
    count1 = result1.single()["deleted_count"]
    logger.info(f"Deleted {count1} HAS_PROCEDURES from Prescription hierarchy to Procedures")
    
    # Delete connections FROM PrescriptionsBatch/Prescription TO LabEvents
    query2 = """
    MATCH (p)-[r:HAS_LAB_EVENTS]->(lab)
    WHERE (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
      AND (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result2 = session.run(query2)
    count2 = result2.single()["deleted_count"]
    logger.info(f"Deleted {count2} HAS_LAB_EVENTS from Prescription hierarchy to LabEvents")
    
    # Delete connections FROM Procedures TO PrescriptionsBatch/Prescription
    query3 = """
    MATCH (proc)-[r:HAS_PRESCRIPTIONS]->(p)
    WHERE (proc:Procedure OR proc:ProceduresBatch)
      AND (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result3 = session.run(query3)
    count3 = result3.single()["deleted_count"]
    logger.info(f"Deleted {count3} HAS_PRESCRIPTIONS from Procedures to Prescription hierarchy")
    
    # Delete connections FROM LabEvents TO PrescriptionsBatch/Prescription
    query4 = """
    MATCH (lab)-[r:HAS_PRESCRIPTIONS]->(p)
    WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
      AND (p:PrescriptionBatch OR p:PrescriptionsBatch OR p:Prescription OR p:Medicine)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result4 = session.run(query4)
    count4 = result4.single()["deleted_count"]
    logger.info(f"Deleted {count4} HAS_PRESCRIPTIONS from LabEvents to Prescription hierarchy")
    
    # Delete connections FROM Procedures TO LabEvents
    query5 = """
    MATCH (proc)-[r:HAS_LAB_EVENTS]->(lab)
    WHERE (proc:Procedure OR proc:ProceduresBatch)
      AND (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result5 = session.run(query5)
    count5 = result5.single()["deleted_count"]
    logger.info(f"Deleted {count5} HAS_LAB_EVENTS from Procedures to LabEvents")
    
    # Delete connections FROM LabEvents TO Procedures
    query6 = """
    MATCH (lab)-[r:HAS_PROCEDURES]->(proc)
    WHERE (lab:LabEvents OR lab:LabEventsBatch OR lab:Collection OR lab:Specimen OR lab:LabEvent)
      AND (proc:Procedure OR proc:ProceduresBatch)
    DELETE r
    RETURN count(r) as deleted_count
    """
    result6 = session.run(query6)
    count6 = result6.single()["deleted_count"]
    logger.info(f"Deleted {count6} HAS_PROCEDURES from LabEvents to Procedures")
    
    total = count1 + count2 + count3 + count4 + count5 + count6
    logger.info(f"\nTotal cross-connections deleted: {total}")

driver.close()
