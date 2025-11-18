"""
Utility functions for incremental loading of knowledge graphs.
Checks if patient data already exists before processing.
"""
import logging
import os
from typing import Set, Optional, List, Dict, Any
from neo4j import GraphDatabase
from utils.etl_tracker import ETLTracker

logger = logging.getLogger(__name__)


class IncrementalLoadChecker:
    """Helper class to check if patient data already exists in Neo4j"""
    
    def __init__(self, driver, tracker: Optional[ETLTracker] = None, database: str = "neo4j"):
        """
        Initialize with Neo4j driver and optional ETL tracker
        
        Args:
            driver: Neo4j driver instance
            tracker: Optional ETL tracker instance for tracking processed patients
            database: Database name to use (default: "neo4j")
        """
        self.driver = driver
        self.tracker = tracker
        self.database = database
    
    def get_existing_patients(self) -> Set[int]:
        """
        Get set of subject_ids that already have Patient nodes.
        If tracker is available, also checks tracker for processed patients.
        
        Returns:
            Set of subject_id integers
        """
        existing = set()
        
        # Check Neo4j database
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (p:Patient)
            RETURN DISTINCT p.subject_id AS subject_id
            """
            result = session.run(query)
            existing.update(record["subject_id"] for record in result if record["subject_id"] is not None)
        
        # Also check tracker if available
        if self.tracker:
            tracker_patients = self.tracker.get_processed_patients('1_add_patient_nodes')
            existing.update(tracker_patients)
        
        logger.info(f"Found {len(existing)} existing patients (from database and tracker)")
        return existing
    
    def sync_tracker_for_existing_patients(self, script_name: str, patient_ids: Set[int]) -> int:
        """
        Sync tracker: Mark patients as processed if they exist in Neo4j but tracker is missing entries.
        This ensures tracker stays in sync with Neo4j even if tracker file was deleted or missing.
        
        Args:
            script_name: Name of the script (e.g., '1_add_patient_nodes')
            patient_ids: Set of patient IDs that exist in Neo4j
            
        Returns:
            Number of patients synced in tracker
        """
        if not self.tracker or not patient_ids:
            return 0
        
        # Find patients that exist in Neo4j but are missing from tracker
        patients_to_sync = []
        for patient_id in patient_ids:
            if not self.tracker.is_patient_processed(patient_id, script_name):
                patients_to_sync.append(patient_id)
        
        if patients_to_sync:
            logger.info(f"Syncing tracker: Marking {len(patients_to_sync)} patients as processed for '{script_name}' (exist in Neo4j but missing from tracker)")
            self.tracker.mark_patients_processed_batch(
                patients_to_sync,
                script_name,
                status='success'
            )
            logger.info(f"✓ Synced tracker: Marked {len(patients_to_sync)} patients as processed for '{script_name}'")
            return len(patients_to_sync)
        
        return 0
    
    def patient_has_complete_graph(self, subject_id: int) -> bool:
        """
        Check if a patient has a complete knowledge graph.
        A complete graph means the patient has:
        - Patient node
        - At least one event (ED, Admission, Discharge, etc.)
        - Prescriptions (if available)
        - Lab events (if available)
        
        Args:
            subject_id: Patient subject_id
            
        Returns:
            True if patient has complete graph, False otherwise
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (p:Patient {subject_id: $subject_id})
            OPTIONAL MATCH (p)-[*1..3]-(e)
            WHERE e:EmergencyDepartment OR e:UnitAdmission OR e:Discharge 
               OR e:HospitalAdmission OR e:ICUStay
            WITH p, count(DISTINCT e) as event_count
            OPTIONAL MATCH (p)-[*1..5]-(presc)
            WHERE presc:PrescriptionsBatch OR presc:Prescription
            WITH p, event_count, count(DISTINCT presc) as presc_count
            OPTIONAL MATCH (p)-[*1..5]-(lab)
            WHERE lab:LabEvents OR lab:LabEvent
            WITH p, event_count, presc_count, count(DISTINCT lab) as lab_count
            RETURN 
                event_count > 0 as has_events,
                presc_count > 0 as has_prescriptions,
                lab_count > 0 as has_labs
            """
            result = session.run(query, subject_id=subject_id)
            record = result.single()
            
            if not record:
                return False
            
            # Patient has complete graph if they have events
            # Prescriptions and labs are optional (may not exist for all patients)
            return record["has_events"]
    
    def get_patients_with_prescriptions(self, subject_ids: Optional[List[int]] = None) -> Set[int]:
        """
        Get set of subject_ids that already have prescription nodes
        
        Args:
            subject_ids: Optional list to filter by. If None, checks all patients.
            
        Returns:
            Set of subject_id integers that have prescriptions
        """
        with self.driver.session(database=self.database) as session:
            if subject_ids:
                query = """
                MATCH (p:Patient)
                WHERE p.subject_id IN $subject_ids
                MATCH (p)-[*1..5]-(presc)
                WHERE presc:PrescriptionsBatch OR presc:Prescription
                RETURN DISTINCT p.subject_id AS subject_id
                """
                result = session.run(query, subject_ids=subject_ids)
            else:
                query = """
                MATCH (p:Patient)-[*1..5]-(presc)
                WHERE presc:PrescriptionsBatch OR presc:Prescription
                RETURN DISTINCT p.subject_id AS subject_id
                """
                result = session.run(query)
            
            existing = {record["subject_id"] for record in result if record["subject_id"] is not None}
            logger.info(f"Found {len(existing)} patients with existing prescriptions")
            return existing
    
    def get_patients_with_lab_events(self, subject_ids: Optional[List[int]] = None) -> Set[int]:
        """
        Get set of subject_ids that already have lab event nodes
        
        Args:
            subject_ids: Optional list to filter by. If None, checks all patients.
            
        Returns:
            Set of subject_id integers that have lab events
        """
        with self.driver.session(database=self.database) as session:
            if subject_ids:
                query = """
                MATCH (p:Patient)
                WHERE p.subject_id IN $subject_ids
                MATCH (p)-[*1..5]-(lab)
                WHERE lab:LabEvents OR lab:LabEvent
                RETURN DISTINCT p.subject_id AS subject_id
                """
                result = session.run(query, subject_ids=subject_ids)
            else:
                query = """
                MATCH (p:Patient)-[*1..5]-(lab)
                WHERE lab:LabEvents OR lab:LabEvent
                RETURN DISTINCT p.subject_id AS subject_id
                """
                result = session.run(query)
            
            existing = {record["subject_id"] for record in result if record["subject_id"] is not None}
            logger.info(f"Found {len(existing)} patients with existing lab events")
            return existing
    
    def event_has_prescriptions(self, event_id: str) -> bool:
        """
        Check if an event already has prescription nodes
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if event has prescriptions, False otherwise
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (e {event_id: $event_id})-[:ISSUED_PRESCRIPTIONS]->(pb:PrescriptionsBatch)
            RETURN count(pb) > 0 as has_prescriptions
            """
            result = session.run(query, event_id=event_id)
            record = result.single()
            return record["has_prescriptions"] if record else False
    
    def event_has_lab_events(self, event_id: str) -> bool:
        """
        Check if an event already has lab event nodes
        
        Args:
            event_id: Event ID to check
            
        Returns:
            True if event has lab events, False otherwise
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (e {event_id: $event_id})-[:INCLUDED_LAB_EVENTS]->(le:LabEvents)
            RETURN count(le) > 0 as has_labs
            """
            result = session.run(query, event_id=event_id)
            record = result.single()
            return record["has_labs"] if record else False
    
    def get_events_with_prescriptions(self) -> Set[str]:
        """
        Get set of event_ids that already have prescription nodes
        
        Returns:
            Set of event_id strings
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (e)-[:ISSUED_PRESCRIPTIONS]->(pb:PrescriptionsBatch)
            RETURN DISTINCT e.event_id AS event_id
            """
            result = session.run(query)
            existing = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(existing)} events with existing prescriptions")
            return existing
    
    def get_events_with_lab_events(self) -> Set[str]:
        """
        Get set of event_ids that already have lab event nodes
        
        Returns:
            Set of event_id strings
        """
        with self.driver.session(database=self.database) as session:
            query = """
            MATCH (e)-[:INCLUDED_LAB_EVENTS]->(le:LabEvents)
            RETURN DISTINCT e.event_id AS event_id
            """
            result = session.run(query)
            existing = {str(record["event_id"]) for record in result if record["event_id"] is not None}
            logger.info(f"Found {len(existing)} events with existing lab events")
            return existing

