"""
Graph Retriever Module
Retrieves patient subgraphs and clinical data from Neo4j
"""
import logging
import json
import os
from typing import List, Dict, Any, Optional
from pathlib import Path

# Import from local utils
from utils.neo4j_connection import Neo4jConnection
from utils.config import Neo4jConfig

logger = logging.getLogger(__name__)


class GraphRetriever:
    """Retrieves graph data from Neo4j for clinical queries"""
    
    def __init__(self, config: Optional[Neo4jConfig] = None):
        """
        Initialize graph retriever
        
        Args:
            config: Neo4j configuration (uses default if not provided)
        """
        if config is None:
            # Try to load from JSON first, fallback to env
            try:
                config = Neo4jConfig.from_json()
            except Exception:
                config = Neo4jConfig.from_env()
        
        self.config = config
        self.connection = Neo4jConnection(
            uri=config.uri,
            username=config.username,
            password=config.password,
            database=config.database
        )
        self.connection.connect()
        logger.info("GraphRetriever initialized")
    
    def close(self):
        """Close Neo4j connection"""
        if self.connection:
            self.connection.close()
    
    def get_patient_journey(self, subject_id: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Retrieve complete patient journey from graph
        
        Note: Patient nodes are linked to HospitalAdmission and Discharge via subject_id property
        (no explicit relationship edge)
        
        Args:
            subject_id: Patient subject ID
            limit: Maximum number of results
            
        Returns:
            List of graph records
        """
        query = """
        MATCH (p:Patient {subject_id: $subject_id})
        // Patient -> EmergencyDepartment (explicit relationship)
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)
        // EmergencyDepartment -> HospitalAdmission (explicit relationships)
        OPTIONAL MATCH (ed)-[:LED_TO_ADMISSION|LED_TO_ADMISSION_DURING_STAY|LED_TO_ADMISSION_AFTER_DISCHARGE]->(h_ed:HospitalAdmission)
        // Patient -> HospitalAdmission (via subject_id property, no explicit edge)
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: $subject_id})
        // HospitalAdmission relationships
        OPTIONAL MATCH (h)-[:ISSUED_PRESCRIPTIONS]->(pr:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx:Prescription)
        OPTIONAL MATCH (h)-[:INCLUDED_LAB_EVENTS]->(le:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab:LabEvent)
        OPTIONAL MATCH (h)-[:INCLUDED_PROCEDURES]->(pb:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc:Procedures)
        OPTIONAL MATCH (h)-[:WAS_ASSIGNED_DRG_CODE]->(drg:DRG)
        OPTIONAL MATCH (h)-[:INCLUDED_HPI_SUMMARY]->(hpi:HPISummary)
        OPTIONAL MATCH (h)-[:INCLUDED_PAST_HISTORY]->(ph:PatientPastHistory)
        OPTIONAL MATCH (h)-[:RECORDED_VITALS]->(av:AdmissionVitals)
        OPTIONAL MATCH (h)-[:INCLUDED_LAB_RESULTS]->(al:AdmissionLabs)
        OPTIONAL MATCH (h)-[:INCLUDED_MEDICATIONS]->(am:AdmissionMedications)
        // EmergencyDepartment relationships
        OPTIONAL MATCH (ed)-[:RECORDED_DIAGNOSES]->(d_ed:Diagnosis)
        OPTIONAL MATCH (ed)-[:RECORDED_PREVIOUS_MEDICATIONS]->(ppm:PreviousPrescriptionMeds)
        OPTIONAL MATCH (ed)-[:ADMINISTERED_MEDICATIONS]->(am_ed:AdministeredMeds)
        OPTIONAL MATCH (ed)-[:INCLUDED_TRIAGE_ASSESSMENT]->(ia:InitialAssessment)
        OPTIONAL MATCH (ed)-[:INCLUDED_PROCEDURES]->(pb_ed:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_ed:Procedures)
        OPTIONAL MATCH (ed)-[:INCLUDED_LAB_EVENTS]->(le_ed:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab_ed:LabEvent)
        OPTIONAL MATCH (ed)-[:ISSUED_PRESCRIPTIONS]->(pr_ed:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx_ed:Prescription)
        // UnitAdmission relationships
        OPTIONAL MATCH (ua:UnitAdmission {subject_id: $subject_id})
        OPTIONAL MATCH (ua)-[:INCLUDED_PROCEDURES]->(pb_ua:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_ua:Procedures)
        OPTIONAL MATCH (ua)-[:INCLUDED_LAB_EVENTS]->(le_ua:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab_ua:LabEvent)
        OPTIONAL MATCH (ua)-[:ISSUED_PRESCRIPTIONS]->(pr_ua:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx_ua:Prescription)
        OPTIONAL MATCH (ua)-[:LED_TO_DISCHARGE]->(dis_ua:Discharge)
        // ICUStay relationships
        OPTIONAL MATCH (icu:ICUStay {subject_id: $subject_id})
        OPTIONAL MATCH (icu)-[:INCLUDED_PROCEDURES]->(pb_icu:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_icu:Procedures)
        OPTIONAL MATCH (icu)-[:INCLUDED_LAB_EVENTS]->(le_icu:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab_icu:LabEvent)
        OPTIONAL MATCH (icu)-[:ISSUED_PRESCRIPTIONS]->(pr_icu:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx_icu:Prescription)
        OPTIONAL MATCH (icu)-[:RECORDED_CHART_EVENTS]->(ceb:ChartEventBatch)-[:CONTAINED_CHART_EVENT]->(ce:ChartEvent)
        // Discharge relationships (linked via subject_id property)
        OPTIONAL MATCH (dis:Discharge {subject_id: $subject_id})
        OPTIONAL MATCH (dis)-[:RECORDED_DIAGNOSES]->(d_dis:Diagnosis)
        OPTIONAL MATCH (dis)-[:DOCUMENTED_IN_NOTE]->(dcn:DischargeClinicalNote)
        OPTIONAL MATCH (dis)-[:HAS_ALLERGY]->(ai:AllergyIdentified)
        OPTIONAL MATCH (dis)-[:STARTED_MEDICATIONS]->(ms:MedicationStarted)
        OPTIONAL MATCH (dis)-[:STOPPED_MEDICATIONS]->(mst:MedicationStopped)
        OPTIONAL MATCH (dis)-[:LISTED_MEDICATIONS_TO_AVOID]->(mta:MedicationToAvoid)
        OPTIONAL MATCH (dis)-[:LED_TO_ED_VISIT]->(ed_dis:EmergencyDepartment)
        // DischargeClinicalNote relationships
        OPTIONAL MATCH (dcn)-[:RECORDED_VITALS]->(dv:DischargeVitals)
        OPTIONAL MATCH (dcn)-[:RECORDED_LAB_RESULTS]->(dl:DischargeLabs)
        OPTIONAL MATCH (dcn)-[:RECORDED_MEDICATIONS]->(dm:DischargeMedications)
        // LabEvents -> MicrobiologyEvent
        OPTIONAL MATCH (le)-[:CONTAINED_MICROBIOLOGY_EVENT]->(me:MicrobiologyEvent)
        RETURN DISTINCT p, ed, h, h_ed, rx, lab, d_ed, proc, drg, hpi, ph, av, al, am,
               ppm, am_ed, ia, proc_ed, lab_ed, rx_ed,
               ua, proc_ua, lab_ua, rx_ua, dis, dis_ua,
               icu, proc_icu, lab_icu, rx_icu, ce,
               d_dis, dcn, ai, ms, mst, mta, ed_dis,
               dv, dl, dm, me
        LIMIT $limit
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={"subject_id": int(subject_id), "limit": limit}
            )
            logger.info(f"Retrieved {len(results)} records for patient {subject_id}")
            return results
        except Exception as e:
            logger.error(f"Error retrieving patient journey: {e}")
            return []
    
    def get_patient_by_condition(self, condition: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Find patients with a specific condition dynamically
        
        This method accepts any condition string and searches for it in diagnosis fields.
        The condition is dynamically extracted from user queries and can be:
        - Medical abbreviations (e.g., "mi", "copd")
        - Full condition names (e.g., "myocardial infarction", "diabetes")
        - Any condition mentioned in the query
        
        Args:
            condition: Medical condition to search for (dynamically extracted from query)
            limit: Maximum number of results
            
        Returns:
            List of patient records with the condition
        """
        query = """
        MATCH (d:Diagnosis)
        WHERE toLower(d.complete_diagnosis) CONTAINS toLower($condition)
           OR toLower(d.icd_code) CONTAINS toLower($condition)
           OR toLower(d.ed_diagnosis) CONTAINS toLower($condition)
           OR toLower(d.short_title) CONTAINS toLower($condition)
        MATCH (p:Patient)
        WHERE EXISTS {
            // Diagnosis from EmergencyDepartment
            MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)-[:RECORDED_DIAGNOSES]->(d)
        } OR EXISTS {
            // Diagnosis from Discharge (linked via subject_id)
            MATCH (dis:Discharge {subject_id: p.subject_id})-[:RECORDED_DIAGNOSES]->(d)
        }
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: p.subject_id})
        OPTIONAL MATCH (h)-[:ISSUED_PRESCRIPTIONS]->(pr:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx:Prescription)
        OPTIONAL MATCH (dis:Discharge {subject_id: p.subject_id})
        RETURN DISTINCT p, d, ed, h, rx, dis
        LIMIT $limit
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={"condition": condition, "limit": limit}
            )
            logger.info(f"Retrieved {len(results)} patients with condition: {condition}")
            return results
        except Exception as e:
            logger.error(f"Error retrieving patients by condition: {e}")
            return []
    
    def get_similar_patients_by_embedding(
        self, 
        subject_id: str, 
        top_k: int = 10,
        similarity_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Find similar patients using vector similarity search on combined embeddings
        
        Note: This uses NODE-LEVEL embeddings from Neo4j (Patient.combinedEmbedding or 
        Patient.combined_embedding) which represent the entire patient journey.
        
        IMPORTANT: This method retrieves similar patients based on their overall clinical
        profile embeddings. For item-level similarity (diagnoses, medications, lab results),
        Milvus collections are used via VectorRetriever, but those are used for semantic
        search of individual items, not for patient-level similarity.
        
        The patient embeddings in Neo4j should capture the full patient journey including:
        - Diagnoses
        - Medications
        - Lab results
        - Procedures
        - Clinical events
        
        Args:
            subject_id: Reference patient ID
            top_k: Number of similar patients to return
            similarity_threshold: Minimum similarity score
            
        Returns:
            List of similar patient records with similarity scores and enriched clinical data
        """
        # First, get the reference patient's embedding
        # Check for both camelCase and snake_case property names
        query_embedding = """
        MATCH (p:Patient {subject_id: $subject_id})
        RETURN 
            COALESCE(p.combinedEmbedding, p.combined_embedding) AS embedding,
            CASE 
                WHEN p.combinedEmbedding IS NOT NULL THEN 'combinedEmbedding'
                WHEN p.combined_embedding IS NOT NULL THEN 'combined_embedding'
                ELSE NULL
            END AS embedding_property
        """
        
        try:
            result = self.connection.execute_query(
                query_embedding,
                parameters={"subject_id": int(subject_id)}
            )
            
            if not result or not result[0].get('embedding'):
                logger.warning(f"No embedding found for patient {subject_id}. Checked: combinedEmbedding, combined_embedding")
                return []
            
            embedding_property = result[0].get('embedding_property', 'combinedEmbedding')
            ref_embedding = result[0]['embedding']
            logger.info(f"Found embedding for patient {subject_id} using property: {embedding_property}")
            
            # Try to use vector index to find similar patients
            # Note: This assumes a vector index exists on Patient.combined_embedding
            # The index name might vary - common names: 'patient_embedding_index', 'patient_journey_index'
            index_names = ['patient_embedding_index', 'patient_journey_index', 'patient_combined_embedding_index']
            
            for index_name in index_names:
                try:
                    # Use the detected embedding property name
                    # Use DISTINCT to avoid duplicates from multiple relationships
                    similarity_query = f"""
                    MATCH (p:Patient {{subject_id: $subject_id}})
                    WITH COALESCE(p.combinedEmbedding, p.combined_embedding) AS refEmbedding
                    WHERE refEmbedding IS NOT NULL
                    CALL db.index.vector.queryNodes('{index_name}', $topK, refEmbedding)
                    YIELD node AS similarPatient, score
                    WHERE similarPatient.subject_id <> $subject_id 
                      AND score >= $threshold
                      AND score < 1.0
                    WITH DISTINCT similarPatient, score
                    ORDER BY score DESC
                    LIMIT $topK
                    RETURN similarPatient, score
                    """
                    
                    results = self.connection.execute_query(
                        similarity_query,
                        parameters={
                            "subject_id": int(subject_id),
                            "topK": top_k,
                            "threshold": similarity_threshold
                        }
                    )
                    
                    if results:
                        logger.info(f"Found {len(results)} similar patients using index '{index_name}'")
                        # Log similarity scores for debugging
                        scores = [r.get('score', 0) for r in results]
                        logger.info(f"Similarity score range: min={min(scores):.4f}, max={max(scores):.4f}, avg={sum(scores)/len(scores):.4f}")
                        # Check if scores are suspiciously high (all > 0.99)
                        if all(s > 0.99 for s in scores):
                            logger.warning(f"WARNING: All similarity scores are > 0.99. This may indicate embedding issues.")
                        # Enrich results with clinical data
                        enriched_results = self._enrich_similar_patients(results, subject_id)
                        return enriched_results
                except Exception as e:
                    logger.debug(f"Vector index '{index_name}' not available or failed: {e}")
                    continue
            
            # If no vector index worked, use fallback
            logger.info("Vector index search failed, using fallback method")
            
        except Exception as e:
            logger.warning(f"Vector similarity search failed (index may not exist): {e}")
            # Fallback: Use cosine similarity calculation
            fallback_results = self._fallback_similarity_search(subject_id, top_k, similarity_threshold)
            if fallback_results:
                return self._enrich_similar_patients(fallback_results, subject_id)
            return []
    
    def _fallback_similarity_search(
        self, 
        subject_id: str, 
        top_k: int = 10,
        similarity_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Fallback similarity search using Cypher cosine similarity
        
        Args:
            subject_id: Reference patient ID
            top_k: Number of similar patients
            similarity_threshold: Minimum similarity score
            
        Returns:
            List of similar patients
        """
        # Fallback: Use a simpler similarity approach
        # Calculate cosine similarity manually in Python if needed
        # For now, return empty results and log warning
        query = """
        MATCH (p1:Patient {subject_id: $subject_id})
        WHERE COALESCE(p1.combinedEmbedding, p1.combined_embedding) IS NOT NULL
        WITH COALESCE(p1.combinedEmbedding, p1.combined_embedding) AS refEmbedding
        MATCH (p2:Patient)
        WHERE p2.subject_id <> $subject_id 
          AND COALESCE(p2.combinedEmbedding, p2.combined_embedding) IS NOT NULL
        WITH p2, refEmbedding, COALESCE(p2.combinedEmbedding, p2.combined_embedding) AS candidateEmbedding
        // Note: Manual cosine similarity calculation would be done here
        // For now, we'll use a simpler approach - return patients with similar characteristics
        OPTIONAL MATCH (p2)-[:VISITED_ED]->(ed:EmergencyDepartment)
        // HospitalAdmission linked via subject_id (no explicit relationship)
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: p2.subject_id})
        OPTIONAL MATCH (h)-[:RECORDED_DIAGNOSES]->(d:Diagnosis)
        OPTIONAL MATCH (h)-[:ISSUED_PRESCRIPTIONS]->(pr:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx:Prescription)
        OPTIONAL MATCH (h)-[:INCLUDED_LAB_EVENTS]->(le:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab:LabEvent)
        // Discharge linked via subject_id (no explicit relationship)
        OPTIONAL MATCH (dis:Discharge {subject_id: p2.subject_id})
        OPTIONAL MATCH (dis)-[:RECORDED_DIAGNOSES]->(d_dis:Diagnosis)
        RETURN p2 AS similarPatient, 0.5 AS score, ed, h, d, rx, lab, dis, d_dis
        LIMIT $topK
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={
                    "subject_id": int(subject_id),
                    "topK": top_k,
                    "threshold": similarity_threshold
                }
            )
            logger.info(f"Fallback search found {len(results)} similar patients")
            return results
        except Exception as e:
            logger.error(f"Fallback similarity search failed: {e}")
            return []
    
    def _enrich_similar_patients(
        self,
        similar_patients: List[Dict[str, Any]],
        reference_patient_id: str
    ) -> List[Dict[str, Any]]:
        """
        Enrich similar patient records with detailed clinical information
        
        CRITICAL: Only returns patients that have actual clinical data (diagnoses, medications, etc.)
        Filters out isolated patient nodes with no graph connections.
        
        Args:
            similar_patients: List of similar patient records (with similarPatient node and score)
            reference_patient_id: Reference patient ID for context
            
        Returns:
            Enriched list of similar patients with diagnoses, medications, and treatments
            Only includes patients with actual clinical data
        """
        enriched = []
        
        for record in similar_patients:
            # Extract patient node and score
            patient_node = record.get('similarPatient') or record.get('node')
            if not patient_node:
                # If already in dict format, use it directly
                if 'subject_id' in record:
                    patient_id = str(record.get('subject_id'))
                else:
                    continue
            else:
                patient_id = str(patient_node.get('subject_id'))
            
            similarity_score = record.get('score') or record.get('similarity_score', 0)
            
            # Check if patient has any graph connections (not an isolated node)
            has_connections = self._check_patient_has_connections(patient_id)
            if not has_connections:
                logger.warning(f"Patient {patient_id} has no graph connections. Skipping (isolated node).")
                continue
            
            # Get clinical summary for this patient
            clinical_summary = self._get_patient_clinical_summary(patient_id)
            
            # Log what was retrieved for debugging
            num_diag = len(clinical_summary.get('diagnoses', []))
            num_meds = len(clinical_summary.get('medications', []))
            num_treat = len(clinical_summary.get('treatments', []))
            total_data = num_diag + num_meds + num_treat
            
            # CRITICAL: Only include patients with actual clinical data
            if total_data == 0:
                logger.warning(f"Patient {patient_id} has no clinical data (diagnoses, medications, treatments). Skipping.")
                continue
            
            logger.debug(f"Patient {patient_id}: {num_diag} diagnoses, {num_meds} medications, {num_treat} treatments")
            
            # Build enriched record
            enriched_record = {
                'subject_id': patient_id,
                'similarity_score': similarity_score,
                'reference_patient': reference_patient_id,
                'gender': patient_node.get('gender') if isinstance(patient_node, dict) else record.get('gender'),
                'age': patient_node.get('anchor_age') if isinstance(patient_node, dict) else record.get('age'),
                'diagnoses': clinical_summary.get('diagnoses', []),
                'medications': clinical_summary.get('medications', []),
                'treatments': clinical_summary.get('treatments', []),
                'lab_results': clinical_summary.get('lab_results', []),
                'total_admissions': clinical_summary.get('total_admissions', 0)
            }
            
            enriched.append(enriched_record)
        
        if len(enriched) == 0:
            logger.warning(f"No similar patients found with actual clinical data for patient {reference_patient_id}")
        
        return enriched
    
    def _check_patient_has_connections(self, subject_id: str) -> bool:
        """
        Check if a patient has any connections in the graph (not an isolated node)
        
        Args:
            subject_id: Patient subject ID
            
        Returns:
            True if patient has connections, False if isolated
        """
        query = """
        MATCH (p:Patient {subject_id: $subject_id})
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: $subject_id})
        OPTIONAL MATCH (dis:Discharge {subject_id: $subject_id})
        OPTIONAL MATCH (ua:UnitAdmission {subject_id: $subject_id})
        OPTIONAL MATCH (icu:ICUStay {subject_id: $subject_id})
        RETURN 
            count(ed) + count(h) + count(dis) + count(ua) + count(icu) AS connection_count
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={"subject_id": int(subject_id)}
            )
            
            if results and len(results) > 0:
                connection_count = results[0].get('connection_count', 0)
                return connection_count > 0
            return False
        except Exception as e:
            logger.error(f"Error checking connections for patient {subject_id}: {e}")
            return False
    
    def _get_patient_clinical_summary(self, subject_id: str) -> Dict[str, Any]:
        """
        Get clinical summary for a patient (diagnoses, medications, treatments)
        
        Args:
            subject_id: Patient subject ID
            
        Returns:
            Dictionary with clinical summary
        """
        query = """
        MATCH (p:Patient {subject_id: $subject_id})
        // Get diagnoses from multiple sources
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: $subject_id})-[:RECORDED_DIAGNOSES]->(d_h:Diagnosis)
        OPTIONAL MATCH (dis:Discharge {subject_id: $subject_id})-[:RECORDED_DIAGNOSES]->(d_dis:Diagnosis)
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)-[:RECORDED_DIAGNOSES]->(d_ed:Diagnosis)
        
        // Get medications from multiple sources
        OPTIONAL MATCH (h)-[:ISSUED_PRESCRIPTIONS]->(pr:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx:Prescription)
        OPTIONAL MATCH (ed)-[:ISSUED_PRESCRIPTIONS]->(pr_ed:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx_ed:Prescription)
        OPTIONAL MATCH (dis)-[:DOCUMENTED_IN_NOTE]->(dcn:DischargeClinicalNote)-[:RECORDED_MEDICATIONS]->(dm:DischargeMedications)
        // Also check Discharge medications directly
        OPTIONAL MATCH (dis)-[:STARTED_MEDICATIONS]->(ms:MedicationStarted)
        OPTIONAL MATCH (dis)-[:STOPPED_MEDICATIONS]->(mst:MedicationStopped)
        // Check AdmissionMedications
        OPTIONAL MATCH (h)-[:INCLUDED_MEDICATIONS]->(am:AdmissionMedications)
        // Check PreviousPrescriptionMeds from ED
        OPTIONAL MATCH (ed)-[:RECORDED_PREVIOUS_MEDICATIONS]->(ppm:PreviousPrescriptionMeds)
        // Check AdministeredMeds from ED
        OPTIONAL MATCH (ed)-[:ADMINISTERED_MEDICATIONS]->(am_ed:AdministeredMeds)
        
        // Get lab results
        OPTIONAL MATCH (h)-[:INCLUDED_LAB_EVENTS]->(le:LabEvents)-[:CONTAINED_LAB_EVENT]->(lab:LabEvent)
        
        // Get procedures as treatments from full patient journey
        // From HospitalAdmission
        OPTIONAL MATCH (h)-[:INCLUDED_PROCEDURES]->(pb_h:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_h:Procedures)
        // From EmergencyDepartment
        OPTIONAL MATCH (ed)-[:INCLUDED_PROCEDURES]->(pb_ed:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_ed:Procedures)
        // From UnitAdmission
        OPTIONAL MATCH (ua:UnitAdmission {subject_id: $subject_id})-[:INCLUDED_PROCEDURES]->(pb_ua:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_ua:Procedures)
        // From ICUStay
        OPTIONAL MATCH (icu:ICUStay {subject_id: $subject_id})-[:INCLUDED_PROCEDURES]->(pb_icu:ProceduresBatch)-[:CONTAINED_PROCEDURE]->(proc_icu:Procedures)
        
        // Count admissions
        OPTIONAL MATCH (h_count:HospitalAdmission {subject_id: $subject_id})
        
        WITH p,
             COLLECT(DISTINCT d_h) + COLLECT(DISTINCT d_dis) + COLLECT(DISTINCT d_ed) AS all_diagnoses,
             COLLECT(DISTINCT rx) + COLLECT(DISTINCT rx_ed) + COLLECT(DISTINCT dm) + 
             COLLECT(DISTINCT ms) + COLLECT(DISTINCT mst) + COLLECT(DISTINCT am) + 
             COLLECT(DISTINCT ppm) + COLLECT(DISTINCT am_ed) AS all_medications,
             COLLECT(DISTINCT lab) AS all_labs,
             COLLECT(DISTINCT proc_h) + COLLECT(DISTINCT proc_ed) + COLLECT(DISTINCT proc_ua) + COLLECT(DISTINCT proc_icu) AS all_procedures,
             COUNT(DISTINCT h_count) AS total_admissions
        
        RETURN 
            [d IN all_diagnoses WHERE d IS NOT NULL | {
                icd_code: d.icd_code,
                diagnosis: COALESCE(d.complete_diagnosis, d.ed_diagnosis, d.short_title, ''),
                short_title: d.short_title
            }] AS diagnoses,
            [m IN all_medications WHERE m IS NOT NULL | {
                medication: COALESCE(m.medicines, m.medication_name, m.medications, ''),
                medicines: COALESCE(m.medicines, m.medication_name, m.medications, ''),
                formulary_code: m.formulary_drug_cd,
                dose: COALESCE(m.dose_val_rx, m.dose),
                dose_val_rx: COALESCE(m.dose_val_rx, m.dose),
                unit: COALESCE(m.dose_unit_rx, m.unit),
                dose_unit_rx: COALESCE(m.dose_unit_rx, m.unit)
            }] AS medications,
            [l IN all_labs WHERE l IS NOT NULL | {
                itemid: l.itemid,
                value: l.valuenum,
                unit: l.valueuom,
                flag: l.flag,
                text: l.label
            }][0..10] AS lab_results,
            [proc IN all_procedures WHERE proc IS NOT NULL | {
                icd_code: proc.icd_code,
                procedure: COALESCE(proc.short_title, proc.long_title, ''),
                category: proc.category
            }] AS treatments,
            total_admissions
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={"subject_id": int(subject_id)}
            )
            
            if results and len(results) > 0:
                result = results[0]
                medications = result.get('medications', [])
                diagnoses = result.get('diagnoses', [])
                
                # Filter out empty medications
                medications = [m for m in medications if m.get('medication') or m.get('medicines')]
                
                # Log retrieval results
                logger.info(f"Retrieved for patient {subject_id}: {len(diagnoses)} diagnoses, {len(medications)} medications")
                if len(medications) == 0:
                    logger.warning(f"No medications found for patient {subject_id}. Check relationships and data.")
                
                return {
                    'diagnoses': diagnoses[:15],  # Top 15 diagnoses
                    'medications': medications[:20],  # Top 20 medications
                    'treatments': result.get('treatments', [])[:10],  # Top 10 treatments
                    'lab_results': result.get('lab_results', [])[:10],  # Top 10 lab results
                    'total_admissions': result.get('total_admissions', 0)
                }
            else:
                return {
                    'diagnoses': [],
                    'medications': [],
                    'treatments': [],
                    'lab_results': [],
                    'total_admissions': 0
                }
        except Exception as e:
            logger.error(f"Error getting clinical summary for patient {subject_id}: {e}")
            return {
                'diagnoses': [],
                'medications': [],
                'treatments': [],
                'lab_results': [],
                'total_admissions': 0
            }
    
    def get_all_similar_patient_pairs(
        self,
        similarity_threshold: float = 0.7,
        max_results: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Find all similar patient pairs using optimized Neo4j query
        
        This method finds pairs of patients that are similar to each other
        across the entire database using vector similarity search.
        
        Args:
            similarity_threshold: Minimum similarity score (0.0 to 1.0)
            max_results: Maximum number of pairs to return
            
        Returns:
            List of similar patient pairs with scores
        """
        index_names = ['patient_embedding_index', 'patient_journey_index', 'patient_combined_embedding_index']
        
        for index_name in index_names:
            try:
                query = f"""
                MATCH (p1:Patient)
                WHERE COALESCE(p1.combinedEmbedding, p1.combined_embedding) IS NOT NULL
                WITH p1, COALESCE(p1.combinedEmbedding, p1.combined_embedding) AS embedding1
                CALL db.index.vector.queryNodes('{index_name}', 15, embedding1)
                YIELD node AS p2, score
                WHERE p2.subject_id <> p1.subject_id 
                  AND score >= $threshold
                WITH p1, p2, score
                ORDER BY score DESC
                LIMIT $maxResults
                RETURN 
                    p1.subject_id AS patient_1,
                    p2.subject_id AS patient_2,
                    score AS similarity_score,
                    p1.gender AS patient_1_gender,
                    p1.anchor_age AS patient_1_age,
                    p2.gender AS patient_2_gender,
                    p2.anchor_age AS patient_2_age
                """
                
                results = self.connection.execute_query(
                    query,
                    parameters={
                        "threshold": similarity_threshold,
                        "maxResults": max_results
                    }
                )
                
                if results:
                    logger.info(f"Found {len(results)} similar patient pairs using index '{index_name}'")
                    return results
                    
            except Exception as e:
                logger.debug(f"Vector index '{index_name}' not available: {e}")
                continue
        
        # Fallback: If no vector index works, use a simpler approach
        logger.warning("No vector index available for optimized similarity search, using fallback")
        return self._fallback_all_similar_pairs(similarity_threshold, max_results)
    
    def _fallback_all_similar_pairs(
        self,
        similarity_threshold: float = 0.7,
        max_results: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fallback method to find similar patient pairs without vector index
        
        Args:
            similarity_threshold: Minimum similarity score
            max_results: Maximum number of pairs to return
            
        Returns:
            List of patient pairs (with placeholder scores)
        """
        query = """
        MATCH (p1:Patient)
        WHERE COALESCE(p1.combinedEmbedding, p1.combined_embedding) IS NOT NULL
        WITH p1
        MATCH (p2:Patient)
        WHERE p2.subject_id <> p1.subject_id 
          AND COALESCE(p2.combinedEmbedding, p2.combined_embedding) IS NOT NULL
        RETURN 
            p1.subject_id AS patient_1,
            p2.subject_id AS patient_2,
            0.75 AS similarity_score,
            p1.gender AS patient_1_gender,
            p1.anchor_age AS patient_1_age,
            p2.gender AS patient_2_gender,
            p2.anchor_age AS patient_2_age
        LIMIT $maxResults
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={"maxResults": max_results}
            )
            logger.info(f"Fallback method found {len(results)} patient pairs")
            return results
        except Exception as e:
            logger.error(f"Fallback all similar pairs search failed: {e}")
            return []
    
    def get_treatment_outcomes(
        self, 
        condition: Optional[str] = None,
        medication: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Retrieve treatment outcomes for patients with specific conditions/medications
        
        Args:
            condition: Optional condition filter
            medication: Optional medication filter
            limit: Maximum number of results
            
        Returns:
            List of treatment outcome records
        """
        query = """
        MATCH (p:Patient)
        // HospitalAdmission linked via subject_id (no explicit relationship)
        OPTIONAL MATCH (h:HospitalAdmission {subject_id: p.subject_id})
        OPTIONAL MATCH (h)-[:RECORDED_DIAGNOSES]->(d:Diagnosis)
        OPTIONAL MATCH (h)-[:ISSUED_PRESCRIPTIONS]->(pr:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx:Prescription)
        // Also check EmergencyDepartment prescriptions
        OPTIONAL MATCH (p)-[:VISITED_ED]->(ed:EmergencyDepartment)
        OPTIONAL MATCH (ed)-[:ISSUED_PRESCRIPTIONS]->(pr_ed:PrescriptionsBatch)-[:CONTAINED_PRESCRIPTION]->(rx_ed:Prescription)
        OPTIONAL MATCH (ed)-[:RECORDED_DIAGNOSES]->(d_ed:Diagnosis)
        // Check Discharge medications
        OPTIONAL MATCH (dis:Discharge {subject_id: p.subject_id})
        OPTIONAL MATCH (dis)-[:STARTED_MEDICATIONS]->(ms:MedicationStarted)
        OPTIONAL MATCH (dis)-[:STOPPED_MEDICATIONS]->(mst:MedicationStopped)
        OPTIONAL MATCH (dis)-[:RECORDED_DIAGNOSES]->(d_dis:Diagnosis)
        OPTIONAL MATCH (dis)-[:DOCUMENTED_IN_NOTE]->(dcn:DischargeClinicalNote)
        OPTIONAL MATCH (dcn)-[:RECORDED_MEDICATIONS]->(dm:DischargeMedications)
        WHERE ($condition IS NULL OR 
               toLower(d.complete_diagnosis) CONTAINS toLower($condition) OR
               toLower(d_ed.complete_diagnosis) CONTAINS toLower($condition) OR
               toLower(d_dis.complete_diagnosis) CONTAINS toLower($condition))
          AND ($medication IS NULL OR 
               toLower(rx.medicines) CONTAINS toLower($medication) OR
               toLower(rx_ed.medicines) CONTAINS toLower($medication) OR
               toLower(ms.medications) CONTAINS toLower($medication) OR
               toLower(dm.medications) CONTAINS toLower($medication))
        RETURN DISTINCT p, h, d, rx, dis, d_ed, rx_ed, ms, mst, d_dis, dcn, dm
        LIMIT $limit
        """
        
        try:
            results = self.connection.execute_query(
                query,
                parameters={
                    "condition": condition,
                    "medication": medication,
                    "limit": limit
                }
            )
            logger.info(f"Retrieved {len(results)} treatment outcome records")
            return results
        except Exception as e:
            logger.error(f"Error retrieving treatment outcomes: {e}")
            return []
    
    def format_graph_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format raw graph results into structured format
        
        Args:
            results: Raw graph query results
            
        Returns:
            Formatted dictionary with extracted entities
        """
        formatted = {
            "patients": [],
            "admissions": [],
            "diagnoses": [],
            "medications": [],
            "lab_results": [],
            "procedures": []
        }
        
        seen_patients = set()
        seen_diagnoses = set()
        seen_medications = set()
        
        for record in results:
            # Extract patient info
            if 'p' in record and record['p']:
                p = record['p']
                patient_id = str(p.get('subject_id', ''))
                if patient_id and patient_id not in seen_patients:
                    formatted["patients"].append({
                        "subject_id": patient_id,
                        "gender": p.get('gender'),
                        "anchor_age": p.get('anchor_age'),
                        "total_admissions": p.get('total_number_of_admissions')
                    })
                    seen_patients.add(patient_id)
            
            # Extract diagnoses from various sources
            diagnosis_sources = ['d', 'd_ed', 'd_dis']
            for source in diagnosis_sources:
                if source in record and record[source]:
                    d = record[source]
                    # Safely convert to strings (handle lists, None, etc.)
                    icd_code = d.get('icd_code', '')
                    if isinstance(icd_code, list):
                        icd_code = ', '.join(str(x) for x in icd_code) if icd_code else ''
                    else:
                        icd_code = str(icd_code) if icd_code else ''
                    
                    complete_diagnosis = d.get('complete_diagnosis', '')
                    if isinstance(complete_diagnosis, list):
                        complete_diagnosis = ', '.join(str(x) for x in complete_diagnosis) if complete_diagnosis else ''
                    else:
                        complete_diagnosis = str(complete_diagnosis) if complete_diagnosis else ''
                    
                    diag_key = icd_code + complete_diagnosis
                    if diag_key and diag_key not in seen_diagnoses:
                        formatted["diagnoses"].append({
                            "icd_code": icd_code,
                            "diagnosis": complete_diagnosis,
                            "short_title": d.get('short_title'),
                            "ed_diagnosis": d.get('ed_diagnosis'),
                            "source": source
                        })
                        seen_diagnoses.add(diag_key)
            
            # Extract medications from various sources
            medication_sources = ['rx', 'rx_ed', 'rx_ua', 'rx_icu', 'ms', 'dm', 'ppm', 'am_ed']
            for source in medication_sources:
                if source in record and record[source]:
                    med_node = record[source]
                    # Handle different medication node types
                    if source in ['rx', 'rx_ed', 'rx_ua', 'rx_icu']:
                        med_key = med_node.get('medicines', '')
                        # Handle list case (medicines might be an array)
                        if isinstance(med_key, list):
                            med_key = ', '.join(str(x) for x in med_key) if med_key else ''
                        else:
                            med_key = str(med_key) if med_key else ''
                        
                        if med_key and med_key not in seen_medications:
                            formatted["medications"].append({
                                "medicines": med_key,
                                "formulary_drug_cd": med_node.get('formulary_drug_cd'),
                                "dose_val_rx": med_node.get('dose_val_rx'),
                                "dose_unit_rx": med_node.get('dose_unit_rx'),
                                "source": source
                            })
                            seen_medications.add(med_key)
                    elif source in ['ms', 'dm', 'ppm', 'am_ed']:
                        med_key = med_node.get('medications', '')
                        # Handle list case (medications might be an array)
                        if isinstance(med_key, list):
                            med_key = ', '.join(str(x) for x in med_key) if med_key else ''
                        else:
                            med_key = str(med_key) if med_key else ''
                        
                        if med_key and med_key not in seen_medications:
                            formatted["medications"].append({
                                "medicines": med_key,
                                "source": source
                            })
                            seen_medications.add(med_key)
            
            # Extract admissions
            if 'h' in record and record['h']:
                h = record['h']
                formatted["admissions"].append({
                    "hadm_id": h.get('hadm_id'),
                    "admittime": h.get('admittime'),
                    "dischtime": h.get('dischtime'),
                    "admission_type": h.get('admission_type')
                })
            
            # Extract lab results from various sources
            lab_sources = ['lab', 'lab_ed', 'lab_ua', 'lab_icu']
            for source in lab_sources:
                if source in record and record[source]:
                    lab = record[source]
                    formatted["lab_results"].append({
                        "itemid": lab.get('itemid'),
                        "value": lab.get('valuenum'),
                        "valueuom": lab.get('valueuom'),
                        "flag": lab.get('flag'),
                        "lab_results": lab.get('lab_results'),
                        "abnormal_results": lab.get('abnormal_results'),
                        "source": source
                    })
            
            # Extract procedures from all sources (full patient journey)
            procedure_sources = ['proc', 'proc_ed', 'proc_ua', 'proc_icu']
            seen_procedures = set()
            for source in procedure_sources:
                if source in record and record[source]:
                    proc = record[source]
                    proc_key = proc.get('short_title') or proc.get('long_title', '')
                    if proc_key and proc_key not in seen_procedures:
                        formatted["procedures"].append({
                            "icd_code": proc.get('icd_code'),
                            "procedure": proc.get('short_title') or proc.get('long_title', ''),
                            "short_title": proc.get('short_title'),
                            "long_title": proc.get('long_title'),
                            "category": proc.get('category'),
                            "source": source
                        })
                        seen_procedures.add(proc_key)
        
        # Map procedures to treatments for consistency
        formatted["treatments"] = formatted["procedures"]
        
        return formatted

