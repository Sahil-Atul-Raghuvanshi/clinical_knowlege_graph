"""
Hybrid Retriever Module
Combines graph traversal (Neo4j) and semantic search (Milvus) results
"""
import logging
from typing import List, Dict, Any, Optional
from graph_retriever import GraphRetriever
from vector_retriever import VectorRetriever
from intent_detector import QueryIntent, IntentDetector

logger = logging.getLogger(__name__)


class HybridRetriever:
    """Combines graph and vector retrieval with ranking"""
    
    def __init__(
        self,
        graph_retriever: Optional[GraphRetriever] = None,
        vector_retriever: Optional[VectorRetriever] = None,
        graph_weight: float = 0.4,
        vector_weight: float = 0.6
    ):
        """
        Initialize hybrid retriever
        
        Args:
            graph_retriever: Graph retriever instance
            vector_retriever: Vector retriever instance
            graph_weight: Weight for graph results (default 0.4)
            vector_weight: Weight for vector results (default 0.6)
        """
        self.graph_retriever = graph_retriever or GraphRetriever()
        self.vector_retriever = vector_retriever or VectorRetriever()
        self.intent_detector = IntentDetector()
        self.graph_weight = graph_weight
        self.vector_weight = vector_weight
        logger.info("HybridRetriever initialized")
    
    def close(self):
        """Close all connections"""
        if self.graph_retriever:
            self.graph_retriever.close()
        if self.vector_retriever:
            self.vector_retriever.close()
    
    def retrieve(
        self,
        query: str,
        top_k: int = 20,
        graph_limit: int = 25,
        vector_limit: int = 15
    ) -> Dict[str, Any]:
        """
        Perform hybrid retrieval combining graph and vector results
        
        Args:
            query: User query
            top_k: Total number of results to return
            graph_limit: Maximum graph results
            vector_limit: Maximum vector results
            
        Returns:
            Combined results dictionary
        """
        # Detect intent
        intent, entities = self.intent_detector.detect_intent(query)
        
        # Retrieve from graph
        graph_results = self._retrieve_graph(query, intent, entities, graph_limit)
        
        # Retrieve from vector database
        vector_results = self._retrieve_vector(query, intent, vector_limit)
        
        # Merge and rank results
        merged_results = self._merge_and_rank(
            graph_results=graph_results,
            vector_results=vector_results,
            top_k=top_k
        )
        
        return {
            "intent": intent.value,
            "entities": entities,
            "graph_results": graph_results,
            "vector_results": vector_results,
            "merged_results": merged_results
        }
    
    def _retrieve_graph(
        self,
        query: str,
        intent: QueryIntent,
        entities: Dict[str, Any],
        limit: int
    ) -> Dict[str, Any]:
        """Retrieve results from graph based on intent"""
        try:
            if intent == QueryIntent.ALL_SIMILAR_PAIRS:
                # Find all similar patient pairs across the database
                similar_pairs = self.graph_retriever.get_all_similar_patient_pairs(
                    similarity_threshold=0.7,
                    max_results=500
                )
                return {
                    "all_similar_pairs": similar_pairs
                }
            
            elif intent == QueryIntent.PATIENT_SIMILARITY:
                patient_id = entities.get('patient_id')
                if patient_id:
                    # First, check if reference patient has connections in the graph
                    has_connections = self.graph_retriever._check_patient_has_connections(patient_id)
                    if not has_connections:
                        logger.warning(f"Reference patient {patient_id} has no graph connections. Cannot find similar patients.")
                        return {
                            "similar_patients": [],
                            "reference_patient": [],
                            "error": f"Patient {patient_id} has no clinical data in the knowledge graph (isolated node)"
                        }
                    
                    # Get more similar patients to account for filtering out isolated nodes
                    similar_patients = self.graph_retriever.get_similar_patients_by_embedding(
                        subject_id=patient_id,
                        top_k=30  # Get more to account for filtering out isolated nodes
                    )
                    # Also get reference patient journey
                    patient_journey = self.graph_retriever.get_patient_journey(
                        subject_id=patient_id,
                        limit=limit
                    )
                    return {
                        "similar_patients": similar_patients,
                        "reference_patient": patient_journey
                    }
                else:
                    # Fallback: search by condition
                    condition = entities.get('condition')
                    if condition:
                        return {
                            "patients_by_condition": self.graph_retriever.get_patient_by_condition(
                                condition=condition,
                                limit=limit
                            )
                        }
            
            elif intent == QueryIntent.TREATMENT_RECOMMENDATION:
                condition = entities.get('condition')
                return {
                    "treatment_outcomes": self.graph_retriever.get_treatment_outcomes(
                        condition=condition,
                        limit=limit
                    )
                }
            
            elif intent == QueryIntent.CLINICAL_SUMMARY:
                patient_id = entities.get('patient_id')
                if patient_id:
                    return {
                        "patient_journey": self.graph_retriever.get_patient_journey(
                            subject_id=patient_id,
                            limit=limit
                        )
                    }
            
            # General query - try to extract patient ID or condition
            patient_id = entities.get('patient_id')
            if patient_id:
                return {
                    "patient_journey": self.graph_retriever.get_patient_journey(
                        subject_id=patient_id,
                        limit=limit
                    )
                }
            
            condition = entities.get('condition')
            if condition:
                return {
                    "patients_by_condition": self.graph_retriever.get_patient_by_condition(
                        condition=condition,
                        limit=limit
                    )
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error in graph retrieval: {e}")
            return {}
    
    def _retrieve_vector(
        self,
        query: str,
        intent: QueryIntent,
        limit: int
    ) -> Dict[str, Any]:
        """Retrieve results from vector database"""
        try:
            results = self.vector_retriever.search_by_intent(
                query=query,
                intent=intent.value,
                top_k=limit
            )
            return self.vector_retriever.format_vector_results(results)
        except Exception as e:
            logger.error(f"Error in vector retrieval: {e}")
            return {}
    
    def _merge_and_rank(
        self,
        graph_results: Dict[str, Any],
        vector_results: Dict[str, Any],
        top_k: int
    ) -> Dict[str, Any]:
        """
        Merge graph and vector results with weighted ranking
        
        Args:
            graph_results: Results from graph retrieval
            vector_results: Results from vector retrieval
            top_k: Number of top results to return
            
        Returns:
            Merged and ranked results
        """
        merged = {
            "patients": [],
            "diagnoses": [],
            "medications": [],
            "treatments": [],
            "lab_results": [],
            "similar_patients": []
        }
        
        # Format graph results
        if "reference_patient" in graph_results:
            formatted_graph = self.graph_retriever.format_graph_results(
                graph_results["reference_patient"]
            )
            merged["patients"].extend(formatted_graph.get("patients", []))
            merged["diagnoses"].extend(formatted_graph.get("diagnoses", []))
            merged["medications"].extend(formatted_graph.get("medications", []))
            merged["treatments"].extend(formatted_graph.get("treatments", []))
            merged["lab_results"].extend(formatted_graph.get("lab_results", []))
        
        if "similar_patients" in graph_results:
            # Use a set to track seen patient IDs to avoid duplicates
            seen_patient_ids = set()
            for record in graph_results["similar_patients"]:
                # Check if record is already enriched (has diagnoses, medications, etc.)
                if isinstance(record, dict) and "diagnoses" in record:
                    # Already enriched, use as-is
                    patient_id = str(record.get("subject_id", ""))
                    if patient_id and patient_id not in seen_patient_ids:
                        merged["similar_patients"].append(record)
                        seen_patient_ids.add(patient_id)
                elif "similarPatient" in record:
                    # Not enriched, extract basic info
                    patient = record["similarPatient"]
                    patient_id = str(patient.get("subject_id", ""))
                    
                    # Only add if we haven't seen this patient ID before
                    if patient_id and patient_id not in seen_patient_ids:
                        merged["similar_patients"].append({
                            "subject_id": patient_id,
                            "similarity_score": record.get("score", 0),
                            "gender": patient.get("gender"),
                            "age": patient.get("anchor_age")
                        })
                        seen_patient_ids.add(patient_id)
                elif isinstance(record, dict) and "subject_id" in record:
                    # Already in dict format, preserve all fields
                    patient_id = str(record.get("subject_id", ""))
                    if patient_id and patient_id not in seen_patient_ids:
                        merged["similar_patients"].append(record)
                        seen_patient_ids.add(patient_id)
        
        # Handle all similar pairs
        if "all_similar_pairs" in graph_results:
            pairs = graph_results["all_similar_pairs"]
            # Convert pairs to similar_patients format for display
            # Group by patient_1 to show all pairs for each patient
            for pair in pairs:
                merged["similar_patients"].append({
                    "subject_id": str(pair.get("patient_2", "")),
                    "similarity_score": pair.get("similarity_score", 0),
                    "gender": pair.get("patient_2_gender"),
                    "age": pair.get("patient_2_age"),
                    "reference_patient": str(pair.get("patient_1", ""))
                })
            # Also store pairs separately
            merged["all_similar_pairs"] = pairs
        
        if "patients_by_condition" in graph_results:
            formatted_graph = self.graph_retriever.format_graph_results(
                graph_results["patients_by_condition"]
            )
            merged["patients"].extend(formatted_graph.get("patients", []))
            merged["diagnoses"].extend(formatted_graph.get("diagnoses", []))
            merged["medications"].extend(formatted_graph.get("medications", []))
            # Map procedures to treatments
            merged["treatments"].extend(formatted_graph.get("treatments", formatted_graph.get("procedures", [])))
        
        if "treatment_outcomes" in graph_results:
            formatted_graph = self.graph_retriever.format_graph_results(
                graph_results["treatment_outcomes"]
            )
            merged["patients"].extend(formatted_graph.get("patients", []))
            merged["medications"].extend(formatted_graph.get("medications", []))
            merged["diagnoses"].extend(formatted_graph.get("diagnoses", []))
        
        # Add vector results with weighted scores
        for med in vector_results.get("medications", []):
            # Apply vector weight to similarity score
            med["final_score"] = med.get("similarity_score", 0) * self.vector_weight
            merged["medications"].append(med)
        
        for lab in vector_results.get("lab_results", []):
            lab["final_score"] = lab.get("similarity_score", 0) * self.vector_weight
            merged["lab_results"].append(lab)
        
        for diag in vector_results.get("diagnoses", []):
            diag["final_score"] = diag.get("similarity_score", 0) * self.vector_weight
            merged["diagnoses"].append(diag)
        
        # Remove duplicates and sort by score
        merged["medications"] = self._deduplicate_and_sort(
            merged["medications"], 
            key="medicines",
            top_k=top_k
        )
        merged["diagnoses"] = self._deduplicate_and_sort(
            merged["diagnoses"],
            key="diagnosis",
            top_k=top_k
        )
        merged["lab_results"] = merged["lab_results"][:top_k]
        # Deduplicate similar patients by subject_id, keeping the highest score
        # Use a dictionary to track unique patients with their best score
        unique_patients = {}
        for patient in merged["similar_patients"]:
            patient_id = patient.get("subject_id", "")
            if patient_id:
                current_score = patient.get("similarity_score", 0)
                if patient_id not in unique_patients or current_score > unique_patients[patient_id].get("similarity_score", 0):
                    unique_patients[patient_id] = patient
        
        # Convert back to list and sort by score
        merged["similar_patients"] = sorted(
            list(unique_patients.values()),
            key=lambda x: x.get("similarity_score", 0),
            reverse=True
        )
        
        # For patient similarity, ensure at least 10 results
        if len(merged["similar_patients"]) > 0:
            # Take at least 10, but up to top_k
            min_results = min(10, len(merged["similar_patients"]))
            max_results = max(min_results, top_k)
            merged["similar_patients"] = merged["similar_patients"][:max_results]
        else:
            merged["similar_patients"] = merged["similar_patients"][:top_k]
        
        return merged
    
    def _deduplicate_and_sort(
        self,
        items: List[Dict[str, Any]],
        key: str,
        top_k: int
    ) -> List[Dict[str, Any]]:
        """Remove duplicates and sort by score"""
        seen = set()
        unique_items = []
        
        for item in items:
            item_key = item.get(key, "")
            if item_key and item_key not in seen:
                seen.add(item_key)
                unique_items.append(item)
        
        # Sort by final_score if available, else by similarity_score
        unique_items.sort(
            key=lambda x: x.get("final_score", x.get("similarity_score", 0)),
            reverse=True
        )
        
        return unique_items[:top_k]

