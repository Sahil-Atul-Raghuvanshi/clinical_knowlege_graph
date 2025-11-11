"""
Context Builder Module
Structures retrieved data into JSON context for LLM
"""
import logging
import json
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class ContextBuilder:
    """Builds structured context from graph and vector results"""
    
    def __init__(self):
        """Initialize context builder"""
        logger.info("ContextBuilder initialized")
    
    def _normalize_to_string(self, value):
        """Convert value to string, handling lists and None"""
        if value is None:
            return ""
        if isinstance(value, list):
            # If it's a list, join non-empty elements or return first element
            non_empty = [str(v) for v in value if v]
            return ", ".join(non_empty) if non_empty else ""
        return str(value)
    
    def build_context(
        self,
        merged_results: Dict[str, Any],
        entities: Dict[str, Any],
        intent: str
    ) -> Dict[str, Any]:
        """
        Build structured context for LLM
        
        Args:
            merged_results: Merged graph and vector results
            entities: Extracted entities from query
            intent: Detected query intent
            
        Returns:
            Structured context dictionary
        """
        context = {
            "query_intent": intent,
            "extracted_entities": entities,
            "patient_data": {},
            "clinical_findings": {},
            "similar_patients": [],
            "summary": {}
        }
        
        # Extract patient data
        if merged_results.get("patients"):
            primary_patient = merged_results["patients"][0] if merged_results["patients"] else {}
            context["patient_data"] = {
                "subject_id": primary_patient.get("subject_id"),
                "gender": primary_patient.get("gender"),
                "age": primary_patient.get("anchor_age"),
                "total_admissions": primary_patient.get("total_admissions")
            }
        
        # Extract clinical findings
        context["clinical_findings"] = {
            "diagnoses": self._format_diagnoses(merged_results.get("diagnoses", [])),
            "medications": self._format_medications(merged_results.get("medications", [])),
            "treatments": self._format_treatments(merged_results.get("treatments", [])),
            "lab_results": self._format_lab_results(merged_results.get("lab_results", []))
        }
        
        # Extract similar patients
        context["similar_patients"] = merged_results.get("similar_patients", [])
        
        # Extract all similar pairs if available
        if merged_results.get("all_similar_pairs"):
            context["all_similar_pairs"] = merged_results.get("all_similar_pairs", [])
        
        # Build summary
        context["summary"] = self._build_summary(merged_results, intent)
        
        return context
    
    def _format_diagnoses(self, diagnoses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format diagnosis data"""
        formatted = []
        seen = set()
        
        for diag in diagnoses:
            diag_key = diag.get("diagnosis") or diag.get("icd_code", "")
            if diag_key and diag_key not in seen:
                formatted.append({
                    "icd_code": diag.get("icd_code"),
                    "diagnosis": diag.get("diagnosis") or diag.get("text", ""),
                    "short_title": diag.get("short_title"),
                    "relevance_score": diag.get("similarity_score") or diag.get("final_score", 0)
                })
                seen.add(diag_key)
        
        return formatted[:10]  # Top 10 diagnoses
    
    def _format_medications(self, medications: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format medication data"""
        formatted = []
        seen = set()
        
        for med in medications:
            med_key = med.get("medicines") or med.get("text", "")
            if med_key and med_key not in seen:
                formatted.append({
                    "medication": med.get("medicines") or med.get("text", ""),
                    "formulary_code": med.get("formulary_drug_cd"),
                    "dose": med.get("dose_val_rx"),
                    "unit": med.get("dose_unit_rx"),
                    "relevance_score": med.get("similarity_score") or med.get("final_score", 0),
                    "source_node_id": med.get("source_node_id")
                })
                seen.add(med_key)
        
        return formatted[:15]  # Top 15 medications
    
    def _format_treatments(self, treatments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format treatment/procedure data"""
        formatted = []
        seen = set()
        
        for treat in treatments:
            treat_key = treat.get("procedure") or treat.get("short_title", "")
            if treat_key and treat_key not in seen:
                formatted.append({
                    "icd_code": treat.get("icd_code"),
                    "procedure": treat.get("procedure") or treat.get("short_title", ""),
                    "short_title": treat.get("short_title"),
                    "category": treat.get("category"),
                    "relevance_score": treat.get("similarity_score") or treat.get("final_score", 0)
                })
                seen.add(treat_key)
        
        return formatted[:15]  # Top 15 treatments
    
    def _format_lab_results(self, lab_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format lab result data"""
        formatted = []
        
        for lab in lab_results[:10]:  # Top 10 lab results
            formatted.append({
                "item_id": lab.get("itemid"),
                "value": lab.get("value") or lab.get("valuenum"),
                "unit": lab.get("valueuom") or lab.get("unit"),
                "flag": lab.get("flag"),
                "text": lab.get("text"),
                "relevance_score": lab.get("similarity_score") or lab.get("final_score", 0)
            })
        
        return formatted
    
    def _build_summary(
        self,
        merged_results: Dict[str, Any],
        intent: str
    ) -> Dict[str, Any]:
        """Build summary statistics"""
        summary = {
            "total_patients": len(merged_results.get("patients", [])),
            "total_diagnoses": len(merged_results.get("diagnoses", [])),
            "total_medications": len(merged_results.get("medications", [])),
            "total_lab_results": len(merged_results.get("lab_results", [])),
            "similar_patients_count": len(merged_results.get("similar_patients", []))
        }
        
        # Add intent-specific summary
        if intent == "patient_similarity" or intent == "all_similar_pairs":
            similar_patients = merged_results.get("similar_patients", [])
            if similar_patients:
                summary["similarity_info"] = {
                    "top_similarity_score": max(
                        [p.get("similarity_score", 0) for p in similar_patients],
                        default=0
                    ),
                    "average_similarity": sum(
                        [p.get("similarity_score", 0) for p in similar_patients]
                    ) / max(len(similar_patients), 1),
                    "total_similar_patients": len(similar_patients)
                }
            
            # For all similar pairs, add pair statistics
            if intent == "all_similar_pairs" and merged_results.get("all_similar_pairs"):
                pairs = merged_results.get("all_similar_pairs", [])
                summary["pair_statistics"] = {
                    "total_pairs": len(pairs),
                    "unique_patients": len(set(
                        [str(p.get("patient_1", "")) for p in pairs] + 
                        [str(p.get("patient_2", "")) for p in pairs]
                    ))
                }
        
        return summary
    
    def format_context_for_llm(
        self,
        context: Dict[str, Any],
        patient_id: Optional[str] = None
    ) -> str:
        """
        Format context as a readable string for LLM prompt
        
        CRITICAL: Only includes data that actually exists in the knowledge graph.
        Does not include empty or missing data.
        
        Args:
            context: Structured context dictionary
            patient_id: Optional patient ID for prompt formatting
            
        Returns:
            Formatted context string
        """
        lines = []
        
        # Check for errors first
        if context.get("error"):
            lines.append(f"ERROR: {context['error']}")
            lines.append("")
            lines.append("This patient has no clinical data in the knowledge graph.")
            lines.append("No similar patients can be found because the reference patient is an isolated node.")
            return "\n".join(lines)
        
        # Patient information
        if context.get("patient_data"):
            pd = context["patient_data"]
            lines.append("REFERENCE PATIENT INFORMATION:")
            if pd.get("subject_id"):
                lines.append(f"  Patient ID: {pd['subject_id']}")
            if pd.get("age"):
                lines.append(f"  Age: {pd['age']}")
            if pd.get("gender"):
                lines.append(f"  Gender: {pd['gender']}")
            if pd.get("total_admissions"):
                lines.append(f"  Total Admissions: {pd['total_admissions']}")
            lines.append("")
        
        # Diagnoses - Format as human-readable list
        if context.get("clinical_findings", {}).get("diagnoses"):
            lines.append("DIAGNOSES:")
            seen_diag = set()
            for diag in context["clinical_findings"]["diagnoses"]:
                diag_text = self._normalize_to_string(
                    diag.get("diagnosis") or diag.get("short_title") or diag.get("icd_code", "")
                )
                if diag_text and diag_text not in seen_diag:
                    lines.append(f"  - {diag_text}")
                    seen_diag.add(diag_text)
                    if len(seen_diag) >= 15:  # Limit to top 15 unique diagnoses
                        break
            lines.append("")
        
        # Medications - Format as human-readable list
        if context.get("clinical_findings", {}).get("medications"):
            lines.append("MEDICATIONS:")
            seen_med = set()
            for med in context["clinical_findings"]["medications"]:
                med_text = self._normalize_to_string(
                    med.get("medication") or med.get("medicines") or med.get("text", "")
                )
                if med_text and med_text not in seen_med:
                    lines.append(f"  - {med_text}")
                    seen_med.add(med_text)
                    if len(seen_med) >= 20:  # Limit to top 20 unique medications
                        break
            lines.append("")
        
        # Treatments/Procedures - Format as human-readable list
        if context.get("clinical_findings", {}).get("treatments"):
            lines.append("TREATMENTS/PROCEDURES:")
            seen_treat = set()
            for treat in context["clinical_findings"]["treatments"]:
                treat_text = self._normalize_to_string(
                    treat.get("procedure") or treat.get("short_title", "")
                )
                if treat_text and treat_text not in seen_treat:
                    lines.append(f"  - {treat_text}")
                    seen_treat.add(treat_text)
                    if len(seen_treat) >= 15:  # Limit to top 15 unique treatments
                        break
            lines.append("")
        
        # Lab Results
        if context.get("clinical_findings", {}).get("lab_results"):
            lines.append("LAB RESULTS:")
            for lab in context["clinical_findings"]["lab_results"][:5]:
                lab_text = self._normalize_to_string(lab.get("text")) or f"Value: {self._normalize_to_string(lab.get('value'))} {self._normalize_to_string(lab.get('unit', ''))}"
                lines.append(f"  - {lab_text}")
            lines.append("")
        
        # Reference Patient Medications (for comparison)
        ref_medications = context.get("clinical_findings", {}).get("medications", [])
        if ref_medications and patient_id:
            lines.append(f"REFERENCE PATIENT {patient_id} MEDICATIONS (for comparison):")
            for med in ref_medications[:15]:  # Show more medications for comparison
                med_text = self._normalize_to_string(
                    med.get("medication") or med.get("medicines") or med.get("text", "Unknown")
                )
                dose = med.get("dose") or med.get("dose_val_rx")
                unit = med.get("unit") or med.get("dose_unit_rx")
                if dose and unit:
                    lines.append(f"  - {med_text} ({dose} {unit})")
                else:
                    lines.append(f"  - {med_text}")
            lines.append("")
        
        # Similar Patients with detailed information
        similar_patients = context.get("similar_patients", [])
        if similar_patients:
            lines.append(f"SIMILAR PATIENTS (with full clinical profiles for comparison): {len(similar_patients)} found")
            lines.append("NOTE: Only patients with actual clinical data in the knowledge graph are included.")
            lines.append("")
            for sp in similar_patients[:10]:  # Show up to 10 similar patients
                lines.append(f"  Patient {sp.get('subject_id')} (Similarity: {sp.get('similarity_score', 0):.3f})")
                if sp.get('gender'):
                    lines.append(f"    - Gender: {sp.get('gender')}")
                if sp.get('age'):
                    lines.append(f"    - Age: {sp.get('age')}")
                
                # Diagnoses - Format as human-readable list
                diagnoses = sp.get('diagnoses', [])
                if diagnoses:
                    lines.append(f"    Diagnoses:")
                    # Filter out duplicates and format as list
                    seen_diag = set()
                    for diag in diagnoses:
                        diag_text = self._normalize_to_string(
                            diag.get('diagnosis') or diag.get('short_title') or diag.get('icd_code', '')
                        )
                        if diag_text and diag_text not in seen_diag:
                            lines.append(f"      - {diag_text}")
                            seen_diag.add(diag_text)
                            if len(seen_diag) >= 20:  # Limit to top 20 unique diagnoses
                                break
                else:
                    lines.append(f"    Diagnoses: Not available in the knowledge graph")
                
                # Medications - Format as human-readable list
                medications = sp.get('medications', [])
                if medications:
                    lines.append(f"    Medications:")
                    # Filter out duplicates and format as list
                    seen_med = set()
                    for med in medications:
                        med_text = self._normalize_to_string(
                            med.get('medication') or med.get('medicines', '')
                        )
                        if med_text and med_text not in seen_med:
                            lines.append(f"      - {med_text}")
                            seen_med.add(med_text)
                            if len(seen_med) >= 20:  # Limit to top 20 unique medications
                                break
                else:
                    lines.append(f"    Medications: Not available in the knowledge graph")
                
                # Treatments/Procedures - Format as human-readable list from patient journey
                treatments = sp.get('treatments', [])
                if treatments:
                    lines.append(f"    Treatments/Procedures:")
                    # Filter out duplicates and format as list
                    seen_treat = set()
                    for treat in treatments:
                        treat_text = self._normalize_to_string(
                            treat.get('procedure') or treat.get('short_title', '')
                        )
                        if treat_text and treat_text not in seen_treat:
                            lines.append(f"      - {treat_text}")
                            seen_treat.add(treat_text)
                            if len(seen_treat) >= 20:  # Limit to top 20 unique treatments
                                break
                else:
                    lines.append(f"    Treatments/Procedures: Not available in the knowledge graph")
                
                lines.append("")  # Empty line between patients
            lines.append("")
        else:
            lines.append("SIMILAR PATIENTS: None found")
            lines.append("")
            lines.append("No similar patients with clinical data were found in the knowledge graph.")
            lines.append("This may indicate:")
            lines.append("  - The reference patient has no connections in the graph (isolated node)")
            lines.append("  - Similar patients found by embeddings also have no clinical data")
            lines.append("  - The embeddings may not be capturing clinical diversity correctly")
            lines.append("")
        
        # Summary
        if context.get("summary"):
            summary = context["summary"]
            lines.append("SUMMARY:")
            lines.append(f"  Total Patients: {summary.get('total_patients', 0)}")
            lines.append(f"  Total Diagnoses: {summary.get('total_diagnoses', 0)}")
            lines.append(f"  Total Medications: {summary.get('total_medications', 0)}")
            lines.append(f"  Similar Patients with Data: {len(similar_patients)}")
            if summary.get("similarity_info"):
                lines.append(
                    f"  Top Similarity Score: {summary['similarity_info'].get('top_similarity_score', 0):.2f}"
                )
        
        lines.append("")
        lines.append("IMPORTANT: All information above is from the knowledge graph. Do not generate or infer data not shown here.")
        
        return "\n".join(lines)
    
    def to_json(self, context: Dict[str, Any]) -> str:
        """Convert context to JSON string"""
        return json.dumps(context, indent=2, default=str)

