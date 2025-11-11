"""
Intent Detection Module
Detects the type of clinical query to route to appropriate retrieval strategies
"""
import re
import logging
from typing import Dict, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class QueryIntent(Enum):
    """Types of clinical queries"""
    PATIENT_SIMILARITY = "patient_similarity"
    ALL_SIMILAR_PAIRS = "all_similar_pairs"
    TREATMENT_RECOMMENDATION = "treatment_recommendation"
    CLINICAL_SUMMARY = "clinical_summary"
    GENERAL = "general"


class IntentDetector:
    """Detects intent from natural language clinical queries"""
    
    def __init__(self):
        """Initialize intent detector with keyword patterns"""
        # Similarity keywords
        self.similarity_keywords = [
            r'similar to',
            r'like patient',
            r'closest to',
            r'most similar',
            r'patients like',
            r'comparable to',
            r'similar patients',
            r'find similar',
            r'patients with similar'
        ]
        
        # All similar pairs keywords (for finding all pairs, not just for one patient)
        self.all_pairs_keywords = [
            r'all similar.*pair',
            r'all.*similar.*patient',
            r'similar.*each other',
            r'patients.*similar.*each other',
            r'show.*all.*similar',
            r'find.*all.*similar',
            r'all.*patient.*pair',
            r'similar.*pair',
            r'cluster.*patient'
        ]
        
        # Treatment keywords
        self.treatment_keywords = [
            r'treatment',
            r'medication',
            r'prescription',
            r'therapy',
            r'intervention',
            r'best treatment',
            r'worked best',
            r'effective',
            r'recommend',
            r'outcome',
            r'response to'
        ]
        
        # Summary keywords
        self.summary_keywords = [
            r'summarize',
            r'summary',
            r'journey',
            r'history',
            r'timeline',
            r'overview',
            r'last.*admission',
            r'recent.*visit',
            r'patient.*story'
        ]
        
    def extract_patient_id(self, query: str) -> Optional[str]:
        """
        Extract patient/subject ID from query
        
        Args:
            query: User query text
            
        Returns:
            Patient ID if found, None otherwise
        """
        # Pattern to match patient IDs (numbers, possibly with "patient" prefix)
        patterns = [
            r'patient\s+(\d+)',
            r'subject\s+(\d+)',
            r'patient\s+id\s+(\d+)',
            r'subject\s+id\s+(\d+)',
            r'(\d{6,})',  # 6+ digit numbers (likely patient IDs)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(1) if match.lastindex else match.group(0)
        
        return None
    
    def extract_condition(self, query: str) -> Optional[str]:
        """
        Extract medical condition from query dynamically
        
        This method uses pattern matching to extract medical conditions from queries.
        It looks for common medical terminology patterns and condition mentions.
        
        Args:
            query: User query text
            
        Returns:
            Condition name if found, None otherwise
        """
        query_lower = query.lower()
        
        # Common medical condition patterns (expanded list for better matching)
        # These are used as fallback if pattern matching doesn't work
        common_conditions = [
            'cirrhosis', 'sepsis', 'diabetes', 'hypertension',
            'pneumonia', 'heart failure', 'copd', 'asthma',
            'kidney disease', 'liver failure', 'stroke', 'mi',
            'myocardial infarction', 'cancer', 'tumor', 'hiv',
            'hepatitis', 'depression', 'anxiety', 'schizophrenia',
            'bipolar', 'epilepsy', 'migraine', 'arthritis',
            'osteoporosis', 'anemia', 'leukemia', 'lymphoma'
        ]
        
        # Pattern 1: Look for condition after keywords like "with", "suffering from", "diagnosed with"
        condition_patterns = [
            r'(?:with|suffering from|diagnosed with|having|has|had)\s+([a-z\s]+?)(?:\s|,|\.|$)',
            r'condition[:\s]+([a-z\s]+?)(?:\s|,|\.|$)',
            r'disease[:\s]+([a-z\s]+?)(?:\s|,|\.|$)',
            r'diagnosis[:\s]+([a-z\s]+?)(?:\s|,|\.|$)',
        ]
        
        for pattern in condition_patterns:
            match = re.search(pattern, query_lower)
            if match:
                condition = match.group(1).strip()
                # Clean up common stop words
                condition = re.sub(r'\b(patient|the|a|an|is|are|was|were)\b', '', condition).strip()
                if condition and len(condition) > 2:  # Valid condition should be at least 3 chars
                    return condition
        
        # Pattern 2: Look for common medical abbreviations/acronyms
        medical_abbreviations = {
            'mi': 'myocardial infarction',
            'copd': 'chronic obstructive pulmonary disease',
            'chf': 'congestive heart failure',
            'cad': 'coronary artery disease',
            'dm': 'diabetes mellitus',
            'htn': 'hypertension',
            'aki': 'acute kidney injury',
            'ckd': 'chronic kidney disease',
            'ards': 'acute respiratory distress syndrome',
            'pe': 'pulmonary embolism',
            'dvt': 'deep vein thrombosis'
        }
        
        for abbrev, full_name in medical_abbreviations.items():
            # Match whole word abbreviation
            if re.search(r'\b' + abbrev + r'\b', query_lower):
                return full_name
        
        # Pattern 3: Check against common conditions list (fallback)
        for condition in common_conditions:
            if condition in query_lower:
                return condition
        
        # Pattern 4: Try to extract condition from query structure
        # Look for phrases that might indicate a condition
        # Example: "patients with X" or "treatment for X"
        phrase_patterns = [
            r'patients?\s+with\s+([a-z\s]+?)(?:\s|,|\.|$)',
            r'treatment\s+for\s+([a-z\s]+?)(?:\s|,|\.|$)',
            r'medication\s+for\s+([a-z\s]+?)(?:\s|,|\.|$)',
            r'best\s+treatment\s+for\s+([a-z\s]+?)(?:\s|,|\.|$)',
        ]
        
        for pattern in phrase_patterns:
            match = re.search(pattern, query_lower)
            if match:
                condition = match.group(1).strip()
                # Remove common words
                condition = re.sub(r'\b(patient|the|a|an|is|are|was|were|worked|best)\b', '', condition).strip()
                if condition and len(condition) > 2:
                    return condition
        
        return None
    
    def detect_intent(self, query: str) -> Tuple[QueryIntent, Dict[str, any]]:
        """
        Detect query intent and extract relevant entities
        
        This method extracts entities (patient_id, condition) dynamically from the query.
        The condition extraction is fully dynamic and can extract any medical condition
        mentioned in the query, not just from a predefined list.
        
        Args:
            query: User query text
            
        Returns:
            Tuple of (intent, extracted_entities)
            
        Example entities:
            {
                "patient_id": "10002930",
                "condition": "mi"  # or "myocardial infarction" - dynamically extracted
            }
        """
        query_lower = query.lower()
        entities = {}
        
        # Extract patient ID
        patient_id = self.extract_patient_id(query)
        if patient_id:
            entities['patient_id'] = patient_id
        
        # Extract condition dynamically from query
        # This will extract any condition mentioned, not just predefined ones
        condition = self.extract_condition(query)
        if condition:
            entities['condition'] = condition
            logger.info(f"Extracted condition dynamically: {condition}")
        
        # Check for all similar pairs intent first (more specific)
        for pattern in self.all_pairs_keywords:
            if re.search(pattern, query_lower):
                logger.info(f"Detected ALL_SIMILAR_PAIRS intent for query: {query[:50]}...")
                return QueryIntent.ALL_SIMILAR_PAIRS, entities
        
        # Check for similarity intent (for specific patient)
        for pattern in self.similarity_keywords:
            if re.search(pattern, query_lower):
                logger.info(f"Detected PATIENT_SIMILARITY intent for query: {query[:50]}...")
                return QueryIntent.PATIENT_SIMILARITY, entities
        
        # Check for treatment intent
        for pattern in self.treatment_keywords:
            if re.search(pattern, query_lower):
                logger.info(f"Detected TREATMENT_RECOMMENDATION intent for query: {query[:50]}...")
                return QueryIntent.TREATMENT_RECOMMENDATION, entities
        
        # Check for summary intent
        for pattern in self.summary_keywords:
            if re.search(pattern, query_lower):
                logger.info(f"Detected CLINICAL_SUMMARY intent for query: {query[:50]}...")
                return QueryIntent.CLINICAL_SUMMARY, entities
        
        # Default to general
        logger.info(f"Detected GENERAL intent for query: {query[:50]}...")
        return QueryIntent.GENERAL, entities

