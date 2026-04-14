"""
Patient comparison generation endpoint.
Ports logic from Scripts/Streamlit_Application/features/compare_patients.py
"""
import json
import os
import re
import time
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any

from services.neo4j_service import get_connection

router = APIRouter()
logger = logging.getLogger(__name__)


def _resolve_api_keys(keys_from_request: List[str]) -> List[str]:
    if keys_from_request:
        return keys_from_request
    env_keys = os.getenv("GEMINI_API_KEYS", "")
    if env_keys:
        parsed = [k.strip() for k in env_keys.split(",") if k.strip()]
        if parsed:
            return parsed
    single = os.getenv("GEMINI_API_KEY", "").strip()
    if single:
        return [single]
    return []


class ComparisonRequest(BaseModel):
    patient1_id: str
    patient2_id: str
    api_keys: List[str] = []


def _clean_json(s: str) -> str:
    s = re.sub(r",\s*}", "}", s)
    s = re.sub(r",\s*]", "]", s)
    return s


def _call_gemini_comparison(p1_data: Dict, p1_id: str, p2_data: Dict, p2_id: str, api_keys: List[str], retries: int = 3) -> Dict:
    p1_json = json.dumps(p1_data, indent=2, default=str)
    p2_json = json.dumps(p2_data, indent=2, default=str)

    prompt = f"""You are a medical comparison expert. Analyze two patient knowledge graphs and create a comprehensive comparison.

IMPORTANT OUTPUT FORMAT REQUIREMENTS:
- Return ONLY a valid JSON object (no markdown, no code blocks, no backticks)
- Start with {{ and end with }}
- No trailing commas

JSON structure:
{{
  "patient1_id": "string",
  "patient2_id": "string",
  "comparison_summary": "2-3 sentence overview (max 150 words)",
  "demographics_comparison": {{
    "similarities": ["Similarity 1"],
    "differences": ["Difference 1"]
  }},
  "presentation_comparison": {{
    "similarities": ["How they presented similarly"],
    "differences": ["How they presented differently"],
    "temporal_differences": "Comparison of timing/speed of presentation (max 100 words)"
  }},
  "diagnoses_comparison": {{
    "common_diagnoses": ["Shared diagnosis"],
    "unique_to_patient1": ["Patient 1 only diagnosis"],
    "unique_to_patient2": ["Patient 2 only diagnosis"],
    "severity_comparison": "Severity and complexity comparison (max 100 words)"
  }},
  "clinical_course_comparison": {{
    "similarities": ["Similar progression patterns"],
    "differences": ["Different progression patterns"],
    "temporal_sequence_comparison": "Chronological event sequence comparison (max 200 words)",
    "length_of_stay_comparison": "Hospital stay duration comparison (max 100 words)"
  }},
  "procedures_comparison": {{
    "common_procedures": ["Shared procedure"],
    "unique_to_patient1": ["Patient 1 only procedure"],
    "unique_to_patient2": ["Patient 2 only procedure"],
    "timing_comparison": "Procedure timing comparison (max 100 words)"
  }},
  "medications_comparison": {{
    "common_medications": ["Shared medication"],
    "unique_to_patient1": ["Patient 1 only medication"],
    "unique_to_patient2": ["Patient 2 only medication"],
    "timing_comparison": "Medication timing comparison (max 100 words)"
  }},
  "lab_findings_comparison": {{
    "similar_abnormalities": ["Shared abnormal lab"],
    "unique_abnormalities_patient1": ["Patient 1 only abnormal lab"],
    "unique_abnormalities_patient2": ["Patient 2 only abnormal lab"],
    "temporal_patterns": "Lab value change comparison over time (max 150 words)"
  }},
  "microbiology_comparison": {{
    "common_findings": ["Shared microbiology finding"],
    "unique_to_patient1": ["Patient 1 only finding"],
    "unique_to_patient2": ["Patient 2 only finding"]
  }},
  "outcomes_comparison": {{
    "discharge_comparison": "Discharge disposition/condition comparison (max 100 words)",
    "recovery_trajectory": "Recovery pattern comparison (max 100 words)",
    "key_differences": ["Key outcome difference 1"]
  }},
  "temporal_analysis": {{
    "event_sequence_comparison": "Side-by-side chronological event comparison (max 250 words)",
    "critical_timepoints": "Critical timepoints comparison (max 150 words)",
    "timing_patterns": "Temporal pattern analysis (max 150 words)"
  }},
  "clinical_insights": {{
    "why_similar": "Why these patients are similar (max 150 words)",
    "why_different": "Why these patients differ (max 150 words)",
    "lessons_learned": "Clinical lessons from comparison (max 150 words)"
  }}
}}

PATIENT 1 DATA:
{p1_json}

PATIENT 2 DATA:
{p2_json}

Return ONLY the JSON object:"""

    import google.generativeai as genai

    key_idx = 0
    tried = set()
    for attempt in range(retries):
        try:
            genai.configure(api_key=api_keys[key_idx])
            model = genai.GenerativeModel("gemini-2.5-pro")
            response = model.generate_content(prompt)
            text = response.text.strip()
            for prefix in ("```json", "```"):
                if text.startswith(prefix):
                    text = text[len(prefix):]
            if text.endswith("```"):
                text = text[:-3]
            return json.loads(_clean_json(text.strip()))
        except json.JSONDecodeError as e:
            logger.error("JSON decode error on attempt %d: %s", attempt, e)
            if attempt == retries - 1:
                raise HTTPException(status_code=500, detail=f"Failed to parse LLM response: {e}")
            time.sleep(3)
        except Exception as e:
            logger.error("Gemini error on attempt %d (key_idx=%d): %s", attempt, key_idx, e)
            err = str(e).lower()
            is_rate_limit = any(x in err for x in ["quota", "rate limit", "429", "resource exhausted"])
            if is_rate_limit:
                tried.add(key_idx)
                if len(tried) < len(api_keys):
                    key_idx = (key_idx + 1) % len(api_keys)
                    time.sleep(2)
                    continue
                raise HTTPException(status_code=429, detail=f"Rate limit hit: {e}")
            raise HTTPException(status_code=500, detail=f"Gemini API error: {e}")

    raise HTTPException(status_code=500, detail="Failed to generate comparison")


@router.post("/generate")
def generate_comparison(req: ComparisonRequest) -> Dict[str, Any]:
    if not req.patient1_id.strip().isdigit() or not req.patient2_id.strip().isdigit():
        raise HTTPException(status_code=400, detail="Patient IDs must be numeric")
    if req.patient1_id.strip() == req.patient2_id.strip():
        raise HTTPException(status_code=400, detail="Patient IDs must be different")
    api_keys = _resolve_api_keys(req.api_keys)
    if not api_keys:
        raise HTTPException(status_code=400, detail="No Gemini API key found. Set GEMINI_API_KEY in .env")

    conn = get_connection()

    for pid in [req.patient1_id, req.patient2_id]:
        check = conn.execute_query(
            "MATCH (p:Patient) WHERE p.subject_id = $sid OR toString(p.subject_id) = $sid RETURN p.subject_id AS id",
            {"sid": str(pid)},
        )
        if not check:
            raise HTTPException(status_code=404, detail=f"Patient {pid} not found")

    from load_data.retrieve_patient_kg import retrieve_patient_kg
    p1_data = retrieve_patient_kg(conn, req.patient1_id.strip())
    p2_data = retrieve_patient_kg(conn, req.patient2_id.strip())

    return _call_gemini_comparison(p1_data, req.patient1_id.strip(), p2_data, req.patient2_id.strip(), api_keys)
