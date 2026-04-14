"""
Patient summary generation endpoint.
Ports logic from Scripts/Streamlit_Application/features/summarize_patient.py
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


class SummaryRequest(BaseModel):
    patient_id: str
    api_keys: List[str] = []


def _clean_json(s: str) -> str:
    s = re.sub(r",\s*}", "}", s)
    s = re.sub(r",\s*]", "]", s)
    return s


def _call_gemini(graph_data: Dict, patient_id: str, api_keys: List[str], retries: int = 3) -> Dict:
    graph_json = json.dumps(graph_data, indent=2, default=str)

    prompt = f"""You are a medical summarization expert. Analyze this Neo4j knowledge graph and generate a clinical summary.

IMPORTANT OUTPUT FORMAT REQUIREMENTS:
- Return ONLY a valid JSON object (no markdown, no code blocks, no backticks)
- Start with {{ and end with }}
- No trailing commas

JSON structure:
{{
  "patient_id": "string",
  "patient_demographics": {{
    "age": "string",
    "gender": "string",
    "race": "string",
    "total_admissions": "string"
  }},
  "executive_summary": "A 2-3 sentence overview (max 100 words)",
  "chief_complaints_and_presentation": "Description of presentation (max 150 words)",
  "clinical_course": "Detailed narrative of hospital stay (max 300 words)",
  "key_diagnoses": ["Primary diagnosis 1", "Primary diagnosis 2"],
  "significant_procedures": ["Procedure 1 with context", "Procedure 2 with context"],
  "medications": {{
    "started": ["medication1"],
    "stopped": ["medication2"],
    "to_avoid": ["medication3"]
  }},
  "key_lab_findings": ["Abnormal finding with value and context"],
  "microbiology_findings": ["Finding if present"],
  "discharge_summary": {{
    "disposition": "string",
    "condition": "string",
    "activity_status": "string",
    "follow_up_instructions": "Brief summary (max 150 words)"
  }},
  "clinical_significance": "Brief analysis (max 100 words)"
}}

KNOWLEDGE GRAPH DATA:
{graph_json}

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

    raise HTTPException(status_code=500, detail="Failed to generate summary")


@router.post("/generate")
def generate_summary(req: SummaryRequest) -> Dict[str, Any]:
    if not req.patient_id.strip().isdigit():
        raise HTTPException(status_code=400, detail="Patient ID must be numeric")
    api_keys = _resolve_api_keys(req.api_keys)
    if not api_keys:
        raise HTTPException(status_code=400, detail="No Gemini API key found. Set GEMINI_API_KEY in .env")

    conn = get_connection()

    # Verify patient exists
    check = conn.execute_query(
        "MATCH (p:Patient) WHERE p.subject_id = $sid OR toString(p.subject_id) = $sid RETURN p.subject_id AS id",
        {"sid": str(req.patient_id)},
    )
    if not check:
        raise HTTPException(status_code=404, detail=f"Patient {req.patient_id} not found")

    from load_data.retrieve_patient_kg import retrieve_patient_kg
    graph_data = retrieve_patient_kg(conn, req.patient_id.strip())
    summary_json = _call_gemini(graph_data, req.patient_id.strip(), api_keys)
    return summary_json
