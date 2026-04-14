"""
Patient similarity search endpoints (text embeddings).
Ports logic from Scripts/Streamlit_Application/features/patient_similarity.py
"""
import logging
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any

from services.neo4j_service import get_connection

router = APIRouter()
logger = logging.getLogger(__name__)

INDEX_NAMES = ["patient_text_index", "patient_embedding_index", "patient_journey_index"]


@router.get("/similarity/{patient_id}")
def find_similar_patients(
    patient_id: str,
    top_k: int = Query(default=20, ge=5, le=50),
) -> Dict[str, Any]:
    conn = get_connection()

    if not patient_id.strip().isdigit():
        raise HTTPException(status_code=400, detail="Patient ID must be numeric")

    # Verify patient exists and has embedding
    check_result = conn.execute_query(
        "MATCH (p:Patient {subject_id: $sid}) RETURN p.textEmbedding IS NOT NULL AS has_emb",
        {"sid": int(patient_id)},
    )
    if not check_result or not check_result[0].get("has_emb"):
        raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found or has no embedding")

    # Try vector indexes
    for idx in INDEX_NAMES:
        try:
            q = f"""
            MATCH (p:Patient {{subject_id: $sid}})
            WITH p.textEmbedding AS ref WHERE ref IS NOT NULL
            CALL db.index.vector.queryNodes('{idx}', $topK, ref)
            YIELD node AS sim, score
            WHERE sim.subject_id <> $sid AND score < 1.0
            WITH DISTINCT sim, score ORDER BY score DESC LIMIT $topK
            RETURN sim.subject_id AS patient_id, score AS similarity_score
            """
            results = conn.execute_query(q, {"sid": int(patient_id), "topK": top_k})
            if results:
                return {"patient_id": patient_id, "results": [dict(r) for r in results]}
        except Exception:
            continue

    # Fallback: GDS cosine
    try:
        fallback = """
        MATCH (p1:Patient {subject_id: $sid})
        MATCH (p2:Patient)
        WHERE p1.textEmbedding IS NOT NULL AND p2.textEmbedding IS NOT NULL AND p1.subject_id <> p2.subject_id
        WITH p2.subject_id AS patient_id,
             gds.similarity.cosine(p1.textEmbedding, p2.textEmbedding) AS similarity_score
        WHERE similarity_score < 1.0 ORDER BY similarity_score DESC LIMIT $topK
        RETURN patient_id, similarity_score
        """
        results = conn.execute_query(fallback, {"sid": int(patient_id), "topK": top_k})
        if results:
            return {"patient_id": patient_id, "results": [dict(r) for r in results]}
    except Exception as e:
        logger.warning(f"Fallback similarity search failed: {e}")

    return {"patient_id": patient_id, "results": []}
