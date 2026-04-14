"""
PDF download endpoints.
Reuses existing ReportLab PDF generators from the Streamlit application.
"""
import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime

router = APIRouter()
logger = logging.getLogger(__name__)


class SummaryDownloadRequest(BaseModel):
    patient_id: str
    summary_json: Dict[str, Any]


class ComparisonDownloadRequest(BaseModel):
    patient1_id: str
    patient2_id: str
    comparison_json: Dict[str, Any]


class JourneyDownloadRequest(BaseModel):
    patient_id: str
    journey_data: Dict[str, Any]


@router.post("/summary")
def download_summary_pdf(req: SummaryDownloadRequest) -> Response:
    try:
        from features.download_summarized_pdf import create_pdf_from_json
        pdf_bytes = create_pdf_from_json(req.summary_json)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="patient_{req.patient_id}_Summary.pdf"'},
        )
    except Exception as e:
        logger.error(f"Error generating summary PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {e}")


@router.post("/comparison")
def download_comparison_pdf(req: ComparisonDownloadRequest) -> Response:
    try:
        from features.download_comparison import create_pdf_from_comparison_json
        pdf_bytes = create_pdf_from_comparison_json(req.comparison_json)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="patient_{req.patient1_id}_vs_{req.patient2_id}_Comparison.pdf"'},
        )
    except Exception as e:
        logger.error(f"Error generating comparison PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {e}")


@router.post("/journey")
def download_journey_pdf(req: JourneyDownloadRequest) -> Response:
    try:
        from features.download_patient_journey import create_journey_pdf

        # Reconstruct datetime objects for timestamps (the PDF generator needs them)
        journey_data = dict(req.journey_data)
        events_with_dt = []
        for event in journey_data.get("events", []):
            event = dict(event)
            ts_str = event.get("timestamp", "")
            if ts_str:
                try:
                    if "T" in ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    else:
                        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    event["timestamp"] = ts
                except Exception:
                    pass
            events_with_dt.append(event)
        journey_data["events"] = events_with_dt

        pdf_bytes = create_journey_pdf(journey_data)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="patient_{req.patient_id}_Journey.pdf"'},
        )
    except Exception as e:
        logger.error(f"Error generating journey PDF: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {e}")


class DiagnosisSimilarityRequest(BaseModel):
    diagnosis_text: str
    top_k: int = 20


@router.post("/diagnosis-search")
def find_by_diagnosis(req: DiagnosisSimilarityRequest) -> Dict[str, Any]:
    """Find similar patients by diagnosis text using embeddings."""
    try:
        import re
        import numpy as np
        from services.neo4j_service import get_connection

        if not req.diagnosis_text.strip():
            raise HTTPException(status_code=400, detail="Diagnosis text is required")

        # Format diagnosis text
        diagnoses = re.split(r"[,\n;|]+", req.diagnosis_text)
        diagnoses = [d.strip() for d in diagnoses if d.strip()]
        formatted_text = " | ".join(diagnoses)

        # Remove stop words (optional, graceful fallback)
        try:
            import nltk
            try:
                from nltk.corpus import stopwords
                stop_words = set(stopwords.words("english"))
            except LookupError:
                nltk.download("stopwords", quiet=True)
                from nltk.corpus import stopwords
                stop_words = set(stopwords.words("english"))
            words = formatted_text.split()
            processed = " ".join(w for w in words if w.lower() not in stop_words)
            if processed.strip():
                formatted_text = processed
        except Exception:
            pass

        # Generate embedding
        from utils.config import Config
        try:
            from text_embeddings import TextEmbeddingGenerator
        except ImportError:
            raise HTTPException(status_code=500, detail="TextEmbeddingGenerator not available")

        config = Config()
        generator = TextEmbeddingGenerator(
            model_name=config.embedding.text_model_name,
            use_openai=getattr(config.embedding, "use_openai", False),
            use_gemini=getattr(config.embedding, "use_gemini", False),
        )
        embedding = generator.generate_embedding(formatted_text)
        embedding_list = embedding.tolist() if isinstance(embedding, np.ndarray) else embedding

        conn = get_connection()

        # Try vector index first
        try:
            q = """
            CALL db.index.vector.queryNodes('patient_diagnosis_index', $topK, $emb)
            YIELD node AS p, score
            WHERE p.diagnosis_embeddings IS NOT NULL
            WITH DISTINCT p, score ORDER BY score DESC LIMIT $topK
            RETURN p.subject_id AS patient_id, score AS similarity_score, p.all_diagnoses AS all_diagnoses
            """
            results = conn.execute_query(q, {"topK": req.top_k, "emb": embedding_list})
            if results:
                return {"results": [dict(r) for r in results]}
        except Exception:
            pass

        # Fallback
        fallback = """
        MATCH (p:Patient) WHERE p.diagnosis_embeddings IS NOT NULL
        WITH p, gds.similarity.cosine(p.diagnosis_embeddings, $emb) AS similarity_score
        WHERE similarity_score < 1.0 ORDER BY similarity_score DESC LIMIT $topK
        RETURN p.subject_id AS patient_id, similarity_score, p.all_diagnoses AS all_diagnoses
        """
        results = conn.execute_query(fallback, {"emb": embedding_list, "topK": req.top_k})
        return {"results": [dict(r) for r in results] if results else []}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in diagnosis search: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
