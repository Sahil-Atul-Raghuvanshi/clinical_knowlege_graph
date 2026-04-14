"""
Clinical Knowledge Graph - FastAPI Backend
Serves all patient analysis features via REST API
"""
import sys
import os
import logging
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "Scripts"
STREAMLIT_APP_DIR = SCRIPTS_DIR / "Streamlit_Application"
CREATE_EMBEDDINGS_DIR = SCRIPTS_DIR / "Create_Embeddings" / "full_patient_embeddings"

for p in [str(SCRIPTS_DIR), str(STREAMLIT_APP_DIR), str(CREATE_EMBEDDINGS_DIR)]:
    if p not in sys.path:
        sys.path.insert(0, p)

load_dotenv(PROJECT_ROOT / ".env")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logging.getLogger("neo4j").setLevel(logging.ERROR)
logging.getLogger("routers.summary").setLevel(logging.ERROR)
logging.getLogger("routers.comparison").setLevel(logging.ERROR)

# ── App ───────────────────────────────────────────────────────────────────────
from routers import patients, summary, comparison, journey, downloads  # noqa: E402

app = FastAPI(
    title="Clinical Knowledge Graph API",
    description="REST API for patient analysis features",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:5174", "http://localhost:5175", "http://127.0.0.1:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(patients.router, prefix="/api/patients", tags=["Patients"])
app.include_router(summary.router, prefix="/api/summary", tags=["Summary"])
app.include_router(comparison.router, prefix="/api/comparison", tags=["Comparison"])
app.include_router(journey.router, prefix="/api/journey", tags=["Journey"])
app.include_router(downloads.router, prefix="/api/downloads", tags=["Downloads"])


def _check_neo4j() -> str:
    """Returns 'connected' or 'disconnected: <reason>'."""
    try:
        from services.neo4j_service import get_connection
        conn = get_connection()
        conn.execute_query("RETURN 1", {})
        return "connected"
    except Exception as e:
        return f"disconnected: {e}"


@app.get("/api/health")
def health_check():
    return {"status": "ok", "neo4j": _check_neo4j()}


