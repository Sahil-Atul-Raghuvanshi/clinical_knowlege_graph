# Clinical Knowledge Graph - Complete Setup Guide

This guide covers end-to-end project setup on Windows, from installation to running pipelines and the app stack.

## 1) Prerequisites

Install these first:

- Python 3.11+ (3.13 works in this repo)
- Node.js 18+ and npm
- Git
- Neo4j 5.x (Desktop or Server)

Optional but recommended:

- A GPU-capable environment for faster embedding generation
- A Gemini API key for summary/comparison features

## 2) Clone and Open Project

```powershell
git clone https://github.com/Sahil-Atul-Raghuvanshi/clinical_knowlege_graph.git
cd clinical_knowlege_graph
```

## 3) Python Environment

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

Install backend dependencies:

```powershell
pip install -r .\backend\requirements.txt
```

Install pipeline/data dependencies (used by ETL + embeddings + Streamlit modules):

```powershell
pip install pandas tqdm sentence-transformers streamlit
```

If your run reports missing packages, install the missing module(s) and re-run.

## 4) Frontend Dependencies

```powershell
cd .\frontend
npm install
cd ..
```

## 5) Environment Variables (`.env`)

Create a root `.env` file (project root). Use this template:

```env
# Neo4j
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=clinicalknowledgegraph

# LLM keys (optional but needed for summary/comparison APIs)
GEMINI_API_KEY=your_gemini_key
# or comma-separated multiple keys:
# GEMINI_API_KEYS=key1,key2,key3
OPENAI_API_KEY=your_openai_key

# Optional tuning
EMBEDDING_DIMENSION=128
TEXT_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2
BATCH_SIZE=2000
VECTOR_INDEX_NAME=patient_journey_index
SIMILARITY_FUNCTION=cosine
MAX_RETRIES=3
CHECKPOINT_FREQUENCY=10
LOG_LEVEL=INFO
```

## 6) Neo4j Setup

1. Start Neo4j.
2. Ensure Bolt is enabled on `7687`.
3. Create/use database `clinicalknowledgegraph` (or match your `.env`).
4. Verify credentials match `.env`.

Quick connectivity check after backend starts:

- `GET http://127.0.0.1:8002/api/health`

Expected: `"status": "ok"` and Neo4j connected.

## 7) Data Folder Layout

The filtering script expects:

- `Complete_Data/hosp`, `Complete_Data/icu`, `Complete_Data/ed`, `Complete_Data/note`
- `1000_patients.csv` in project root

Filtering output goes to:

- `Filtered_Data/hosp`, `Filtered_Data/icu`, `Filtered_Data/ed`, `Filtered_Data/note`

## 8) Run Data Filtering (if needed)

```powershell
python .\filter_data.py
```

This creates `Filtered_Data` from `Complete_Data` using patient IDs from `1000_patients.csv`.

## 9) Run ETL + Embedding Pipelines

### Option A: Full orchestrator (recommended)

```powershell
python .\Scripts\Run_Pipeline\3_run_full_pipeline.py
```

Execution order:

1. `1_create_clinical_knowledge_graph_pipeline.py`
2. `2_create_embeddings_pipeline.py`

Logs are written to `logs/`.

### Option B: Run phases independently

```powershell
python .\Scripts\Run_Pipeline\1_create_clinical_knowledge_graph_pipeline.py
python .\Scripts\Run_Pipeline\2_create_embeddings_pipeline.py
```

## 10) Run Backend API

From project root:

```powershell
cd .\backend
python -m uvicorn main:app --host 127.0.0.1 --port 8002 --reload
```

Or use:

```powershell
.\start.bat
```

## 11) Run Frontend (Vite + React)

In another terminal:

```powershell
cd .\frontend
npm run dev
```

Frontend default URL:

- `http://localhost:5173`

## 12) Optional: Run Streamlit App

```powershell
cd .\Scripts\Streamlit_Application
streamlit run app.py
```

Or on Windows:

```powershell
.\launch_app.bat
```

## 13) Typical Local Dev Workflow

1. Activate venv
2. Start Neo4j
3. (If data changed) run `filter_data.py`
4. Run full pipeline
5. Start backend (`:8002`)
6. Start frontend (`:5173`)
7. Open app and test features

## 14) Validation Checklist

- Health endpoint returns connected Neo4j
- `Patient` nodes exist in Neo4j
- Embedding fields exist for expected patients
- Frontend can load patient similarity results
- Summary/comparison endpoints work when `GEMINI_API_KEY` is set

## 15) Troubleshooting

### Neo4j connection errors

- Re-check `NEO4J_URI`, username, password, database
- Make sure Neo4j is running and Bolt is enabled
- Try switching URI format between `neo4j://` and `bolt://` if needed

### Pipeline fails mid-run

- Check latest file in `logs/`
- Re-run the same command (ETL tracker supports incremental behavior)
- Confirm source CSVs exist and are readable

### Summary/comparison fails with API key error

- Set `GEMINI_API_KEY` (or `GEMINI_API_KEYS`) in `.env`
- Restart backend after changing `.env`

### Frontend cannot reach backend

- Ensure backend runs on `127.0.0.1:8002`
- Ensure frontend runs on one of allowed CORS origins (`5173/5174/5175`)

## 16) Security Notes

- Never commit `.env` to Git.
- Keep API keys private and rotate keys if exposed.
- Avoid pushing large/raw data folders unless explicitly required.

