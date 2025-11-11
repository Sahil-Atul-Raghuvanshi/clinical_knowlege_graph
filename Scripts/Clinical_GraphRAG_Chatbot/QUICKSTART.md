# Quick Start Guide

## Prerequisites

1. **Neo4j** running and accessible
   - Default: `neo4j://127.0.0.1:7687`
   - Database: `clinicalknowledgegraph`
   - Ensure embeddings are created and stored

2. **Milvus** running and accessible
   - Default: `localhost:19530`
   - Collections should exist: `prescription_items`, `lab_result_items`, `diagnosis_items`, `microbiology_items`

3. **Gemini API Key**
   - Get your API key from: https://makersuite.google.com/app/apikey
   - Set it in environment variable or `.env` file

## Setup Steps

### 1. Install Dependencies

```bash
cd Scripts/Clinical_GraphRAG_Chatbot
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the `Clinical_GraphRAG_Chatbot` directory:

```env
# Neo4j Configuration
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=admin123
NEO4J_DATABASE=clinicalknowledgegraph

# Milvus Configuration
MILVUS_HOST=localhost
MILVUS_PORT=19530
MILVUS_ALIAS=default

# Gemini API Configuration
GEMINI_API_KEY=your_gemini_api_key_here

# Embedding Model
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
```

### 3. Update Configuration Files (if needed)

Edit `config/neo4j_config.json` and `config/milvus_config.json` if your setup differs from defaults.

### 4. Run the Chatbot

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Testing

Run the test script to verify everything works:

```bash
python test_chatbot.py
```

## Example Queries

### Patient Similarity
- "Find patients similar to patient 100045"
- "Find patients with similar diagnoses to patient 100012"

### Treatment Recommendations
- "What treatments worked best for cirrhosis?"
- "What medications are commonly prescribed for sepsis?"

### Clinical Summaries
- "Summarize the last 3 admissions for patient 100023"
- "What is the patient journey for patient 100045?"

## Troubleshooting

### "Connection Error: Neo4j"
- Verify Neo4j is running: `docker ps | grep neo4j`
- Check credentials in config
- Ensure database exists

### "Connection Error: Milvus"
- Verify Milvus is running: `docker ps | grep milvus`
- Check port 19530 is accessible
- Ensure collections are loaded

### "LLM module is not available"
- Check `GEMINI_API_KEY` is set correctly
- Verify API key is valid
- Check API quota

### "No embedding found for patient"
- Ensure embeddings have been generated
- Check that `combined_embedding` property exists on Patient nodes
- Verify vector index exists (optional, fallback will be used)

## Architecture Overview

```
User Query
    ↓
Intent Detection (similarity/treatment/summary)
    ↓
Hybrid Retrieval
    ├── Graph Traversal (Neo4j)
    │   └── Patient journeys, diagnoses, medications
    └── Semantic Search (Milvus)
        └── Similar items (medications, labs, diagnoses)
    ↓
Context Builder
    └── Structured JSON context
    ↓
LLM (Gemini)
    └── Natural language answer
    ↓
Response + Context
```

## Next Steps

1. Customize prompt templates in `prompts/` directory
2. Adjust retrieval parameters in `hybrid_retriever.py`
3. Add graph visualization using PyVis (optional)
4. Extend intent detection for more query types

