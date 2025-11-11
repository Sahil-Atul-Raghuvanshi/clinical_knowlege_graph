# Clinical GraphRAG Chatbot

An intelligent chatbot that integrates **Neo4j, Milvus, and Google Gemini** to answer clinical questions using data stored in a Clinical Knowledge Graph (CKG).

## Features

- 🔍 **Hybrid Retrieval**: Combines graph traversal (Neo4j) and semantic similarity search (Milvus)
- 🧠 **Intent Detection**: Automatically detects query type (similarity, treatment, summary)
- 💬 **Natural Language Answers**: Uses Gemini to generate evidence-based responses
- 📊 **Context Visualization**: Displays retrieved data and similar patients
- 🎯 **Patient Similarity**: Finds similar patients using vector embeddings

## Architecture

```
User Query
    ↓
Intent Detection
    ↓
Hybrid Retrieval
    ├── Graph Traversal (Neo4j)
    └── Semantic Search (Milvus)
    ↓
Context Builder
    ↓
LLM (Gemini)
    ↓
Answer + Context
```

## Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   - Copy `.env.example` to `.env` (or create it manually)
   - Set your Gemini API key:
     ```
     GEMINI_API_KEY=your_api_key_here
     ```

3. **Ensure services are running:**
   - Neo4j (default: `neo4j://127.0.0.1:7687`)
   - Milvus (default: `localhost:19530`)

## Usage

### Run the Streamlit app:

```bash
streamlit run app.py
```

### Example Queries

- **Patient Similarity:**
  - "Find patients similar to patient 100045"
  - "Find patients with similar diagnoses to patient 100012"

- **Treatment Recommendations:**
  - "What treatments worked best for cirrhosis?"
  - "What medications are commonly prescribed for sepsis?"

- **Clinical Summaries:**
  - "Summarize the last 3 admissions for patient 100023"
  - "What is the patient journey for patient 100045?"

## Project Structure

```
Clinical_GraphRAG_Chatbot/
├── app.py                      # Streamlit frontend
├── query_processor.py          # Main orchestrator
├── graph_retriever.py          # Neo4j graph queries
├── vector_retriever.py         # Milvus semantic search
├── hybrid_retriever.py         # Merge graph + vector results
├── context_builder.py          # Structure context for LLM
├── intent_detector.py          # Query intent detection
├── llm_module.py               # Gemini integration
├── prompts/                    # LLM prompt templates
│   ├── patient_similarity.txt
│   ├── treatment_recommendation.txt
│   ├── summary.txt
│   └── general.txt
├── config/                     # Configuration files
│   ├── neo4j_config.json
│   └── milvus_config.json
└── requirements.txt
```

## Configuration

### Neo4j Configuration

Edit `config/neo4j_config.json`:
```json
{
  "uri": "neo4j://127.0.0.1:7687",
  "username": "neo4j",
  "password": "your_password",
  "database": "clinicalknowledgegraph"
}
```

### Milvus Configuration

Edit `config/milvus_config.json`:
```json
{
  "host": "localhost",
  "port": 19530,
  "alias": "default",
  "collections": {
    "prescription_items": "prescription_items",
    "microbiology_items": "microbiology_items",
    "lab_result_items": "lab_result_items",
    "diagnosis_items": "diagnosis_items"
  }
}
```

## Key Components

### 1. Intent Detector
Detects query type:
- Patient similarity
- Treatment recommendation
- Clinical summary
- General query

### 2. Graph Retriever
Performs Cypher queries on Neo4j:
- Patient journey retrieval
- Similar patient search (using embeddings)
- Condition-based patient search
- Treatment outcome analysis

### 3. Vector Retriever
Searches Milvus collections:
- Medication items
- Lab result items
- Diagnosis items
- Microbiology items

### 4. Hybrid Retriever
Combines and ranks results from both sources with weighted scoring.

### 5. Context Builder
Structures retrieved data into JSON format for LLM consumption.

### 6. LLM Module
Integrates with Google Gemini API to generate natural language answers.

## Troubleshooting

### Connection Issues

**Neo4j:**
- Verify Neo4j is running: `docker ps | grep neo4j`
- Check connection credentials in config

**Milvus:**
- Verify Milvus is running: `docker ps | grep milvus`
- Check port 19530 is accessible

**Gemini API:**
- Verify API key is set in `.env` file
- Check API key is valid and has quota

### Common Errors

1. **"No embedding found for patient"**
   - Ensure embeddings have been generated and stored in Neo4j

2. **"Collection not found"**
   - Verify Milvus collections exist and are loaded

3. **"LLM module is not available"**
   - Check Gemini API key configuration

## License

This project is part of the Clinical Knowledge Graph system.

