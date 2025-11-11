# Hybrid Embedding System for Clinical Knowledge Graph

This is the new updated embedding system that uses a hybrid approach:
- **Neo4j**: Stores node-level embeddings (Patient, HospitalAdmission, Diagnosis)
- **Milvus**: Stores item-level embeddings (prescriptions, microbiology events, lab results)

## Architecture

### Components

1. **Enhanced Text Extractor** (`generators/enhanced_text_extractor.py`)
   - Extracts comprehensive text data using important node attributes
   - Formats text for embedding generation

2. **Text Embedding Generator** (`generators/text_embeddings.py`)
   - Generates text embeddings using SentenceTransformers, OpenAI, or Gemini
   - Supports batch processing

3. **Structural Embedding Generator** (`generators/structural_embeddings.py`)
   - Uses Neo4j GDS FastRP for structural embeddings
   - Captures graph topology and relationships

4. **Combined Embedding Generator** (`generators/combined_embeddings.py`)
   - Combines structural and textual embeddings
   - Supports concatenation or weighted sum methods

5. **Item Embedding Generator** (`generators/item_embeddings.py`)
   - Generates embeddings for individual items (medications, lab results, etc.)
   - Processes array fields into individual items

6. **Hybrid Storage** (`storage/hybrid_storage.py`)
   - Stores node-level embeddings in Neo4j
   - Stores item-level embeddings in Milvus
   - Manages vector indexes

7. **Main Pipeline** (`pipeline/embedding_pipeline.py`)
   - Orchestrates the entire embedding generation process
   - Handles batch processing for large datasets

## Usage

### Setup

1. Install dependencies:
```bash
pip install -r Scripts/Embeddings/requirements.txt
```

2. Ensure Milvus is running (via Docker):
```bash
docker-compose up -d milvus
```

3. Configure environment variables in `.env`:
```
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=admin123
NEO4J_DATABASE=clinicalknowledgegraph
MILVUS_HOST=localhost
MILVUS_PORT=19530
```

### Running the Pipeline

The pipeline is integrated into `Scripts/Run_Pipeline/2_create_embeddings_pipeline.py`:

```bash
# Full pipeline (all patients + all items)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch

# Test mode (5 patients)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode test

# Skip item embeddings (only patient embeddings)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch --skip-items

# Force regenerate item embeddings
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch --force-items
```

## Important Attributes Used

The system extracts and uses important attributes from nodes:

- **Patient**: gender, anchor_age, anchor_year_group, total_number_of_admissions
- **EmergencyDepartment**: period, disposition, arrival_transport
- **HospitalAdmission**: admission_type, admission_location, discharge_location, insurance, language, marital_status, race, service, chief_complaint, social_history, family_history
- **ICUStay**: careunit, period, service_given, first_careunit, last_careunit, los
- **Diagnosis**: ed_diagnosis, complete_diagnosis, primary_diagnoses, secondary_diagnoses
- **Prescription**: medicines
- **LabEvent**: lab_results, abnormal_results
- **MicrobiologyEvent**: micro_results
- And many more...

## Storage Strategy

### Neo4j (Node-Level)
- Patient embeddings (text + combined)
- HospitalAdmission embeddings
- Diagnosis embeddings
- Vector indexes for fast similarity search

### Milvus (Item-Level)
- Prescription items (20M+)
- Microbiology result items (4M+)
- Lab result items
- Diagnosis items

## Benefits

1. **Scalability**: Milvus handles billions of vectors efficiently
2. **Performance**: Specialized vector database for item-level search
3. **Flexibility**: Combined graph and vector queries
4. **Efficiency**: Optimal storage for different embedding types

