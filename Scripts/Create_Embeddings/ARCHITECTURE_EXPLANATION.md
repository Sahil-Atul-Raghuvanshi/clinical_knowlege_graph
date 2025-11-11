# Embedding Architecture Explanation

## Overview

This system uses a **hybrid embedding architecture** that creates embeddings at two different levels and stores them in specialized databases optimized for each use case.

---

## 🏗️ Architecture Components

### 1. **Embedding Generation Pipeline**

The system generates embeddings through a multi-stage process:

```
Patient Data (Neo4j Graph)
    ↓
[1] Structural Embeddings (GDS FastRP)
    ↓
[2] Text Embeddings (SentenceTransformers/OpenAI/Gemini)
    ↓
[3] Combined Embeddings (Structural + Text)
    ↓
[4] Item Embeddings (Individual items from arrays)
    ↓
Storage: Neo4j (node-level) + Milvus (item-level)
```

---

## 📊 Embedding Levels

### **Level 1: Node-Level Embeddings** (Stored in Neo4j)

**What are they?**
- Embeddings created for entire **graph nodes** (Patient, HospitalAdmission, Diagnosis, etc.)
- Each node gets a single embedding vector representing the entire node

**Types of Node-Level Embeddings:**

#### A. **Structural Embeddings** (128 dimensions)
- **Method**: Neo4j GDS FastRP (Fast Random Projection)
- **Purpose**: Captures graph topology and relationships
- **What it learns**: 
  - How nodes are connected in the graph
  - Structural patterns (e.g., "patients with similar admission patterns")
  - Graph-based similarity

#### B. **Text Embeddings** (384 dimensions)
- **Method**: SentenceTransformers (`all-MiniLM-L6-v2`) or OpenAI/Gemini
- **Purpose**: Captures semantic meaning from text attributes
- **What it learns**:
  - Clinical text semantics (diagnoses, medications, notes)
  - Patient attributes (demographics, history)
  - Medical terminology relationships

#### C. **Combined Embeddings** (512 dimensions = 128 + 384)
- **Method**: Concatenation or weighted sum of structural + text embeddings
- **Purpose**: Holistic patient representation combining structure and semantics
- **Stored as**: `combinedEmbedding` property on Patient nodes

**Node Types with Embeddings:**
- ✅ **Patient** nodes: `textEmbedding` (384D) + `combinedEmbedding` (512D)
- ✅ **Diagnosis** nodes: Structural embeddings
- ✅ **Prescription** nodes: Structural embeddings
- ✅ **DischargeClinicalNote** nodes: Structural embeddings

---

### **Level 2: Item-Level Embeddings** (Stored in Milvus)

**What are they?**
- Embeddings created for **individual items** extracted from array fields
- Each item in an array gets its own embedding

**Item Types:**

#### A. **Medication Items** (384 dimensions)
- Extracted from: `Prescription.medicines[]`, `PreviousPrescriptionMeds.medications[]`, etc.
- Example: Each medication name gets its own embedding
- **Collection**: `prescription_items` in Milvus
- **Volume**: 20M+ items

#### B. **Lab Result Items** (384 dimensions)
- Extracted from: `LabEvent.lab_results[]`
- Example: Each lab test result gets its own embedding
- **Collection**: `lab_result_items` in Milvus

#### C. **Microbiology Result Items** (384 dimensions)
- Extracted from: `MicrobiologyEvent.micro_results[]`
- Example: Each microbiology test result gets its own embedding
- **Collection**: `microbiology_items` in Milvus
- **Volume**: 4M+ items

#### D. **Diagnosis Items** (384 dimensions)
- Extracted from: `Diagnosis.primary_diagnoses[]`, `Diagnosis.secondary_diagnoses[]`
- Example: Each diagnosis code/name gets its own embedding
- **Collection**: `diagnosis_items` in Milvus

**Why Item-Level?**
- Enables **fine-grained similarity search** (e.g., "find patients with similar medications")
- Handles large-scale item data (millions of items)
- Optimized for vector similarity queries

---

## 💾 Storage Architecture

### **Neo4j** (Node-Level Storage)

**Stores:**
- Patient node embeddings:
  - `Patient.textEmbedding` (384D) - for semantic queries
  - `Patient.combinedEmbedding` (512D) - for patient-to-patient similarity
- Structural embeddings for multi-node types (temporary, then cleaned up)

**Vector Indexes:**
- `patient_journey_index`: Index on `combinedEmbedding` (512D) for patient similarity
- `patient_text_index`: Index on `textEmbedding` (384D) for semantic search
- `diagnosis_item_embedding_index`: Index on `DiagnosisItem.embedding` (384D)
- `medication_item_embedding_index`: Index on `MedicationItem.embedding` (384D)
- `lab_result_item_embedding_index`: Index on `LabResultItem.embedding` (384D)
- `microbiology_result_item_embedding_index`: Index on `MicrobiologyResultItem.embedding` (384D)

**Why Neo4j?**
- Native graph queries (combine embedding search with graph traversal)
- Integrated vector search (Neo4j vector indexes)
- Relationship-aware queries

---

### **Milvus** (Item-Level Storage)

**Stores:**
- Individual item embeddings in separate collections:
  - `prescription_items` (20M+ items)
  - `microbiology_items` (4M+ items)
  - `lab_result_items`
  - `diagnosis_items`

**Index Type:** HNSW (Hierarchical Navigable Small World)
**Similarity Metric:** Cosine similarity

**Why Milvus?**
- Optimized for **billions of vectors**
- Fast approximate nearest neighbor search
- Handles high-dimensional vectors efficiently
- Specialized vector database (better than storing in Neo4j for large-scale items)

---

## 🔄 Embedding Generation Process

### **Step 1: Structural Embeddings (GDS FastRP)**

```python
# Creates graph projection
gds.graph.project(
    graph_name="patient_journey_graph",
    node_labels=['Patient', 'Diagnosis', 'Prescription', ...],
    relationship_types=['VISITED_ED', 'RECORDED_DIAGNOSES', ...]
)

# Generates FastRP embeddings (one-time for ALL nodes)
gds.fastRP.write(
    embeddingDimension=128,
    iterationWeights=[0.0, 1.0],
    normalizationStrength=0.0
)
```

**Time**: 10-20 hours for 364K patients (one-time computation)

---

### **Step 2: Text Embeddings**

```python
# Extract text data from patient subgraph
text_data = extractor.extract_patient_text_data(patient_id)
# Includes: demographics, diagnoses, medications, notes, etc.

# Generate text embedding
text_embedding = text_generator.generate_patient_text_embedding(text_data)
# Dimension: 384 (all-MiniLM-L6-v2)
```

**Processed in batches** (default: 2000 patients per batch)

---

### **Step 3: Combined Embeddings**

```python
# Combine structural + text embeddings
combined = concatenate(structural_embedding, text_embedding)
# Dimension: 128 + 384 = 512
```

**Stored in Neo4j** as `Patient.combinedEmbedding`

---

### **Step 4: Item Embeddings**

```python
# Extract items from array fields
prescription_items = extract_items_from_array(prescription.medicines)

# Generate embedding for each item
for item in prescription_items:
    item_embedding = item_generator.generate_item_embedding(item)
    # Dimension: 384

# Store in Milvus
milvus.insert(prescription_items)
```

**Processed per item type** (Prescriptions, Lab Results, Microbiology, Diagnoses)

---

## 🎯 Use Cases & Benefits

### **1. Patient Similarity Search**

**Query**: "Find patients similar to patient X"

**How it works:**
```cypher
// Using combined embedding (512D) - captures both structure and semantics
MATCH (p:Patient {subject_id: $patient_id})
WITH p.combinedEmbedding AS query_embedding
MATCH (similar:Patient)
WHERE similar.combinedEmbedding IS NOT NULL
RETURN similar, 
       vector.similarity.cosine(similar.combinedEmbedding, query_embedding) AS similarity
ORDER BY similarity DESC
LIMIT 10
```

**Benefits:**
- Find patients with similar clinical journeys
- Identify cohorts for research
- Clinical decision support

---

### **2. Semantic Search**

**Query**: "Find patients with heart conditions"

**How it works:**
```cypher
// Using text embedding (384D) - captures semantic meaning
MATCH (p:Patient)
WHERE p.textEmbedding IS NOT NULL
WITH p, 
     vector.similarity.cosine(
         p.textEmbedding, 
         $query_embedding  // Embedding of "heart conditions"
     ) AS similarity
WHERE similarity > 0.7
RETURN p, similarity
ORDER BY similarity DESC
```

**Benefits:**
- Natural language queries
- Concept-based search (not just keyword matching)
- Understands medical terminology relationships

---

### **3. Item-Level Similarity Search**

**Query**: "Find patients with medications similar to 'Aspirin'"

**How it works:**
```python
# 1. Get embedding for "Aspirin"
aspirin_embedding = item_generator.generate_item_embedding("Aspirin")

# 2. Search Milvus for similar medications
similar_items = milvus.search(
    collection="prescription_items",
    query_vector=aspirin_embedding,
    top_k=100
)

# 3. Get source node IDs and find patients
patient_ids = [item['source_node_id'] for item in similar_items]

# 4. Query Neo4j for patients
MATCH (p:Patient)-[:HAS_PRESCRIPTION]->(presc:Prescription)
WHERE id(presc) IN $prescription_ids
RETURN DISTINCT p
```

**Benefits:**
- Fine-grained medication similarity
- Find alternative medications
- Drug interaction analysis

---

### **4. Hybrid Queries (Graph + Vector)**

**Query**: "Find patients similar to patient X who also had ICU stays"

**How it works:**
```cypher
// Combine vector similarity with graph traversal
MATCH (p:Patient {subject_id: $patient_id})
WITH p.combinedEmbedding AS query_embedding
MATCH (similar:Patient)-[:HAD_ICU_STAY]->(icu:ICUStay)
WHERE similar.combinedEmbedding IS NOT NULL
WITH similar, 
     vector.similarity.cosine(similar.combinedEmbedding, query_embedding) AS similarity
WHERE similarity > 0.8
RETURN similar, similarity, icu
ORDER BY similarity DESC
```

**Benefits:**
- Leverage both graph structure and semantic similarity
- Complex multi-hop queries
- Relationship-aware similarity

---

### **5. Clinical Research & Analytics**

**Use Cases:**
- **Cohort Discovery**: Find patient groups with similar characteristics
- **Outcome Prediction**: Use embeddings as features for ML models
- **Drug Repurposing**: Find similar medications using item embeddings
- **Clinical Trial Matching**: Match patients to trials based on embeddings

---

## 📈 Performance Characteristics

### **Node-Level (Neo4j)**
- **Storage**: ~512 floats per patient = 2KB per patient
- **Index**: Vector indexes for fast similarity search
- **Query Time**: <100ms for top-10 similar patients
- **Scale**: Optimized for 100K-1M patients

### **Item-Level (Milvus)**
- **Storage**: ~384 floats per item = 1.5KB per item
- **Index**: HNSW index for approximate nearest neighbor
- **Query Time**: <50ms for top-100 similar items
- **Scale**: Handles billions of items efficiently

---

## 🔧 Configuration

### **Embedding Dimensions**
- Structural: 128D (FastRP)
- Text: 384D (all-MiniLM-L6-v2) or 1536D (OpenAI) or 768D (Gemini)
- Combined: 512D (128 + 384) or varies based on combination method
- Items: 384D (all-MiniLM-L6-v2)

### **Batch Sizes**
- Patient processing: 2000 patients per batch
- Item insertion: 5000 items per batch
- Text embedding: Processed in batches for efficiency

---

## 🚀 Running the Pipeline

```bash
# Full pipeline (all patients + all items)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch

# Only patient embeddings (skip items)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch --skip-items

# Only item embeddings
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode batch --only-items

# Test mode (5 patients)
python Scripts/Run_Pipeline/2_create_embeddings_pipeline.py --mode test
```

---

## 📝 Summary

| Aspect | Node-Level | Item-Level |
|--------|------------|------------|
| **Storage** | Neo4j | Milvus |
| **Granularity** | Entire nodes | Individual items |
| **Dimensions** | 384D (text) / 512D (combined) | 384D |
| **Use Case** | Patient similarity, semantic search | Item similarity, fine-grained search |
| **Scale** | 100K-1M nodes | Billions of items |
| **Query Type** | Graph + Vector hybrid | Vector similarity |

**Key Insight**: The hybrid approach optimizes for both **graph-aware node-level queries** (Neo4j) and **large-scale item-level similarity search** (Milvus), providing the best of both worlds for clinical knowledge graph applications.

