# Patient Similarity Search Application

A simple application to find similar patients using Neo4j vector similarity search on patient text embeddings.

## Features

- 🔍 **Patient Similarity Search**: Find similar patients based on their clinical profiles
- 📊 **Similarity Scores**: View similarity scores for each patient
- 🎯 **Vector Search**: Uses Neo4j vector index for fast similarity search
- 📈 **Interactive Table**: Display results in an easy-to-read table format

## Architecture

```
Patient ID Input
    ↓
Neo4j Vector Similarity Search
    ↓
Similar Patients Table
    (Patient ID, Similarity Score)
```

## Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   - Ensure your `.env` file has Neo4j configuration:
     ```
     NEO4J_URI=neo4j://127.0.0.1:7687
     NEO4J_USERNAME=neo4j
     NEO4J_PASSWORD=your_password
     NEO4J_DATABASE=clinicalknowledgegraph
     ```

3. **Ensure Neo4j is running:**
   - Neo4j (default: `neo4j://127.0.0.1:7687`)
   - Patient embeddings must be generated and stored in Neo4j

## Usage

### Method 1: Command Line (Standard)
Run the Streamlit application from the command line:

```bash
streamlit run app.py
```

### Method 2: Double-Click Launcher (Windows)
**For Windows users:** Simply double-click `launch_app.bat` to launch the app.

### Method 3: Shell Script (Linux/Mac)
Make the script executable and run it:
```bash
chmod +x launch_app.sh
./launch_app.sh
```

The app will open in your browser at `http://localhost:8501`

### How to Use

1. Enter a patient ID in the input field
2. Select the number of similar patients to show (5-50)
3. Click "Search" to find similar patients
4. View results in the table with Patient ID and Similarity Score

## Requirements

- Neo4j database with patient embeddings
- Patient nodes must have `textEmbedding` property
- Vector index should be created on `Patient.textEmbedding` (index name: `patient_text_index`)

## Configuration

The application uses the centralized configuration from `Scripts/utils/config.py` which loads from:
1. `.env` file in the project root
2. Environment variables
3. Default values

## Troubleshooting

### "Patient not found or has no embedding"
- Verify the patient ID exists in Neo4j
- Check that the patient has a `textEmbedding` property
- Run the embedding generation pipeline if embeddings are missing

### "No similar patients found"
- Check that other patients have embeddings
- Verify the vector index exists: `SHOW INDEXES` in Neo4j
- Ensure the index name matches: `patient_text_index`

### Connection Errors
- Verify Neo4j is running
- Check connection credentials in `.env` file
- Ensure the database name is correct
