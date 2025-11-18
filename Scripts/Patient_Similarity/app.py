"""
Patient Similarity and Summarization Application
Application to find similar patients and generate AI-powered clinical summaries
"""
import streamlit as st
import logging
import sys
import os
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# Add Scripts directory to path for utils imports
scripts_dir = Path(__file__).parent.parent
sys.path.insert(0, str(scripts_dir))

# Import from centralized utils (Scripts/utils)
from utils.config import Config
from utils.neo4j_connection import Neo4jConnection

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress Neo4j driver logs
logging.getLogger('neo4j').setLevel(logging.ERROR)
logging.getLogger('neo4j.io').setLevel(logging.ERROR)

# Page configuration
st.set_page_config(
    page_title="Patient Analysis Dashboard",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
    }
    .summary-section {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_neo4j_connection():
    """Initialize Neo4j connection (cached)"""
    try:
        config = Config()
        conn = Neo4jConnection(
            uri=config.neo4j.uri,
            username=config.neo4j.username,
            password=config.neo4j.password,
            database=config.neo4j.database
        )
        conn.connect()
        return conn, None
    except Exception as e:
        logger.error(f"Error initializing Neo4j connection: {e}")
        return None, str(e)


def get_gemini_api_keys():
    """Get Gemini API keys from environment or user input"""
    # Try to get from environment variables
    api_keys_env = os.getenv('GEMINI_API_KEYS', '')
    if api_keys_env:
        keys = [k.strip() for k in api_keys_env.split(',') if k.strip()]
        if keys:
            return keys
    
    # Try individual environment variable
    single_key = os.getenv('GEMINI_API_KEY', '')
    if single_key:
        return [single_key]
    
    return []


def extract_graph_structure(connection: Neo4jConnection, subject_id):
    """Extract complete graph structure with nodes, relationships, and attributes"""
    logger.info(f"Extracting graph structure for patient {subject_id}...")
    
    query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    WITH p
    OPTIONAL MATCH path = (p)-[r*1..3]->(n)
    WHERE NOT n:DiagnosisItem 
      AND NOT n:MedicationItem 
      AND NOT n:LabResultItem 
      AND NOT n:MicrobiologyResultItem
    WITH p, relationships(path) as rels, nodes(path) as nodeList
    WHERE rels IS NOT NULL AND size(rels) > 0
    UNWIND range(0, size(rels)-1) as idx
    WITH p, rels[idx] as rel, nodeList[idx] as startNode, nodeList[idx+1] as endNode
    WHERE NOT startNode:DiagnosisItem 
      AND NOT startNode:MedicationItem 
      AND NOT startNode:LabResultItem 
      AND NOT startNode:MicrobiologyResultItem
      AND NOT endNode:DiagnosisItem 
      AND NOT endNode:MedicationItem 
      AND NOT endNode:LabResultItem 
      AND NOT endNode:MicrobiologyResultItem
    RETURN DISTINCT
        labels(startNode) as start_labels,
        properties(startNode) as start_props,
        type(rel) as relationship_type,
        labels(endNode) as end_labels,
        properties(endNode) as end_props
    """
    
    results = connection.execute_query(query, {"subject_id": str(subject_id)})
    
    # Build structured graph data
    graph_data = {
        "patient_id": str(subject_id),
        "nodes": {},
        "relationships": []
    }
    
    # Get patient node separately
    patient_query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    RETURN labels(p) as labels, properties(p) as props
    """
    
    patient_results = connection.execute_query(patient_query, {"subject_id": str(subject_id)})
    
    if patient_results:
        patient_record = patient_results[0]
        graph_data["patient"] = {
            "labels": list(patient_record['labels']) if patient_record.get('labels') else [],
            "properties": dict(patient_record['props']) if patient_record.get('props') else {}
        }
    
    # Process relationships and nodes
    for record in results:
        start_labels = list(record['start_labels']) if record.get('start_labels') else []
        start_props = dict(record['start_props']) if record.get('start_props') else {}
        rel_type = record.get('relationship_type')
        end_labels = list(record['end_labels']) if record.get('end_labels') else []
        end_props = dict(record['end_props']) if record.get('end_props') else {}
        
        # Store nodes
        if start_labels and start_props:
            node_key = f"{start_labels[0]}_{start_props.get('name', 'unknown')}"
            if node_key not in graph_data["nodes"]:
                graph_data["nodes"][node_key] = {
                    "labels": start_labels,
                    "properties": start_props
                }
        
        if end_labels and end_props:
            node_key = f"{end_labels[0]}_{end_props.get('name', 'unknown')}"
            if node_key not in graph_data["nodes"]:
                graph_data["nodes"][node_key] = {
                    "labels": end_labels,
                    "properties": end_props
                }
        
        # Store relationship
        if rel_type and start_labels and end_labels:
            graph_data["relationships"].append({
                "from": {
                    "label": start_labels[0] if start_labels else "Unknown",
                    "name": start_props.get('name', 'unknown')
                },
                "relationship": rel_type,
                "to": {
                    "label": end_labels[0] if end_labels else "Unknown",
                    "name": end_props.get('name', 'unknown'),
                    "properties": end_props
                }
            })
    
    logger.info(f"Extracted {len(graph_data['nodes'])} nodes and {len(graph_data['relationships'])} relationships")
    
    if len(graph_data['relationships']) == 0:
        logger.warning(f"Patient {subject_id} has no relationships in the graph")
    
    return graph_data


def clean_json_string(json_str):
    """Clean JSON string by removing trailing commas and other common issues"""
    json_str = re.sub(r',\s*}', '}', json_str)
    json_str = re.sub(r',\s*]', ']', json_str)
    return json_str


def get_llm_summary(graph_data, subject_id, api_keys, max_retries=3):
    """Send graph structure to LLM and get structured JSON summary"""
    logger.info("Sending graph structure to LLM for summarization...")
    
    if not api_keys:
        return None, "No Gemini API keys configured. Please add API keys in the sidebar."
    
    # Convert graph data to JSON string for prompt
    graph_json_str = json.dumps(graph_data, indent=2, default=str)
    
    prompt = f"""You are a medical summarization expert. You will receive a KNOWLEDGE GRAPH representing a patient's clinical journey from a Neo4j database. The graph contains nodes (entities) and relationships showing how different clinical events are connected.

KNOWLEDGE GRAPH STRUCTURE:
- "patient": Core patient information with demographics
- "nodes": All clinical entities (admissions, diagnoses, procedures, lab tests, medications, etc.) with their properties
- "relationships": Connections showing how entities relate (e.g., Patient -> HAS_ADMISSION -> HospitalAdmission)

YOUR TASK:
Analyze this knowledge graph and create a comprehensive yet concise 1000-word clinical summary.

IMPORTANT OUTPUT FORMAT REQUIREMENTS:
- You MUST return ONLY a valid JSON object
- Do NOT include any markdown formatting, code blocks, or backticks
- Do NOT include ```json or ``` in your response
- Return ONLY the raw JSON object starting with {{ and ending with }}
- Do NOT use trailing commas in arrays or objects

The JSON structure must be EXACTLY as follows:
{{
  "patient_id": "string",
  "patient_demographics": {{
    "age": "string",
    "gender": "string",
    "race": "string",
    "total_admissions": "string"
  }},
  "executive_summary": "A 2-3 sentence overview of the patient's condition and outcome (max 100 words)",
  "chief_complaints_and_presentation": "Description of how the patient presented, initial complaints, and triage findings (max 150 words)",
  "clinical_course": "Detailed narrative of the hospital stay, including key events, treatments, and patient progression through different units (max 300 words)",
  "key_diagnoses": [
    "Primary diagnosis 1",
    "Primary diagnosis 2",
    "Secondary diagnosis 1 (if relevant)"
  ],
  "significant_procedures": [
    "Procedure 1 with brief context",
    "Procedure 2 with brief context"
  ],
  "medications": {{
    "started": ["medication1", "medication2"],
    "stopped": ["medication3", "medication4"],
    "to_avoid": ["medication5"]
  }},
  "key_lab_findings": [
    "Abnormal finding 1 with value and context",
    "Abnormal finding 2 with value and context"
  ],
  "microbiology_findings": [
    "Finding 1 if present",
    "Finding 2 if present"
  ],
  "discharge_summary": {{
    "disposition": "string",
    "condition": "string",
    "activity_status": "string",
    "follow_up_instructions": "Brief summary of discharge instructions (max 150 words)"
  }},
  "clinical_significance": "A brief analysis of the overall clinical picture, complications, and outcomes (max 100 words)"
}}

GUIDELINES:
1. Keep the total word count around 1000 words
2. Focus on clinically significant information
3. Use clear, professional medical language
4. Look for timestamp fields (admittime, charttime, starttime, etc.) to maintain chronological flow
5. Extract key information from node properties (diagnoses from Diagnosis nodes, procedures from Procedures nodes, etc.)
6. Highlight abnormal lab findings (look for [abnormal] markers in lab_results)
7. Be concise but comprehensive
8. Remove any placeholder text like [redacted], ___, or [hospital contact]
9. If a section has no data, use empty string "" or empty array []
10. Pay attention to relationships to understand the patient's journey through the hospital

KNOWLEDGE GRAPH DATA:
{graph_json_str}

Return ONLY the JSON object without any additional text or formatting:"""

    current_key_index = 0
    keys_tried = set()
    
    for attempt in range(max_retries):
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_keys[current_key_index])
            model = genai.GenerativeModel('gemini-2.5-pro')
            
            response = model.generate_content(prompt)
            response_text = response.text.strip()
            
            # Remove markdown code block formatting if present
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            elif response_text.startswith("```"):
                response_text = response_text[3:]
            
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            
            response_text = response_text.strip()
            
            # Clean JSON string
            response_text = clean_json_string(response_text)
            
            # Parse JSON
            summary_json = json.loads(response_text)
            
            logger.info("Successfully received and parsed LLM summary")
            return summary_json, None
            
        except json.JSONDecodeError as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} - Failed to parse LLM response as JSON: {e}")
            if attempt == max_retries - 1:
                return None, f"Failed to parse LLM response as JSON: {str(e)}"
            time.sleep(3)
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Check if it's a rate limit or quota error
            if any(indicator in error_msg for indicator in ['quota', 'rate limit', 'resource exhausted', '429', 'too many requests']):
                keys_tried.add(current_key_index)
                if len(keys_tried) < len(api_keys):
                    current_key_index = (current_key_index + 1) % len(api_keys)
                    time.sleep(2)
                    continue
                else:
                    return None, "All API keys hit rate limits"
            
            # Check if it's an invalid API key error
            if 'api key' in error_msg or 'api_key_invalid' in error_msg:
                keys_tried.add(current_key_index)
                if len(keys_tried) < len(api_keys):
                    current_key_index = (current_key_index + 1) % len(api_keys)
                    time.sleep(1)
                    continue
                else:
                    return None, "All API keys are invalid"
            
            if attempt == max_retries - 1:
                return None, f"Error generating summary: {str(e)}"
            
            time.sleep(5)
    
    return None, f"Failed to get LLM summary after {max_retries} attempts"


def find_similar_patients(connection: Neo4jConnection, subject_id: str, top_k: int = 20):
    """Find similar patients using vector similarity search on textEmbedding"""
    try:
        # First, check if patient exists and has embedding
        check_query = """
        MATCH (p:Patient {subject_id: $subject_id})
        RETURN p.textEmbedding IS NOT NULL AS has_embedding
        """
        
        result = connection.execute_query(check_query, {"subject_id": int(subject_id)})
        
        if not result or not result[0].get('has_embedding'):
            return None, f"Patient {subject_id} not found or has no embedding"
        
        # Use vector index to find similar patients
        index_names = ['patient_text_index', 'patient_embedding_index', 'patient_journey_index']
        
        for index_name in index_names:
            try:
                similarity_query = f"""
                MATCH (p:Patient {{subject_id: $subject_id}})
                WITH p.textEmbedding AS refEmbedding
                WHERE refEmbedding IS NOT NULL
                CALL db.index.vector.queryNodes('{index_name}', $topK, refEmbedding)
                YIELD node AS similarPatient, score
                WHERE similarPatient.subject_id <> $subject_id 
                  AND score < 1.0
                WITH DISTINCT similarPatient, score
                ORDER BY score DESC
                LIMIT $topK
                RETURN similarPatient.subject_id AS patient_id, score AS similarity_score
                """
                
                results = connection.execute_query(
                    similarity_query,
                    {
                        "subject_id": int(subject_id),
                        "topK": top_k
                    }
                )
                
                if results:
                    logger.info(f"Found {len(results)} similar patients using index '{index_name}'")
                    return results, None
                    
            except Exception as e:
                logger.debug(f"Vector index '{index_name}' not available: {e}")
                continue
        
        # Fallback: manual cosine similarity calculation
        logger.info("Using fallback cosine similarity calculation")
        fallback_query = """
        MATCH (p1:Patient {subject_id: $subject_id})
        MATCH (p2:Patient)
        WHERE p1.textEmbedding IS NOT NULL 
          AND p2.textEmbedding IS NOT NULL
          AND p1.subject_id <> p2.subject_id
        WITH p1, p2,
             p1.textEmbedding AS emb1,
             p2.textEmbedding AS emb2
        WHERE size(emb1) = size(emb2)
        WITH p2.subject_id AS patient_id,
             gds.similarity.cosine(emb1, emb2) AS similarity_score
        WHERE similarity_score < 1.0
        ORDER BY similarity_score DESC
        LIMIT $topK
        RETURN patient_id, similarity_score
        """
        
        try:
            results = connection.execute_query(
                fallback_query,
                {
                    "subject_id": int(subject_id),
                    "topK": top_k
                }
            )
            if results:
                return results, None
        except Exception as e:
            logger.warning(f"Fallback similarity search failed: {e}")
        
        return [], "No similar patients found"
        
    except Exception as e:
        logger.error(f"Error finding similar patients: {e}", exc_info=True)
        return None, str(e)


def render_summary_tab(connection):
    """Render the patient summarization tab"""
    st.markdown("### 📋 Generate Patient Summary")
    st.info("Enter a patient ID to generate an AI-powered clinical summary from the knowledge graph.")
    
    # Patient ID input
    patient_id = st.text_input(
        "Enter Patient ID:",
        placeholder="e.g., 10000032",
        key="summary_patient_id_input"
    )
    
    # Generate button
    generate_button = st.button("🤖 Generate Summary", type="primary", use_container_width=True)
    
    if generate_button and patient_id:
        if not patient_id.strip().isdigit():
            st.error("Please enter a valid numeric patient ID")
            return
        
        # Get API keys from session state or sidebar
        api_keys = st.session_state.get('gemini_api_keys', [])
        if not api_keys:
            st.error("⚠️ No Gemini API keys configured. Please add API keys in the sidebar.")
            return
        
        with st.spinner("Extracting knowledge graph structure..."):
            try:
                # Check if patient exists
                check_query = """
                MATCH (p:Patient)
                WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
                RETURN p.subject_id as subject_id
                """
                result = connection.execute_query(check_query, {"subject_id": str(patient_id)})
                
                if not result:
                    st.error(f"Patient {patient_id} not found in database!")
                    return
                
                # Extract graph structure
                graph_data = extract_graph_structure(connection, patient_id.strip())
                
                if len(graph_data.get('relationships', [])) == 0:
                    st.warning(f"Patient {patient_id} has no relationships in the graph. Limited data available.")
                
            except Exception as e:
                st.error(f"Error extracting graph structure: {str(e)}")
                logger.error(f"Error extracting graph: {e}", exc_info=True)
                return
        
        with st.spinner("🤖 Generating AI summary (this may take a minute)..."):
            try:
                summary_json, error = get_llm_summary(graph_data, patient_id.strip(), api_keys)
                
                if error:
                    st.error(f"Error: {error}")
                    return
                
                if not summary_json:
                    st.error("Failed to generate summary")
                    return
                
                # Display summary
                st.success("✅ Summary generated successfully!")
                st.markdown("---")
                
                # Patient Demographics
                if summary_json.get('patient_demographics'):
                    st.markdown("#### 👤 Patient Demographics")
                    demo = summary_json['patient_demographics']
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Age", demo.get('age', 'N/A'))
                    with col2:
                        st.metric("Gender", demo.get('gender', 'N/A'))
                    with col3:
                        st.metric("Race", demo.get('race', 'N/A'))
                    with col4:
                        st.metric("Total Admissions", demo.get('total_admissions', 'N/A'))
                    st.markdown("---")
                
                # Executive Summary
                if summary_json.get('executive_summary'):
                    st.markdown("#### 📊 Executive Summary")
                    st.write(summary_json['executive_summary'])
                    st.markdown("---")
                
                # Chief Complaints
                if summary_json.get('chief_complaints_and_presentation'):
                    st.markdown("#### 🏥 Chief Complaints and Presentation")
                    st.write(summary_json['chief_complaints_and_presentation'])
                    st.markdown("---")
                
                # Key Diagnoses
                if summary_json.get('key_diagnoses'):
                    st.markdown("#### 🩺 Key Diagnoses")
                    for diagnosis in summary_json['key_diagnoses']:
                        st.write(f"• {diagnosis}")
                    st.markdown("---")
                
                # Clinical Course
                if summary_json.get('clinical_course'):
                    st.markdown("#### 📈 Clinical Course")
                    st.write(summary_json['clinical_course'])
                    st.markdown("---")
                
                # Significant Procedures
                if summary_json.get('significant_procedures'):
                    st.markdown("#### 🔬 Significant Procedures")
                    for procedure in summary_json['significant_procedures']:
                        st.write(f"• {procedure}")
                    st.markdown("---")
                
                # Key Lab Findings
                if summary_json.get('key_lab_findings'):
                    st.markdown("#### 🧪 Key Laboratory Findings")
                    for finding in summary_json['key_lab_findings']:
                        st.write(f"• {finding}")
                    st.markdown("---")
                
                # Microbiology Findings
                if summary_json.get('microbiology_findings'):
                    findings = [f for f in summary_json['microbiology_findings'] if f.strip()]
                    if findings:
                        st.markdown("#### 🦠 Microbiology Findings")
                        for finding in findings:
                            st.write(f"• {finding}")
                        st.markdown("---")
                
                # Medications
                if summary_json.get('medications'):
                    meds = summary_json['medications']
                    if any([meds.get('started'), meds.get('stopped'), meds.get('to_avoid')]):
                        st.markdown("#### 💊 Medication Management")
                        
                        if meds.get('started'):
                            st.markdown("**Medications Started:**")
                            for med in meds['started']:
                                st.write(f"• {med}")
                        
                        if meds.get('stopped'):
                            st.markdown("**Medications Stopped:**")
                            for med in meds['stopped']:
                                st.write(f"• {med}")
                        
                        if meds.get('to_avoid'):
                            st.markdown("**Medications to Avoid:**")
                            for med in meds['to_avoid']:
                                st.write(f"• {med}")
                        st.markdown("---")
                
                # Discharge Summary
                if summary_json.get('discharge_summary'):
                    discharge = summary_json['discharge_summary']
                    st.markdown("#### 🏥 Discharge Summary")
                    
                    if discharge.get('disposition'):
                        st.write(f"**Disposition:** {discharge['disposition']}")
                    if discharge.get('condition'):
                        st.write(f"**Condition:** {discharge['condition']}")
                    if discharge.get('activity_status'):
                        st.write(f"**Activity Status:** {discharge['activity_status']}")
                    if discharge.get('follow_up_instructions'):
                        st.write(f"**Follow-up Instructions:** {discharge['follow_up_instructions']}")
                    st.markdown("---")
                
                # Clinical Significance
                if summary_json.get('clinical_significance'):
                    st.markdown("#### ⚕️ Clinical Significance")
                    st.write(summary_json['clinical_significance'])
                
                # Download JSON button
                st.markdown("---")
                json_str = json.dumps(summary_json, indent=2, ensure_ascii=False)
                st.download_button(
                    label="📥 Download Summary as JSON",
                    data=json_str,
                    file_name=f"patient_{patient_id}_summary.json",
                    mime="application/json"
                )
                
            except Exception as e:
                st.error(f"Error generating summary: {str(e)}")
                logger.error(f"Error in summary generation: {e}", exc_info=True)
    
    elif generate_button and not patient_id:
        st.warning("Please enter a patient ID first.")


def render_similarity_tab(connection):
    """Render the patient similarity search tab"""
    st.markdown("### 🔍 Find Similar Patients")
    st.info("Enter a patient ID to find similar patients based on their clinical profiles using vector similarity search.")
    
    # Patient ID input
    patient_id = st.text_input(
        "Enter Patient ID:",
        placeholder="e.g., 10000032",
        key="similarity_patient_id_input"
    )
    
    # Number of results
    top_k = st.slider(
        "Number of similar patients to show:",
        min_value=5,
        max_value=50,
        value=20,
        step=5
    )
    
    # Search button
    search_button = st.button("🔍 Search", type="primary", use_container_width=True)
    
    # Process search
    if search_button and patient_id:
        if not patient_id.strip().isdigit():
            st.error("Please enter a valid numeric patient ID")
            return
        
        with st.spinner("Searching for similar patients..."):
            try:
                results, error = find_similar_patients(connection, patient_id.strip(), top_k)
                
                if error:
                    st.error(f"Error: {error}")
                    return
                
                if not results:
                    st.warning(f"No similar patients found for patient {patient_id}")
                    return
                
                # Display results in a table
                st.markdown("### 📊 Similar Patients")
                
                # Create DataFrame
                df = pd.DataFrame(results)
                df = df.rename(columns={
                    'patient_id': 'Patient ID',
                    'similarity_score': 'Similarity Score'
                })
                
                # Format similarity score to 4 decimal places
                df['Similarity Score'] = df['Similarity Score'].apply(lambda x: f"{x:.4f}")
                
                # Display table
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Summary statistics
                if len(results) > 0:
                    scores = [r['similarity_score'] for r in results]
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Results", len(results))
                    with col2:
                        st.metric("Highest Similarity", f"{max(scores):.4f}")
                    with col3:
                        st.metric("Lowest Similarity", f"{min(scores):.4f}")
                
            except Exception as e:
                st.error(f"Error processing search: {str(e)}")
                logger.error(f"Error in search: {e}", exc_info=True)
    
    elif search_button and not patient_id:
        st.warning("Please enter a patient ID first.")


def main():
    """Main application"""
    st.markdown('<h1 class="main-header">🩺 Patient Analysis Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize Neo4j connection
    connection, init_error = get_neo4j_connection()
    
    if init_error:
        st.error(f"Connection Error: {init_error}")
        st.info("Please check your Neo4j configuration and ensure Neo4j is running.")
        return
    
    if not connection:
        st.error("Failed to initialize Neo4j connection")
        return
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Gemini API Keys configuration
        st.subheader("🔑 Gemini API Keys")
        st.info("""
        For patient summarization, you need Gemini API keys.
        Add keys separated by commas or set GEMINI_API_KEYS environment variable.
        """)
        
        api_keys_input = st.text_area(
            "Enter Gemini API Keys (one per line or comma-separated):",
            value="\n".join(st.session_state.get('gemini_api_keys', [])),
            height=100,
            help="You can add multiple keys for automatic rotation"
        )
        
        if st.button("💾 Save API Keys"):
            # Parse API keys
            keys = []
            for line in api_keys_input.split('\n'):
                line = line.strip()
                if line:
                    # Handle comma-separated keys
                    keys.extend([k.strip() for k in line.split(',') if k.strip()])
            
            # Also try to get from environment
            env_keys = get_gemini_api_keys()
            if env_keys and not keys:
                keys = env_keys
            
            if keys:
                st.session_state['gemini_api_keys'] = keys
                st.success(f"✅ Saved {len(keys)} API key(s)")
            else:
                st.warning("⚠️ No valid API keys entered")
        
        # Show current API keys count
        current_keys = st.session_state.get('gemini_api_keys', [])
        if current_keys:
            st.info(f"📌 {len(current_keys)} API key(s) configured")
        else:
            # Try to load from environment
            env_keys = get_gemini_api_keys()
            if env_keys:
                st.session_state['gemini_api_keys'] = env_keys
                st.info(f"📌 {len(env_keys)} API key(s) loaded from environment")
        
        st.markdown("---")
        
        st.header("ℹ️ About")
        st.markdown("""
        This application provides two features:
        
        **1. Find Similar Patients**
        - Uses Neo4j vector similarity search
        - Based on patient text embeddings
        - Results ranked by similarity score
        
        **2. Summarize Patient**
        - Extracts knowledge graph structure
        - Uses AI (Gemini) to generate clinical summary
        - Comprehensive 1000-word report
        """)
    
    # Create tabs
    tab1, tab2 = st.tabs(["🔍 Find Similar Patients", "📋 Summarize Patient"])
    
    with tab1:
        render_similarity_tab(connection)
    
    with tab2:
        render_summary_tab(connection)
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "Patient Analysis Dashboard | Powered by Neo4j & Gemini AI"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
