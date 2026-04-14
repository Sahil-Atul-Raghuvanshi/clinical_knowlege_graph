"""
Diagnosis Similarity Feature Module
Handles finding patients with similar diagnoses using vector similarity search
"""
import streamlit as st
import pandas as pd
import logging
import sys
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import numpy as np

# Add Scripts directory to path for imports
current_file = Path(__file__).resolve()
scripts_dir = current_file.parent.parent.parent
sys.path.insert(0, str(scripts_dir))

# Add Create_Embeddings directory to path
create_embeddings_dir = scripts_dir / 'Create_Embeddings' / 'full_patient_embeddings'
sys.path.insert(0, str(create_embeddings_dir))

from utils.neo4j_connection import Neo4jConnection
from utils.config import Config

# Import TextEmbeddingGenerator for generating embeddings
try:
    from text_embeddings import TextEmbeddingGenerator
except ImportError:
    TextEmbeddingGenerator = None
    logging.warning("TextEmbeddingGenerator not available. Diagnosis similarity search may not work.")

logger = logging.getLogger(__name__)

# Initialize stop words (lazy loading)
_stop_words = None

def _get_stop_words():
    """Get English stop words, downloading if necessary"""
    global _stop_words
    if _stop_words is None:
        try:
            import nltk
            try:
                from nltk.corpus import stopwords
                _stop_words = set(stopwords.words('english'))
            except LookupError:
                # Download stopwords if not available
                logger.info("Downloading NLTK stopwords data...")
                nltk.download('stopwords', quiet=True)
                from nltk.corpus import stopwords
                _stop_words = set(stopwords.words('english'))
            logger.info(f"Loaded {len(_stop_words)} stop words")
        except ImportError:
            logger.warning("NLTK not installed. Install with: pip install nltk. Stop word removal will be skipped.")
            _stop_words = set()  # Empty set - no stop words removed
    return _stop_words

def remove_stop_words(text: str) -> str:
    """
    Remove stop words from text using NLP while preserving structure
    
    Args:
        text: Input text (diagnosis text)
        
    Returns:
        Text with stop words removed, structure preserved
    """
    if not text or not text.strip():
        return text
    
    stop_words = _get_stop_words()
    if not stop_words:
        # NLTK not available, return original text
        return text
    
    # Tokenize text into words while preserving structure
    def process_segment(segment):
        """Process a text segment, removing stop words"""
        tokens = []
        words = re.finditer(r'\b\w+\b', segment)
        last_end = 0
        
        for match in words:
            # Add text before this word
            if match.start() > last_end:
                before_text = segment[last_end:match.start()]
                tokens.append(before_text)
            
            # Check if word is a stop word
            word = match.group()
            if word.lower() not in stop_words:
                tokens.append(word)
            # Otherwise skip the stop word
            last_end = match.end()
        
        # Add remaining text
        if last_end < len(segment):
            tokens.append(segment[last_end:])
        
        result = ''.join(tokens)
        # Clean up multiple spaces
        result = re.sub(r'\s+', ' ', result)
        return result
    
    # Split by common separators to preserve structure
    parts = re.split(r'([|:,;])', text)
    
    processed_parts = []
    for part in parts:
        if part in ['|', ':', ',', ';']:
            # Keep separators as-is
            processed_parts.append(part)
        else:
            # Process this segment to remove stop words
            processed_parts.append(process_segment(part))
    
    # Reconstruct full text
    result = ''.join(processed_parts)
    # Clean up multiple spaces
    result = re.sub(r'\s+', ' ', result)
    # Clean up spaces around separators
    result = re.sub(r'\s*([|:,;])\s*', r' \1 ', result)
    # Final cleanup
    result = re.sub(r'\s+', ' ', result)
    
    return result.strip()

def format_diagnoses_text(diagnosis_text: str) -> str:
    """
    Format diagnosis text for embedding (similar to how it's stored)
    
    Args:
        diagnosis_text: Raw diagnosis text input
        
    Returns:
        Formatted text string for embedding
    """
    if not diagnosis_text or not diagnosis_text.strip():
        return ""
    
    # Split by common delimiters and clean
    diagnoses = re.split(r'[,\n;|]+', diagnosis_text)
    diagnoses = [d.strip() for d in diagnoses if d.strip()]
    
    # Join with separator (same format as stored in all_diagnoses)
    formatted = " | ".join(diagnoses)
    
    return formatted

def generate_diagnosis_embedding(diagnosis_text: str, config: Config) -> Optional[np.ndarray]:
    """
    Generate embedding for diagnosis text
    
    Args:
        diagnosis_text: Processed diagnosis text
        config: Configuration object
        
    Returns:
        Embedding vector or None if error
    """
    if not TextEmbeddingGenerator:
        logger.error("TextEmbeddingGenerator not available")
        return None
    
    try:
        text_generator = TextEmbeddingGenerator(
            model_name=config.embedding.text_model_name,
            use_openai=config.embedding.use_openai if hasattr(config.embedding, 'use_openai') else False,
            use_gemini=config.embedding.use_gemini if hasattr(config.embedding, 'use_gemini') else False
        )
        
        embedding = text_generator.generate_embedding(diagnosis_text)
        return embedding
    except Exception as e:
        logger.error(f"Error generating embedding: {e}", exc_info=True)
        return None

def find_patients_by_diagnosis(
    connection: Neo4jConnection,
    diagnosis_embedding: np.ndarray,
    top_k: int = 20
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Find patients with similar diagnoses using vector similarity search on diagnosis_embeddings
    
    Args:
        connection: Neo4j connection object
        diagnosis_embedding: Embedding vector for the input diagnosis text
        top_k: Number of similar patients to return
        
    Returns:
        Tuple of (results list, error message). Results is None if error occurred.
    """
    try:
        # Convert embedding to list
        embedding_list = diagnosis_embedding.tolist() if isinstance(diagnosis_embedding, np.ndarray) else diagnosis_embedding
        
        # Use vector index to find similar patients
        index_name = 'patient_diagnosis_index'
        
        try:
            similarity_query = f"""
            CALL db.index.vector.queryNodes('{index_name}', $topK, $queryEmbedding)
            YIELD node AS similarPatient, score
            WHERE similarPatient.diagnosis_embeddings IS NOT NULL
            WITH DISTINCT similarPatient, score
            ORDER BY score DESC
            LIMIT $topK
            RETURN similarPatient.subject_id AS patient_id, 
                   score AS similarity_score,
                   similarPatient.all_diagnoses AS all_diagnoses
            """
            
            results = connection.execute_query(
                similarity_query,
                {
                    "topK": top_k,
                    "queryEmbedding": embedding_list
                }
            )
            
            if results:
                logger.info(f"Found {len(results)} similar patients using index '{index_name}'")
                return results, None
                
        except Exception as e:
            logger.debug(f"Vector index '{index_name}' not available: {e}")
        
        # Fallback: manual cosine similarity calculation
        logger.info("Using fallback cosine similarity calculation")
        fallback_query = """
        MATCH (p:Patient)
        WHERE p.diagnosis_embeddings IS NOT NULL
        WITH p,
             p.diagnosis_embeddings AS patientEmbedding,
             $queryEmbedding AS queryEmbedding
        WHERE size(patientEmbedding) = size(queryEmbedding)
        WITH p.subject_id AS patient_id,
             p.all_diagnoses AS all_diagnoses,
             gds.similarity.cosine(patientEmbedding, queryEmbedding) AS similarity_score
        WHERE similarity_score < 1.0
        ORDER BY similarity_score DESC
        LIMIT $topK
        RETURN patient_id, similarity_score, all_diagnoses
        """
        
        try:
            results = connection.execute_query(
                fallback_query,
                {
                    "queryEmbedding": embedding_list,
                    "topK": top_k
                }
            )
            if results:
                return results, None
        except Exception as e:
            logger.warning(f"Fallback similarity search failed: {e}")
        
        return [], "No similar patients found"
        
    except Exception as e:
        logger.error(f"Error finding patients by diagnosis: {e}", exc_info=True)
        return None, str(e)

def render_diagnosis_similarity_tab(connection: Neo4jConnection):
    """
    Render the diagnosis similarity search tab in Streamlit
    
    Args:
        connection: Neo4j connection object
    """
    st.markdown("### 🔬 Find Patients by Diagnosis")
    st.info("Enter a list of diagnoses to find patients with similar diagnoses using vector similarity search on diagnosis embeddings.")
    
    # Diagnosis text input
    diagnosis_text = st.text_area(
        "Enter Diagnosis(es):",
        placeholder="e.g., Diabetes mellitus, Hypertension, Chronic kidney disease\n\nYou can enter multiple diagnoses separated by commas, semicolons, or new lines.",
        height=150,
        key="diagnosis_text_input"
    )
    
    # Number of results
    top_k = st.slider(
        "Number of similar patients to show:",
        min_value=5,
        max_value=50,
        value=20,
        step=5,
        key="diagnosis_similarity_top_k"
    )
    
    # Show processed text (optional, for debugging)
    show_processed = st.checkbox("Show processed text (after stop word removal)", value=False)
    
    # Search button
    search_button = st.button("🔍 Search", type="primary", use_container_width=True, key="diagnosis_search_button")
    
    # Process search
    if search_button and diagnosis_text:
        if not diagnosis_text.strip():
            st.warning("Please enter at least one diagnosis.")
            return
        
        with st.spinner("Processing diagnosis text and searching for similar patients..."):
            try:
                # Load config for embedding generation
                config = Config()
                
                # Step 1: Format diagnosis text
                formatted_text = format_diagnoses_text(diagnosis_text)
                
                if not formatted_text:
                    st.error("No valid diagnosis text found after formatting.")
                    return
                
                # Step 2: Remove stop words
                processed_text = remove_stop_words(formatted_text)
                
                if show_processed:
                    st.info(f"**Processed text (after stop word removal):**\n\n{processed_text}")
                
                if not processed_text.strip():
                    st.warning("Text became empty after stop word removal. Using original formatted text.")
                    processed_text = formatted_text
                
                # Step 3: Generate embedding
                with st.spinner("Generating embedding for diagnosis text..."):
                    diagnosis_embedding = generate_diagnosis_embedding(processed_text, config)
                    
                    if diagnosis_embedding is None:
                        st.error("Failed to generate embedding. Please check your embedding configuration.")
                        return
                
                # Step 4: Search for similar patients
                with st.spinner("Searching for similar patients..."):
                    results, error = find_patients_by_diagnosis(connection, diagnosis_embedding, top_k)
                    
                    if error:
                        st.error(f"Error: {error}")
                        return
                    
                    if not results:
                        st.warning("No patients found with similar diagnoses.")
                        return
                    
                    # Display results in a table
                    st.markdown("### 📊 Patients with Similar Diagnoses")
                    
                    # Create DataFrame
                    df_data = []
                    for r in results:
                        patient_id = r.get('patient_id')
                        similarity_score = r.get('similarity_score', 0)
                        all_diagnoses = r.get('all_diagnoses', [])
                        
                        # Format diagnoses for display
                        diagnoses_display = ", ".join(all_diagnoses[:5]) if all_diagnoses else "N/A"
                        if all_diagnoses and len(all_diagnoses) > 5:
                            diagnoses_display += f" ... (+{len(all_diagnoses) - 5} more)"
                        
                        df_data.append({
                            'Patient ID': patient_id,
                            'Similarity Score': similarity_score,
                            'Diagnoses': diagnoses_display
                        })
                    
                    df = pd.DataFrame(df_data)
                    
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
                logger.error(f"Error in diagnosis search: {e}", exc_info=True)
    
    elif search_button and not diagnosis_text:
        st.warning("Please enter diagnosis text first.")

