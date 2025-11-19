"""
Patient Similarity Feature Module
Handles finding similar patients using vector similarity search
"""
import streamlit as st
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Tuple

from utils.neo4j_connection import Neo4jConnection

logger = logging.getLogger(__name__)


def find_similar_patients(connection: Neo4jConnection, subject_id: str, top_k: int = 20) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Find similar patients using vector similarity search on textEmbedding
    
    Args:
        connection: Neo4j connection object
        subject_id: Patient ID to find similar patients for
        top_k: Number of similar patients to return
        
    Returns:
        Tuple of (results list, error message). Results is None if error occurred.
    """
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


def render_similarity_tab(connection: Neo4jConnection):
    """
    Render the patient similarity search tab in Streamlit
    
    Args:
        connection: Neo4j connection object
    """
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

