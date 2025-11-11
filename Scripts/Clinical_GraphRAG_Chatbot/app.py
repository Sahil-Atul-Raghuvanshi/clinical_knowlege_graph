"""
Streamlit Chatbot Application
Main frontend for Clinical GraphRAG Chatbot
"""
import streamlit as st
import logging
import json
from pathlib import Path
from query_processor import QueryProcessor
from context_builder import ContextBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Clinical GraphRAG Chatbot",
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
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f0f2f6;
        margin: 1rem 0;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def initialize_processor():
    """Initialize query processor (cached)"""
    try:
        processor = QueryProcessor()
        return processor, None
    except Exception as e:
        logger.error(f"Error initializing processor: {e}")
        return None, str(e)


def main():
    """Main application"""
    st.markdown('<h1 class="main-header">🩺 Clinical GraphRAG Chatbot</h1>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.info("""
        This chatbot uses:
        - **Neo4j** for graph traversal
        - **Milvus** for semantic search
        - **Gemini** for answer generation
        """)
        
        st.header("📝 Example Queries")
        example_queries = [
            "Find patients similar to patient 100045",
            "Show me all similar patient pairs",
            "What treatments worked best for cirrhosis?",
            "Find patients that are similar to each other",
            "Summarize the last 3 admissions for patient 100023",
            "What medications are commonly prescribed for sepsis?",
            "Find patients with similar diagnoses to patient 100012"
        ]
        
        for i, example in enumerate(example_queries):
            if st.button(f"Example {i+1}", key=f"example_{i}"):
                st.session_state.query_input = example
                st.rerun()
        
        st.header("ℹ️ About")
        st.markdown("""
        This chatbot answers clinical questions by:
        1. Retrieving relevant data from the knowledge graph
        2. Finding semantically similar items
        3. Generating evidence-based answers
        """)
    
    # Initialize processor
    processor, init_error = initialize_processor()
    
    if init_error:
        st.error(f"Initialization Error: {init_error}")
        st.info("Please check your configuration files and ensure Neo4j and Milvus are running.")
        return
    
    if not processor:
        st.error("Failed to initialize query processor")
        return
    
    # Chat interface
    st.markdown("### 💬 Ask a Clinical Question")
    
    # Initialize query_input in session state if not exists
    if 'query_input' not in st.session_state:
        st.session_state.query_input = ""
    
    # Query input
    query = st.text_input(
        "Enter your question:",
        value=st.session_state.query_input,
        placeholder="e.g., Find patients similar to patient 100045",
        key="query_input"
    )
    
    # Process button
    col1, col2 = st.columns([1, 4])
    with col1:
        process_button = st.button("🔍 Process Query", type="primary", use_container_width=True)
    
    # Process query
    if process_button and query:
        with st.spinner("Processing query..."):
            try:
                # Process query
                result = processor.process_query(query, top_k=20, generate_answer=True)
                
                # Display answer
                st.markdown("### 🧠 Answer")
                if result.get("answer"):
                    st.markdown(result["answer"])
                else:
                    st.warning("No answer generated. Check the context data below.")
                
                # Display intent
                intent = result.get("intent", "unknown")
                st.info(f"**Detected Intent:** {intent.replace('_', ' ').title()}")
                
                # Display entities
                entities = result.get("entities", {})
                if entities:
                    with st.expander("🔍 Extracted Entities"):
                        st.json(entities)
                
                # Context data
                with st.expander("📊 Context Data"):
                    context = result.get("context", {})
                    if context:
                        # Display formatted context
                        context_builder = ContextBuilder()
                        context_text = context_builder.format_context_for_llm(context)
                        st.text_area("Formatted Context:", context_text, height=300)
                        
                        # Display JSON context
                        st.markdown("**JSON Context:**")
                        st.json(context)
                
                # Retrieval results
                with st.expander("🔎 Retrieval Results"):
                    retrieval_results = result.get("retrieval_results", {})
                    if retrieval_results:
                        st.json(retrieval_results)
                
                # Similar patients
                context = result.get("context", {})
                intent = result.get("intent", "")
                
                # Check if this is an "all similar pairs" query
                all_similar_pairs = context.get("all_similar_pairs", [])
                if all_similar_pairs and intent == "all_similar_pairs":
                    st.markdown("### 🔗 All Similar Patient Pairs")
                    summary = context.get("summary", {})
                    pair_stats = summary.get("pair_statistics", {})
                    
                    if pair_stats:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Pairs", pair_stats.get("total_pairs", 0))
                        with col2:
                            st.metric("Unique Patients", pair_stats.get("unique_patients", 0))
                    
                    # Display pairs in a table format
                    with st.expander(f"📊 View All Similar Patient Pairs ({len(all_similar_pairs)})", expanded=True):
                        # Show top 50 pairs
                        display_pairs = all_similar_pairs[:50]
                        for i, pair in enumerate(display_pairs, 1):
                            st.markdown(f"""
                            **Pair {i}:** Patient {pair.get('patient_1')} ↔ Patient {pair.get('patient_2')}
                            - **Similarity Score:** {pair.get('similarity_score', 0):.3f}
                            - **Patient {pair.get('patient_1')}:** Age {pair.get('patient_1_age', 'N/A')}, {pair.get('patient_1_gender', 'N/A')}
                            - **Patient {pair.get('patient_2')}:** Age {pair.get('patient_2_age', 'N/A')}, {pair.get('patient_2_gender', 'N/A')}
                            """)
                            st.divider()
                        
                        if len(all_similar_pairs) > 50:
                            st.info(f"Showing top 50 pairs out of {len(all_similar_pairs)} total pairs. Use the JSON view below to see all pairs.")
                
                # Display similar patients for specific patient queries
                similar_patients = context.get("similar_patients", [])
                if similar_patients and intent != "all_similar_pairs":
                    # Ensure at least 10 are shown, but show all available up to 20
                    num_to_show = max(10, min(len(similar_patients), 20))
                    with st.expander(f"👥 Similar Patients ({len(similar_patients)} found, showing {num_to_show})", expanded=False):
                        for i, sp in enumerate(similar_patients[:num_to_show], 1):
                            ref_patient = sp.get('reference_patient', '')
                            ref_text = f" (similar to Patient {ref_patient})" if ref_patient else ""
                            
                            # Basic info
                            st.markdown(f"""
                            **{i}. Patient {sp.get('subject_id')}{ref_text}**
                            - Similarity Score: **{sp.get('similarity_score', 0):.3f}**
                            - Gender: {sp.get('gender', 'N/A')}
                            - Age: {sp.get('age', 'N/A')}
                            """)
                            
                            # Detailed clinical information
                            diagnoses = sp.get('diagnoses', [])
                            medications = sp.get('medications', [])
                            treatments = sp.get('treatments', [])
                            
                            if diagnoses or medications or treatments:
                                with st.container():
                                    if diagnoses:
                                        st.markdown(f"**Diagnoses ({len(diagnoses)}):**")
                                        diag_list = [d.get('diagnosis') or d.get('short_title') or d.get('icd_code', 'N/A') for d in diagnoses[:5]]
                                        for diag in diag_list:
                                            st.markdown(f"  - {diag}")
                                    
                                    if medications:
                                        st.markdown(f"**Medications ({len(medications)}):**")
                                        med_list = [m.get('medication') or m.get('medicines', 'N/A') for m in medications[:5]]
                                        for med in med_list:
                                            st.markdown(f"  - {med}")
                                    
                                    if treatments:
                                        st.markdown(f"**Treatments/Procedures ({len(treatments)}):**")
                                        treat_list = [t.get('procedure') or t.get('short_title', 'N/A') for t in treatments[:3]]
                                        for treat in treat_list:
                                            st.markdown(f"  - {treat}")
                            
                            st.divider()
                        
                        if len(similar_patients) > num_to_show:
                            st.info(f"Showing top {num_to_show} similar patients. Total: {len(similar_patients)}")
                
                # Clinical findings summary
                clinical_findings = context.get("clinical_findings", {})
                if clinical_findings:
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "Diagnoses",
                            len(clinical_findings.get("diagnoses", []))
                        )
                    
                    with col2:
                        st.metric(
                            "Medications",
                            len(clinical_findings.get("medications", []))
                        )
                    
                    with col3:
                        st.metric(
                            "Lab Results",
                            len(clinical_findings.get("lab_results", []))
                        )
                
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")
                logger.error(f"Error in query processing: {e}", exc_info=True)
    
    elif process_button and not query:
        st.warning("Please enter a query first.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>"
        "Clinical GraphRAG Chatbot | Powered by Neo4j, Milvus, and Gemini"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()

