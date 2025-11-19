"""
Patient Similarity and Summarization Application
Application to find similar patients and generate AI-powered clinical summaries
"""
import streamlit as st
import logging
import sys
import os
from pathlib import Path

# Add Scripts directory to path for utils imports
# Get the absolute path to the Scripts directory
current_file = Path(__file__).resolve()
scripts_dir = current_file.parent.parent  # Go up from Patient_Similarity to Scripts
scripts_dir_str = str(scripts_dir)

# Add to path if not already there
if scripts_dir_str not in sys.path:
    sys.path.insert(0, scripts_dir_str)

# Verify utils directory exists
utils_dir = scripts_dir / 'utils'
if not utils_dir.exists():
    raise ImportError(f"utils directory not found at {utils_dir}. Current file: {current_file}, Scripts dir: {scripts_dir}")

# Import from centralized utils (Scripts/utils)
from utils.config import Config
from utils.neo4j_connection import Neo4jConnection

# Import feature modules
from features.patient_similarity import render_similarity_tab
from features.summarize_patient import render_summary_tab
from features.chronological_patient_journey import render_patient_journey_tab

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
        
        **3. Patient Journey**
        - Displays complete chronological patient journey
        - Shows all events in temporal order
        - Download as formatted PDF report
        """)
    
    # Use query parameters to track and preserve active tab
    query_params = st.query_params
    
    # Check if we need to stay on summarize tab
    should_stay_on_summarize = query_params.get('tab') == 'summarize' or st.session_state.get('generating_summary', False)
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["🔍 Find Similar Patients", "📋 Summarize Patient", "📅 Patient Journey"])
    
    # Inject JavaScript early to switch tab if needed (before rendering content)
    if should_stay_on_summarize:
        st.markdown("""
        <script>
        (function() {
            function switchToSummarizeTab() {
                // Try multiple selectors
                var selectors = [
                    'button[data-baseweb="tab"]',
                    '[role="tab"]',
                    'button.stTabButton',
                    '[data-testid="stTabButton"]'
                ];
                
                for (var s = 0; s < selectors.length; s++) {
                    var tabs = document.querySelectorAll(selectors[s]);
                    if (tabs.length >= 2) {
                        // Check if second tab is not already active
                        var secondTab = tabs[1];
                        var isActive = secondTab.classList.contains('stTabButton-active') || 
                                      secondTab.getAttribute('aria-selected') === 'true';
                        if (!isActive) {
                            secondTab.click();
                            return true;
                        }
                    }
                }
                
                // Fallback: search by text
                var allButtons = document.querySelectorAll('button');
                for (var i = 0; i < allButtons.length; i++) {
                    var text = (allButtons[i].textContent || allButtons[i].innerText || '').trim();
                    if (text.includes('Summarize Patient') || text.includes('📋')) {
                        var isActive = allButtons[i].classList.contains('stTabButton-active');
                        if (!isActive) {
                            allButtons[i].click();
                            return true;
                        }
                    }
                }
                return false;
            }
            
            // Run immediately
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', function() {
                    setTimeout(switchToSummarizeTab, 50);
                });
            } else {
                setTimeout(switchToSummarizeTab, 50);
            }
            
            // Also try on window load as backup
            window.addEventListener('load', function() {
                setTimeout(switchToSummarizeTab, 100);
            });
        })();
        </script>
        """, unsafe_allow_html=True)
    
    with tab1:
        render_similarity_tab(connection)
    
    with tab2:
        render_summary_tab(connection)
    
    with tab3:
        render_patient_journey_tab(connection)
    
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
