"""
Patient Similarity and Summarization Application
Application to find similar patients and generate AI-powered clinical summaries
"""
import streamlit as st
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (3 levels up: Streamlit_Application -> Scripts -> project root)
_project_root = Path(__file__).resolve().parents[2]
load_dotenv(_project_root / ".env")

# Add Scripts directory to path for utils imports
# Get the absolute path to the Scripts directory
current_file = Path(__file__).resolve()
scripts_dir = current_file.parent.parent  # Go up from Streamlit_Application to Scripts
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
from features.compare_patients import render_comparison_tab
from features.diagnosis_similarity import render_diagnosis_similarity_tab

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
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
    <style>
    [data-testid="collapsedControl"] { display: none; }
    [data-testid="stSidebar"] { display: none; }
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
    
    # Auto-load Gemini API keys from environment
    if not st.session_state.get('gemini_api_keys'):
        env_keys = get_gemini_api_keys()
        if env_keys:
            st.session_state['gemini_api_keys'] = env_keys
    
    # Use query parameters to track and preserve active tab
    query_params = st.query_params
    
    # Check if we need to stay on summarize or compare tab
    should_stay_on_summarize = query_params.get('tab') == 'summarize' or st.session_state.get('generating_summary', False)
    should_stay_on_compare = query_params.get('tab') == 'compare' or st.session_state.get('generating_comparison', False)
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🔍 Find Similar Patients", 
        "📋 Summarize Patient",
        "🔬 Compare Patients",
        "📅 Patient Journey",
        "🏥 Find by Diagnosis"
    ])
    
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
    
    if should_stay_on_compare:
        st.markdown("""
        <script>
        (function() {
            function switchToCompareTab() {
                // Try multiple selectors
                var selectors = [
                    'button[data-baseweb="tab"]',
                    '[role="tab"]',
                    'button.stTabButton',
                    '[data-testid="stTabButton"]'
                ];
                
                for (var s = 0; s < selectors.length; s++) {
                    var tabs = document.querySelectorAll(selectors[s]);
                    if (tabs.length >= 3) {
                        // Check if third tab is not already active
                        var thirdTab = tabs[2];
                        var isActive = thirdTab.classList.contains('stTabButton-active') || 
                                      thirdTab.getAttribute('aria-selected') === 'true';
                        if (!isActive) {
                            thirdTab.click();
                            return true;
                        }
                    }
                }
                
                // Fallback: search by text
                var allButtons = document.querySelectorAll('button');
                for (var i = 0; i < allButtons.length; i++) {
                    var text = (allButtons[i].textContent || allButtons[i].innerText || '').trim();
                    if (text.includes('Compare Patients') || text.includes('🔬')) {
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
                    setTimeout(switchToCompareTab, 50);
                });
            } else {
                setTimeout(switchToCompareTab, 50);
            }
            
            // Also try on window load as backup
            window.addEventListener('load', function() {
                setTimeout(switchToCompareTab, 100);
            });
        })();
        </script>
        """, unsafe_allow_html=True)
    
    with tab1:
        render_similarity_tab(connection)
    
    with tab2:
        render_summary_tab(connection)
    
    with tab3:
        render_comparison_tab(connection)
    
    with tab4:
        render_patient_journey_tab(connection)
    
    with tab5:
        render_diagnosis_similarity_tab(connection)
    
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
