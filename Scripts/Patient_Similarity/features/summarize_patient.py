"""
Patient Summarization Feature Module
Handles generating AI-powered clinical summaries from patient knowledge graphs
"""
import streamlit as st
import json
import re
import time
import logging
from typing import Dict, List, Any, Optional, Tuple

from utils.neo4j_connection import Neo4jConnection
from load_data.retrieve_patient_kg import retrieve_patient_kg
from features.download_summarized_pdf import create_pdf_from_json

logger = logging.getLogger(__name__)


def clean_json_string(json_str: str) -> str:
    """
    Clean JSON string by removing trailing commas and other common issues
    
    Args:
        json_str: JSON string to clean
        
    Returns:
        Cleaned JSON string
    """
    json_str = re.sub(r',\s*}', '}', json_str)
    json_str = re.sub(r',\s*]', ']', json_str)
    return json_str


def get_llm_summary(graph_data: Dict[str, Any], subject_id: str, api_keys: List[str], max_retries: int = 3) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Send graph structure to LLM and get structured JSON summary
    
    Args:
        graph_data: Graph structure dictionary
        subject_id: Patient ID
        api_keys: List of Gemini API keys
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (summary JSON dict, error message). Summary is None if error occurred.
    """
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
- "temporal_events": Chronologically ordered list of all events with timestamps, showing what happened when in the patient's journey

YOUR TASK:
Analyze this knowledge graph and create a comprehensive yet concise 1000-word clinical summary. 
Pay special attention to the "temporal_events" array which provides a chronologically ordered timeline of all clinical events. 
Use this temporal ordering to construct a narrative that accurately reflects the sequence of events in the patient's clinical journey.

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


def render_summary_tab(connection: Neo4jConnection):
    """
    Render the patient summarization tab in Streamlit
    
    Args:
        connection: Neo4j connection object
    """
    st.markdown("### 📋 Generate Patient Summary")
    st.info("Enter a patient ID to generate an AI-powered clinical summary from the knowledge graph.")
    
    # Patient ID input
    patient_id = st.text_input(
        "Enter Patient ID:",
        placeholder="e.g., 10000032",
        key="summary_patient_id_input"
    )
    
    # Generate button - using key to prevent tab reset
    generate_button = st.button("🤖 Generate Summary", type="primary", use_container_width=True, key="generate_summary_button")
    
    # If button is clicked, set query param to preserve tab state and store patient_id
    if generate_button:
        if patient_id:
            st.query_params.tab = "summarize"
            st.session_state['generating_summary'] = True
            st.session_state['summary_patient_id'] = patient_id
            st.rerun()  # Rerun immediately to start generation
        else:
            st.warning("Please enter a patient ID first.")
            return
    
    # Check if we're continuing from a previous button click (after rerun)
    generating = st.session_state.get('generating_summary', False)
    stored_patient_id = st.session_state.get('summary_patient_id', '')
    
    # If continuing from rerun, use stored patient_id
    if generating and stored_patient_id:
        patient_id = stored_patient_id
    
    # Determine if we should generate (continuing from rerun)
    should_generate = generating and stored_patient_id
    
    # Check if we have a cached summary for this patient
    cached_summary_key = f"summary_{patient_id.strip()}" if patient_id else None
    cached_summary = st.session_state.get(cached_summary_key) if cached_summary_key else None
    
    # Display cached summary if available and no new generation requested
    if cached_summary and not should_generate and patient_id:
        summary_json = cached_summary
        st.info(f"📋 Showing previously generated summary for Patient {patient_id}")
        st.markdown("---")
        
        # Download PDF button for cached summary (at the start)
        try:
            pdf_bytes = create_pdf_from_json(summary_json)
            st.download_button(
                label="📄 Download Summary as PDF",
                data=pdf_bytes,
                file_name=f"patient_{patient_id.strip()}_Summary.pdf",
                mime="application/pdf",
                use_container_width=True
            )
        except Exception as e:
            logger.error(f"Error creating PDF: {e}", exc_info=True)
            st.error(f"Error creating PDF: {str(e)}")
        
        st.markdown("---")
        
        # Display the cached summary (same format as newly generated)
        # Patient Demographics
        if summary_json.get('patient_demographics'):
            st.markdown("#### 👤 Patient Demographics")
            demo = summary_json['patient_demographics']
            
            # Use a table format for better text wrapping and no truncation
            demo_data = {
                "Age": demo.get('age', 'N/A'),
                "Gender": demo.get('gender', 'N/A'),
                "Race": demo.get('race', 'N/A'),
                "Total Admissions": demo.get('total_admissions', 'N/A')
            }
            
            # Display in a 2x2 grid format with better spacing
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Age:** {demo_data['Age']}")
                st.markdown(f"**Gender:** {demo_data['Gender']}")
            with col2:
                st.markdown(f"**Race:** {demo_data['Race']}")
                st.markdown(f"**Total Admissions:** {demo_data['Total Admissions']}")
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
    
    if should_generate and patient_id:
        if not patient_id.strip().isdigit():
            st.error("Please enter a valid numeric patient ID")
            # Clear generation flags on error
            st.session_state['generating_summary'] = False
            st.session_state['summary_patient_id'] = ''
            return
        
        # Get API keys from session state or sidebar
        api_keys = st.session_state.get('gemini_api_keys', [])
        if not api_keys:
            st.error("⚠️ No Gemini API keys configured. Please add API keys in the sidebar.")
            # Clear generation flags on error
            st.session_state['generating_summary'] = False
            st.session_state['summary_patient_id'] = ''
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
                graph_data = retrieve_patient_kg(connection, patient_id.strip())
                
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
                
                # Store summary in session state to persist across downloads
                st.session_state[cached_summary_key] = summary_json
                
                # Clear generation flags after successful generation
                st.session_state['generating_summary'] = False
                st.session_state['summary_patient_id'] = ''
                
                # Display summary
                st.success("✅ Summary generated successfully!")
                st.markdown("---")
                
                # Download PDF button (at the start)
                try:
                    pdf_bytes = create_pdf_from_json(summary_json)
                    st.download_button(
                        label="📄 Download Summary as PDF",
                        data=pdf_bytes,
                        file_name=f"patient_{patient_id.strip()}_Summary.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                except Exception as e:
                    logger.error(f"Error creating PDF: {e}", exc_info=True)
                    st.error(f"Error creating PDF: {str(e)}")
                
                st.markdown("---")
                
                # Patient Demographics
                if summary_json.get('patient_demographics'):
                    st.markdown("#### 👤 Patient Demographics")
                    demo = summary_json['patient_demographics']
                    
                    # Use a table format for better text wrapping and no truncation
                    demo_data = {
                        "Age": demo.get('age', 'N/A'),
                        "Gender": demo.get('gender', 'N/A'),
                        "Race": demo.get('race', 'N/A'),
                        "Total Admissions": demo.get('total_admissions', 'N/A')
                    }
                    
                    # Display in a 2x2 grid format with better spacing
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Age:** {demo_data['Age']}")
                        st.markdown(f"**Gender:** {demo_data['Gender']}")
                    with col2:
                        st.markdown(f"**Race:** {demo_data['Race']}")
                        st.markdown(f"**Total Admissions:** {demo_data['Total Admissions']}")
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
                
            except Exception as e:
                st.error(f"Error generating summary: {str(e)}")
                logger.error(f"Error in summary generation: {e}", exc_info=True)
                # Clear generation flags on error
                st.session_state['generating_summary'] = False
                st.session_state['summary_patient_id'] = ''
    
    elif should_generate and not patient_id:
        st.warning("Please enter a patient ID first.")

