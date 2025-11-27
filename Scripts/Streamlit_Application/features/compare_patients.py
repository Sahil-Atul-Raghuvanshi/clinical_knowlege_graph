"""
Patient Comparison Feature Module
Handles comparing two patients using AI-powered analysis of their knowledge graphs
"""
import streamlit as st
import json
import re
import time
import logging
from typing import Dict, List, Any, Optional, Tuple

from utils.neo4j_connection import Neo4jConnection
from load_data.retrieve_patient_kg import retrieve_patient_kg
from features.download_comparison import create_pdf_from_comparison_json

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


def get_llm_comparison(
    patient1_data: Dict[str, Any], 
    patient1_id: str,
    patient2_data: Dict[str, Any], 
    patient2_id: str,
    api_keys: List[str], 
    max_retries: int = 3
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Send both patients' graph structures to LLM and get structured JSON comparison
    
    Args:
        patient1_data: Graph structure dictionary for first patient
        patient1_id: First patient ID
        patient2_data: Graph structure dictionary for second patient
        patient2_id: Second patient ID
        api_keys: List of Gemini API keys
        max_retries: Maximum number of retry attempts
        
    Returns:
        Tuple of (comparison JSON dict, error message). Comparison is None if error occurred.
    """
    logger.info(f"Sending graph structures to LLM for comparing patients {patient1_id} and {patient2_id}...")
    
    if not api_keys:
        return None, "No Gemini API keys configured. Please add API keys in the sidebar."
    
    # Convert graph data to JSON string for prompt
    patient1_json_str = json.dumps(patient1_data, indent=2, default=str)
    patient2_json_str = json.dumps(patient2_data, indent=2, default=str)
    
    prompt = f"""You are a medical comparison expert. You will receive TWO KNOWLEDGE GRAPHS representing two different patients' clinical journeys from a Neo4j database. Each graph contains nodes (entities) and relationships showing how different clinical events are connected.

KNOWLEDGE GRAPH STRUCTURE (for each patient):
- "patient": Core patient information with demographics
- "nodes": All clinical entities (admissions, diagnoses, procedures, lab tests, medications, etc.) with their properties
- "relationships": Connections showing how entities relate (e.g., Patient -> HAS_ADMISSION -> HospitalAdmission)
- "temporal_events": Chronologically ordered list of all events with timestamps, showing what happened when in the patient's journey

YOUR TASK:
Analyze both knowledge graphs and create a comprehensive comparison that identifies:
1. Similarities between the two patients (demographics, diagnoses, treatments, outcomes, etc.)
2. Key differences between the two patients (presentation, clinical course, interventions, outcomes, etc.)
3. Temporal patterns - compare the sequence and timing of events in both patients' journeys
4. Clinical significance of similarities and differences

IMPORTANT OUTPUT FORMAT REQUIREMENTS:
- You MUST return ONLY a valid JSON object
- Do NOT include any markdown formatting, code blocks, or backticks
- Do NOT include ```json or ``` in your response
- Return ONLY the raw JSON object starting with {{ and ending with }}
- Do NOT use trailing commas in arrays or objects

The JSON structure must be EXACTLY as follows:
{{
  "patient1_id": "string",
  "patient2_id": "string",
  "comparison_summary": "A 2-3 sentence overview highlighting the most significant similarities and differences (max 150 words)",
  "demographics_comparison": {{
    "similarities": ["Similarity 1", "Similarity 2"],
    "differences": ["Difference 1", "Difference 2"]
  }},
  "presentation_comparison": {{
    "similarities": ["How they presented similarly"],
    "differences": ["How they presented differently"],
    "temporal_differences": "Comparison of when/how quickly they presented (max 100 words)"
  }},
  "diagnoses_comparison": {{
    "common_diagnoses": ["Diagnosis 1", "Diagnosis 2"],
    "unique_to_patient1": ["Diagnosis only in patient 1"],
    "unique_to_patient2": ["Diagnosis only in patient 2"],
    "severity_comparison": "Comparison of diagnosis severity and complexity (max 100 words)"
  }},
  "clinical_course_comparison": {{
    "similarities": ["Similar progression patterns, treatments, or responses"],
    "differences": ["Different progression patterns, treatments, or responses"],
    "temporal_sequence_comparison": "Detailed comparison of the chronological sequence of events, highlighting when key events occurred relative to admission and how timing differed between patients (max 200 words)",
    "length_of_stay_comparison": "Comparison of hospital stay duration and unit transitions (max 100 words)"
  }},
  "procedures_comparison": {{
    "common_procedures": ["Procedure both underwent"],
    "unique_to_patient1": ["Procedure only for patient 1"],
    "unique_to_patient2": ["Procedure only for patient 2"],
    "timing_comparison": "Comparison of when procedures were performed relative to admission and each other (max 100 words)"
  }},
  "medications_comparison": {{
    "common_medications": ["Medication both received"],
    "unique_to_patient1": ["Medication only for patient 1"],
    "unique_to_patient2": ["Medication only for patient 2"],
    "timing_comparison": "Comparison of medication initiation, changes, and discontinuation timing (max 100 words)"
  }},
  "lab_findings_comparison": {{
    "similar_abnormalities": ["Lab finding both had"],
    "unique_abnormalities_patient1": ["Abnormal lab only in patient 1"],
    "unique_abnormalities_patient2": ["Abnormal lab only in patient 2"],
    "temporal_patterns": "Comparison of how lab values changed over time in both patients (max 150 words)"
  }},
  "microbiology_comparison": {{
    "common_findings": ["Microbiology finding both had"],
    "unique_to_patient1": ["Finding only in patient 1"],
    "unique_to_patient2": ["Finding only in patient 2"]
  }},
  "outcomes_comparison": {{
    "discharge_comparison": "Comparison of discharge disposition, condition, and activity status (max 100 words)",
    "recovery_trajectory": "Comparison of recovery patterns and timelines (max 100 words)",
    "key_differences": ["Outcome difference 1", "Outcome difference 2"]
  }},
  "temporal_analysis": {{
    "event_sequence_comparison": "Detailed side-by-side comparison of major events in chronological order, showing when each event occurred in both patients' journeys (max 250 words)",
    "critical_timepoints": "Comparison of critical timepoints (e.g., time to diagnosis, time to treatment, time to improvement) (max 150 words)",
    "timing_patterns": "Analysis of temporal patterns - were events clustered differently, did one patient progress faster/slower, etc. (max 150 words)"
  }},
  "clinical_insights": {{
    "why_similar": "Analysis of why these patients are similar (underlying factors, risk factors, disease patterns) (max 150 words)",
    "why_different": "Analysis of why these patients differ (age, comorbidities, disease severity, response to treatment) (max 150 words)",
    "lessons_learned": "Clinical insights and potential lessons from comparing these two cases (max 150 words)"
  }}
}}

GUIDELINES:
1. Keep the total word count around 2000-2500 words
2. Focus on clinically significant similarities and differences
3. Use clear, professional medical language
4. Pay special attention to temporal_events arrays - they provide chronological timelines for both patients
5. Compare the SEQUENCE and TIMING of events, not just what happened
6. Look for timestamp fields (admittime, charttime, starttime, etc.) to maintain chronological flow
7. Extract key information from node properties (diagnoses from Diagnosis nodes, procedures from Procedures nodes, etc.)
8. Highlight abnormal lab findings (look for [abnormal] markers in lab_results)
9. Be comprehensive but concise
10. Remove any placeholder text like [redacted], ___, or [hospital contact]
11. If a section has no data, use empty string "" or empty array []
12. For temporal comparisons, create a clear narrative that shows the progression of both patients side-by-side
13. Identify patterns in timing - did one patient progress faster? Were interventions timed differently?
14. Compare the duration and sequence of key clinical milestones

PATIENT 1 KNOWLEDGE GRAPH DATA:
{patient1_json_str}

PATIENT 2 KNOWLEDGE GRAPH DATA:
{patient2_json_str}

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
            comparison_json = json.loads(response_text)
            
            logger.info("Successfully received and parsed LLM comparison")
            return comparison_json, None
            
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
                return None, f"Error generating comparison: {str(e)}"
            
            time.sleep(5)
    
    return None, f"Failed to get LLM comparison after {max_retries} attempts"


def render_comparison_tab(connection: Neo4jConnection):
    """
    Render the patient comparison tab in Streamlit
    
    Args:
        connection: Neo4j connection object
    """
    st.markdown("### 🔬 Compare Two Patients")
    st.info("Enter two patient IDs to generate an AI-powered comparison of their clinical journeys.")
    
    # Patient ID inputs
    col1, col2 = st.columns(2)
    
    with col1:
        patient1_id = st.text_input(
            "Patient 1 ID:",
            placeholder="e.g., 10000032",
            key="compare_patient1_input"
        )
    
    with col2:
        patient2_id = st.text_input(
            "Patient 2 ID:",
            placeholder="e.g., 10000033",
            key="compare_patient2_input"
        )
    
    # Generate button
    generate_button = st.button("🤖 Generate Comparison", type="primary", use_container_width=True, key="generate_comparison_button")
    
    # If button is clicked, set query param to preserve tab state and store patient IDs
    if generate_button:
        if patient1_id and patient2_id:
            if patient1_id.strip() == patient2_id.strip():
                st.warning("Please enter two different patient IDs.")
                return
            st.query_params.tab = "compare"
            st.session_state['generating_comparison'] = True
            st.session_state['comparison_patient1_id'] = patient1_id
            st.session_state['comparison_patient2_id'] = patient2_id
            st.rerun()  # Rerun immediately to start generation
        else:
            st.warning("Please enter both patient IDs.")
            return
    
    # Check if we're continuing from a previous button click (after rerun)
    generating = st.session_state.get('generating_comparison', False)
    stored_patient1_id = st.session_state.get('comparison_patient1_id', '')
    stored_patient2_id = st.session_state.get('comparison_patient2_id', '')
    
    # If continuing from rerun, use stored patient IDs
    if generating and stored_patient1_id and stored_patient2_id:
        patient1_id = stored_patient1_id
        patient2_id = stored_patient2_id
    
    # Determine if we should generate (continuing from rerun)
    should_generate = generating and stored_patient1_id and stored_patient2_id
    
    # Check if we have a cached comparison for these patients
    cache_key = f"comparison_{stored_patient1_id.strip()}_{stored_patient2_id.strip()}" if (stored_patient1_id and stored_patient2_id) else None
    cached_comparison = st.session_state.get(cache_key) if cache_key else None
    
    # Display cached comparison if available and no new generation requested
    if cached_comparison and not should_generate and patient1_id and patient2_id:
        comparison_json = cached_comparison
        st.info(f"🔬 Showing previously generated comparison for Patient {patient1_id} vs Patient {patient2_id}")
        st.markdown("---")
        
        # Download PDF button for cached comparison (at the start)
        try:
            pdf_bytes = create_pdf_from_comparison_json(comparison_json)
            st.download_button(
                label="📄 Download Comparison as PDF",
                data=pdf_bytes,
                file_name=f"patient_{patient1_id.strip()}_vs_{patient2_id.strip()}_Comparison.pdf",
                mime="application/pdf",
                use_container_width=True
            )
        except Exception as e:
            logger.error(f"Error creating PDF: {e}", exc_info=True)
            st.error(f"Error creating PDF: {str(e)}")
        
        st.markdown("---")
        _display_comparison_results(comparison_json, patient1_id, patient2_id)
    
    if should_generate and patient1_id and patient2_id:
        if not patient1_id.strip().isdigit() or not patient2_id.strip().isdigit():
            st.error("Please enter valid numeric patient IDs")
            # Clear generation flags on error
            st.session_state['generating_comparison'] = False
            st.session_state['comparison_patient1_id'] = ''
            st.session_state['comparison_patient2_id'] = ''
            return
        
        # Get API keys from session state or sidebar
        api_keys = st.session_state.get('gemini_api_keys', [])
        if not api_keys:
            st.error("⚠️ No Gemini API keys configured. Please add API keys in the sidebar.")
            # Clear generation flags on error
            st.session_state['generating_comparison'] = False
            st.session_state['comparison_patient1_id'] = ''
            st.session_state['comparison_patient2_id'] = ''
            return
        
        # Extract graph structures for both patients
        with st.spinner("Extracting knowledge graph structures for both patients..."):
            try:
                # Check if patients exist
                check_query = """
                MATCH (p:Patient)
                WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
                RETURN p.subject_id as subject_id
                """
                
                result1 = connection.execute_query(check_query, {"subject_id": str(patient1_id)})
                result2 = connection.execute_query(check_query, {"subject_id": str(patient2_id)})
                
                if not result1:
                    st.error(f"Patient {patient1_id} not found in database!")
                    st.session_state['generating_comparison'] = False
                    st.session_state['comparison_patient1_id'] = ''
                    st.session_state['comparison_patient2_id'] = ''
                    return
                
                if not result2:
                    st.error(f"Patient {patient2_id} not found in database!")
                    st.session_state['generating_comparison'] = False
                    st.session_state['comparison_patient1_id'] = ''
                    st.session_state['comparison_patient2_id'] = ''
                    return
                
                # Extract graph structures
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("Extracting Patient 1 knowledge graph...")
                patient1_data = retrieve_patient_kg(connection, patient1_id.strip())
                progress_bar.progress(50)
                
                status_text.text("Extracting Patient 2 knowledge graph...")
                patient2_data = retrieve_patient_kg(connection, patient2_id.strip())
                progress_bar.progress(100)
                
                status_text.text("Knowledge graphs extracted successfully!")
                
                if len(patient1_data.get('relationships', [])) == 0:
                    st.warning(f"Patient {patient1_id} has no relationships in the graph. Limited data available.")
                
                if len(patient2_data.get('relationships', [])) == 0:
                    st.warning(f"Patient {patient2_id} has no relationships in the graph. Limited data available.")
                
            except Exception as e:
                st.error(f"Error extracting graph structures: {str(e)}")
                logger.error(f"Error extracting graphs: {e}", exc_info=True)
                st.session_state['generating_comparison'] = False
                st.session_state['comparison_patient1_id'] = ''
                st.session_state['comparison_patient2_id'] = ''
                return
        
        with st.spinner("🤖 Generating AI comparison (this may take a minute)..."):
            try:
                comparison_json, error = get_llm_comparison(
                    patient1_data, 
                    patient1_id.strip(),
                    patient2_data, 
                    patient2_id.strip(),
                    api_keys
                )
                
                if error:
                    st.error(f"Error: {error}")
                    st.session_state['generating_comparison'] = False
                    st.session_state['comparison_patient1_id'] = ''
                    st.session_state['comparison_patient2_id'] = ''
                    return
                
                if not comparison_json:
                    st.error("Failed to generate comparison")
                    st.session_state['generating_comparison'] = False
                    st.session_state['comparison_patient1_id'] = ''
                    st.session_state['comparison_patient2_id'] = ''
                    return
                
                # Store comparison in session state to persist
                st.session_state[cache_key] = comparison_json
                
                # Clear generation flags after successful generation
                st.session_state['generating_comparison'] = False
                st.session_state['comparison_patient1_id'] = ''
                st.session_state['comparison_patient2_id'] = ''
                
                # Display comparison
                st.success("✅ Comparison generated successfully!")
                st.markdown("---")
                
                # Download PDF button (at the start)
                try:
                    pdf_bytes = create_pdf_from_comparison_json(comparison_json)
                    st.download_button(
                        label="📄 Download Comparison as PDF",
                        data=pdf_bytes,
                        file_name=f"patient_{patient1_id.strip()}_vs_{patient2_id.strip()}_Comparison.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                except Exception as e:
                    logger.error(f"Error creating PDF: {e}", exc_info=True)
                    st.error(f"Error creating PDF: {str(e)}")
                
                st.markdown("---")
                _display_comparison_results(comparison_json, patient1_id.strip(), patient2_id.strip())
                
            except Exception as e:
                st.error(f"Error generating comparison: {str(e)}")
                logger.error(f"Error in comparison generation: {e}", exc_info=True)
                # Clear generation flags on error
                st.session_state['generating_comparison'] = False
                st.session_state['comparison_patient1_id'] = ''
                st.session_state['comparison_patient2_id'] = ''
    
    elif should_generate and (not patient1_id or not patient2_id):
        st.warning("Please enter both patient IDs.")


def _display_comparison_results(comparison_json: Dict[str, Any], patient1_id: str, patient2_id: str):
    """
    Display the comparison results in a formatted way
    
    Args:
        comparison_json: The comparison JSON dictionary
        patient1_id: First patient ID
        patient2_id: Second patient ID
    """
    # Comparison Summary
    if comparison_json.get('comparison_summary'):
        st.markdown("#### 📊 Comparison Summary")
        st.write(comparison_json['comparison_summary'])
        st.markdown("---")
    
    # Demographics Comparison
    if comparison_json.get('demographics_comparison'):
        demo_comp = comparison_json['demographics_comparison']
        st.markdown("#### 👤 Demographics Comparison")
        
        if demo_comp.get('similarities'):
            st.markdown("**Similarities:**")
            for similarity in demo_comp['similarities']:
                st.write(f"• {similarity}")
        
        if demo_comp.get('differences'):
            st.markdown("**Differences:**")
            for difference in demo_comp['differences']:
                st.write(f"• {difference}")
        
        st.markdown("---")
    
    # Presentation Comparison
    if comparison_json.get('presentation_comparison'):
        pres_comp = comparison_json['presentation_comparison']
        st.markdown("#### 🏥 Presentation Comparison")
        
        if pres_comp.get('similarities'):
            st.markdown("**Similarities:**")
            for similarity in pres_comp['similarities']:
                st.write(f"• {similarity}")
        
        if pres_comp.get('differences'):
            st.markdown("**Differences:**")
            for difference in pres_comp['differences']:
                st.write(f"• {difference}")
        
        if pres_comp.get('temporal_differences'):
            st.markdown("**Temporal Differences:**")
            st.write(pres_comp['temporal_differences'])
        
        st.markdown("---")
    
    # Diagnoses Comparison
    if comparison_json.get('diagnoses_comparison'):
        diag_comp = comparison_json['diagnoses_comparison']
        st.markdown("#### 🩺 Diagnoses Comparison")
        
        if diag_comp.get('common_diagnoses'):
            st.markdown("**Common Diagnoses:**")
            for diag in diag_comp['common_diagnoses']:
                st.write(f"• {diag}")
        
        col1, col2 = st.columns(2)
        with col1:
            if diag_comp.get('unique_to_patient1'):
                st.markdown(f"**Unique to Patient {patient1_id}:**")
                for diag in diag_comp['unique_to_patient1']:
                    st.write(f"• {diag}")
        
        with col2:
            if diag_comp.get('unique_to_patient2'):
                st.markdown(f"**Unique to Patient {patient2_id}:**")
                for diag in diag_comp['unique_to_patient2']:
                    st.write(f"• {diag}")
        
        if diag_comp.get('severity_comparison'):
            st.markdown("**Severity Comparison:**")
            st.write(diag_comp['severity_comparison'])
        
        st.markdown("---")
    
    # Clinical Course Comparison
    if comparison_json.get('clinical_course_comparison'):
        course_comp = comparison_json['clinical_course_comparison']
        st.markdown("#### 📈 Clinical Course Comparison")
        
        if course_comp.get('similarities'):
            st.markdown("**Similarities:**")
            for similarity in course_comp['similarities']:
                st.write(f"• {similarity}")
        
        if course_comp.get('differences'):
            st.markdown("**Differences:**")
            for difference in course_comp['differences']:
                st.write(f"• {difference}")
        
        if course_comp.get('temporal_sequence_comparison'):
            st.markdown("**Temporal Sequence Comparison:**")
            st.write(course_comp['temporal_sequence_comparison'])
        
        if course_comp.get('length_of_stay_comparison'):
            st.markdown("**Length of Stay Comparison:**")
            st.write(course_comp['length_of_stay_comparison'])
        
        st.markdown("---")
    
    # Procedures Comparison
    if comparison_json.get('procedures_comparison'):
        proc_comp = comparison_json['procedures_comparison']
        st.markdown("#### 🔬 Procedures Comparison")
        
        if proc_comp.get('common_procedures'):
            st.markdown("**Common Procedures:**")
            for proc in proc_comp['common_procedures']:
                st.write(f"• {proc}")
        
        col1, col2 = st.columns(2)
        with col1:
            if proc_comp.get('unique_to_patient1'):
                st.markdown(f"**Unique to Patient {patient1_id}:**")
                for proc in proc_comp['unique_to_patient1']:
                    st.write(f"• {proc}")
        
        with col2:
            if proc_comp.get('unique_to_patient2'):
                st.markdown(f"**Unique to Patient {patient2_id}:**")
                for proc in proc_comp['unique_to_patient2']:
                    st.write(f"• {proc}")
        
        if proc_comp.get('timing_comparison'):
            st.markdown("**Timing Comparison:**")
            st.write(proc_comp['timing_comparison'])
        
        st.markdown("---")
    
    # Medications Comparison
    if comparison_json.get('medications_comparison'):
        med_comp = comparison_json['medications_comparison']
        st.markdown("#### 💊 Medications Comparison")
        
        if med_comp.get('common_medications'):
            st.markdown("**Common Medications:**")
            for med in med_comp['common_medications']:
                st.write(f"• {med}")
        
        col1, col2 = st.columns(2)
        with col1:
            if med_comp.get('unique_to_patient1'):
                st.markdown(f"**Unique to Patient {patient1_id}:**")
                for med in med_comp['unique_to_patient1']:
                    st.write(f"• {med}")
        
        with col2:
            if med_comp.get('unique_to_patient2'):
                st.markdown(f"**Unique to Patient {patient2_id}:**")
                for med in med_comp['unique_to_patient2']:
                    st.write(f"• {med}")
        
        if med_comp.get('timing_comparison'):
            st.markdown("**Timing Comparison:**")
            st.write(med_comp['timing_comparison'])
        
        st.markdown("---")
    
    # Lab Findings Comparison
    if comparison_json.get('lab_findings_comparison'):
        lab_comp = comparison_json['lab_findings_comparison']
        st.markdown("#### 🧪 Laboratory Findings Comparison")
        
        if lab_comp.get('similar_abnormalities'):
            st.markdown("**Similar Abnormalities:**")
            for finding in lab_comp['similar_abnormalities']:
                st.write(f"• {finding}")
        
        col1, col2 = st.columns(2)
        with col1:
            if lab_comp.get('unique_abnormalities_patient1'):
                st.markdown(f"**Unique to Patient {patient1_id}:**")
                for finding in lab_comp['unique_abnormalities_patient1']:
                    st.write(f"• {finding}")
        
        with col2:
            if lab_comp.get('unique_abnormalities_patient2'):
                st.markdown(f"**Unique to Patient {patient2_id}:**")
                for finding in lab_comp['unique_abnormalities_patient2']:
                    st.write(f"• {finding}")
        
        if lab_comp.get('temporal_patterns'):
            st.markdown("**Temporal Patterns:**")
            st.write(lab_comp['temporal_patterns'])
        
        st.markdown("---")
    
    # Microbiology Comparison
    if comparison_json.get('microbiology_comparison'):
        micro_comp = comparison_json['microbiology_comparison']
        st.markdown("#### 🦠 Microbiology Comparison")
        
        if micro_comp.get('common_findings'):
            st.markdown("**Common Findings:**")
            for finding in micro_comp['common_findings']:
                st.write(f"• {finding}")
        
        col1, col2 = st.columns(2)
        with col1:
            if micro_comp.get('unique_to_patient1'):
                st.markdown(f"**Unique to Patient {patient1_id}:**")
                for finding in micro_comp['unique_to_patient1']:
                    st.write(f"• {finding}")
        
        with col2:
            if micro_comp.get('unique_to_patient2'):
                st.markdown(f"**Unique to Patient {patient2_id}:**")
                for finding in micro_comp['unique_to_patient2']:
                    st.write(f"• {finding}")
        
        st.markdown("---")
    
    # Outcomes Comparison
    if comparison_json.get('outcomes_comparison'):
        outcome_comp = comparison_json['outcomes_comparison']
        st.markdown("#### 🏥 Outcomes Comparison")
        
        if outcome_comp.get('discharge_comparison'):
            st.markdown("**Discharge Comparison:**")
            st.write(outcome_comp['discharge_comparison'])
        
        if outcome_comp.get('recovery_trajectory'):
            st.markdown("**Recovery Trajectory:**")
            st.write(outcome_comp['recovery_trajectory'])
        
        if outcome_comp.get('key_differences'):
            st.markdown("**Key Differences:**")
            for diff in outcome_comp['key_differences']:
                st.write(f"• {diff}")
        
        st.markdown("---")
    
    # Temporal Analysis
    if comparison_json.get('temporal_analysis'):
        temp_analysis = comparison_json['temporal_analysis']
        st.markdown("#### ⏱️ Temporal Analysis")
        
        if temp_analysis.get('event_sequence_comparison'):
            st.markdown("**Event Sequence Comparison:**")
            st.write(temp_analysis['event_sequence_comparison'])
        
        if temp_analysis.get('critical_timepoints'):
            st.markdown("**Critical Timepoints:**")
            st.write(temp_analysis['critical_timepoints'])
        
        if temp_analysis.get('timing_patterns'):
            st.markdown("**Timing Patterns:**")
            st.write(temp_analysis['timing_patterns'])
        
        st.markdown("---")
    
    # Clinical Insights
    if comparison_json.get('clinical_insights'):
        insights = comparison_json['clinical_insights']
        st.markdown("#### 💡 Clinical Insights")
        
        if insights.get('why_similar'):
            st.markdown("**Why These Patients Are Similar:**")
            st.write(insights['why_similar'])
        
        if insights.get('why_different'):
            st.markdown("**Why These Patients Differ:**")
            st.write(insights['why_different'])
        
        if insights.get('lessons_learned'):
            st.markdown("**Lessons Learned:**")
            st.write(insights['lessons_learned'])

