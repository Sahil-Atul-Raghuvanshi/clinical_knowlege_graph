"""
Chronological Patient Journey Feature Module
Handles displaying patient journey in chronological order
"""
import streamlit as st
import pandas as pd
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from utils.neo4j_connection import Neo4jConnection
from load_data.retrieve_patient_kg import extract_timestamp

logger = logging.getLogger(__name__)


def get_child_nodes(connection: Neo4jConnection, parent_node_id: str, parent_props: dict) -> Dict[str, List[Tuple[dict, str]]]:
    """
    Get child nodes that don't have timestamps
    Excludes item nodes (DiagnosisItem, MedicationItem, LabResultItem, MicrobiologyResultItem)
    Matches the logic from create_patient_journey_pdf.py get_child_nodes function
    
    Args:
        connection: Neo4j connection object
        parent_node_id: Element ID of parent node
        parent_props: Properties of parent node (not used but kept for compatibility)
        
    Returns:
        Dictionary mapping child label to list of (child_props, relationship_type) tuples
    """
    query = """
    MATCH (parent)-[r]->(child)
    WHERE elementId(parent) = $node_id
      AND NOT child:DiagnosisItem 
      AND NOT child:MedicationItem 
      AND NOT child:LabResultItem 
      AND NOT child:MicrobiologyResultItem
    RETURN labels(child) as labels, properties(child) as props, type(r) as relationship_type
    """
    
    try:
        results = connection.execute_query(query, {
            "node_id": parent_node_id
        })
        
        children_dict = {}
        for record in results:
            child_labels = list(record.get('labels', []))
            child_props = dict(record.get('props', {}))
            rel_type = record.get('relationship_type', '')
            
            if child_labels:
                child_label = child_labels[0]
                # Only include child nodes that don't have timestamps (matching create_patient_journey_pdf.py logic)
                if extract_timestamp(child_props, child_labels) is None:
                    if child_label not in children_dict:
                        children_dict[child_label] = []
                    children_dict[child_label].append((child_props, rel_type))
        
        return children_dict
    except Exception as e:
        logger.error(f"Error getting child nodes: {e}", exc_info=True)
        return {}


def get_patient_journey_data(connection: Neo4jConnection, subject_id: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get all patient journey events in chronological order using complete knowledge graph
    Uses the same query logic as create_patient_journey_pdf.py to get all nodes with timestamps
    
    Args:
        connection: Neo4j connection object
        subject_id: Patient ID
        
    Returns:
        Tuple of (journey_data dict, error message). journey_data is None if error occurred.
    """
    try:
        # Get patient node first
        patient_query = """
        MATCH (p:Patient)
        WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
        RETURN labels(p) as labels, properties(p) as props
        """
        
        patient_results = connection.execute_query(patient_query, {"subject_id": str(subject_id)})
        
        if not patient_results:
            return None, f"Patient {subject_id} not found in database"
        
        patient_record = patient_results[0]
        patient_labels = list(patient_record.get('labels', []))
        patient_props = dict(patient_record.get('props', {}))
        
        # Use the same query as create_patient_journey_pdf.py to get all nodes
        # This query gets all nodes reachable from the patient, matching the PDF generation logic
        nodes_query = """
        MATCH (p:Patient)
        WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
        WITH p
        OPTIONAL MATCH (p)-[*]->(n)
        WHERE n.name IS NOT NULL
        RETURN DISTINCT labels(n) as labels, properties(n) as props, elementId(n) as element_id
        """
        
        results = connection.execute_query(nodes_query, {"subject_id": str(subject_id)})
        
        # Collect and sort nodes by timestamp (matching create_patient_journey_pdf.py logic)
        nodes_with_timestamps = []
        
        for record in results:
            if not record.get('props'):
                continue
            
            labels = list(record.get('labels', []))
            props = dict(record.get('props', {}))
            element_id = record.get('element_id', '')
            
            # Skip item nodes (same as create_patient_journey_pdf.py)
            if any(label in ['DiagnosisItem', 'MedicationItem', 'LabResultItem', 'MicrobiologyResultItem'] for label in labels):
                continue
            
            # Skip Patient node (we already have it)
            if 'Patient' in labels:
                continue
            
            # Extract timestamp using the same logic as create_patient_journey_pdf.py
            timestamp = extract_timestamp(props, labels)
            if timestamp:
                nodes_with_timestamps.append({
                    'labels': labels,
                    'properties': props,
                    'element_id': element_id,
                    'timestamp': timestamp
                })
        
        # Sort by timestamp (strict chronological order)
        nodes_with_timestamps.sort(key=lambda x: x['timestamp'])
        
        logger.info(f"Extracted {len(nodes_with_timestamps)} temporal events for patient {subject_id}")
        
        return {
            'patient': {
                'labels': patient_labels,
                'properties': patient_props
            },
            'events': nodes_with_timestamps
        }, None
        
    except Exception as e:
        logger.error(f"Error getting patient journey data: {e}", exc_info=True)
        return None, str(e)


def ordinal_suffix(n: int) -> str:
    """Convert number to ordinal string (e.g., 1 -> '1st', 2 -> '2nd', 3 -> '3rd')"""
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])


def _create_matrix_dataframe(items: List[str], num_columns: int = None) -> pd.DataFrame:
    """
    Create a matrix-style dataframe with items arranged horizontally in columns
    Data stacked horizontally in a matrix format
    
    Args:
        items: List of items to display
        num_columns: Number of columns (if None, auto-determine based on list length:
                     1 item -> 1 column, 2 items -> 2 columns, 3+ items -> 3 columns)
        
    Returns:
        DataFrame with items arranged in matrix format
    """
    if not items:
        return pd.DataFrame()
    
    # Auto-determine number of columns based on list length
    if num_columns is None:
        if len(items) == 1:
            num_columns = 1
        elif len(items) == 2:
            num_columns = 2
        else:
            num_columns = 3
    
    # Calculate number of rows needed
    num_rows = (len(items) + num_columns - 1) // num_columns
    
    # Create matrix data - use unique column names
    matrix_data = {}
    for col in range(num_columns):
        col_data = []
        for row in range(num_rows):
            idx = row * num_columns + col
            if idx < len(items):
                col_data.append(items[idx])
            else:
                col_data.append('')
        # Use unique column names (will be hidden via column_config)
        matrix_data[str(col)] = col_data
    
    df = pd.DataFrame(matrix_data)
    return df


def format_timestamp(timestamp: datetime) -> str:
    """Format timestamp for display"""
    return timestamp.strftime('%B %d, %Y at %I:%M %p')


def calculate_time_gap(last_timestamp: Optional[datetime], current_timestamp: datetime) -> Optional[str]:
    """Calculate time gap between events"""
    if last_timestamp and current_timestamp:
        delta = current_timestamp - last_timestamp
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        
        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        
        return ", ".join(parts) if parts else "less than a minute"
    return None


def render_ed_visit(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render Emergency Department visit"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            st.markdown(f"*Time gap of **{gap}** since last event*")
            st.markdown("---")
    
    # Header
    ed_seq_num = props.get('ed_seq_num')
    if ed_seq_num is not None:
        st.markdown(f"### Emergency Department Visit #{ed_seq_num}")
    else:
        st.markdown("### Emergency Department Visit")
    
    st.markdown(f"**Admission Time:** {format_timestamp(timestamp)}")
    
    arrival = props.get('arrival_transport', 'N/A')
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    disposition = props.get('disposition', 'N/A')
    
    if ed_seq_num is not None:
        st.write(f"This is the patient's **{ordinal_suffix(ed_seq_num)}** Emergency Department visit. "
                f"The patient arrived via **{arrival}**. "
                f"The patient departed at **{outtime}**, staying for a duration of **{period}**.")
    else:
        st.write(f"The patient arrived at the Emergency Department via **{arrival}**. "
                f"The patient departed at **{outtime}**, staying for a duration of **{period}**.")
    
    # Check if ED discharge
    ed_discharge_keywords = ['HOME', 'DISCHARGED', 'AGAINST ADVICE', 'LEFT', 'AMA']
    is_ed_discharge = any(keyword in disposition.upper() for keyword in ed_discharge_keywords) if disposition != 'N/A' else False
    
    if is_ed_discharge:
        st.warning(f"**ED Disposition:** {disposition} - Patient was discharged directly from Emergency Department without hospital admission.")
    
    # Initial diagnosis - 3-column matrix format
    for child_props, _ in children_dict.get('Diagnosis', []):
        if child_props.get('ed_diagnosis') == 'True':
            diagnoses = child_props.get('complete_diagnosis', [])
            if diagnoses:
                st.markdown("**Initial Diagnosis:**")
                if isinstance(diagnoses, list):
                    diag_matrix = _create_matrix_dataframe(diagnoses)
                    # Configure columns to hide headers - dynamically based on actual columns
                    num_cols = len(diag_matrix.columns)
                    column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                    st.dataframe(diag_matrix, column_config=column_config, width='stretch', hide_index=True)
                else:
                    st.write(diagnoses)
    
    # Initial assessment
    for child_props, _ in children_dict.get('InitialAssessment', []):
        st.markdown("**Initial Assessment:**")
        
        chief_complaint = child_props.get('chiefcomplaint', 'N/A')
        if chief_complaint and chief_complaint != 'N/A':
            st.write(f"**Chief Complaint:** {chief_complaint}")
        
        # Vitals table - use dataframe
        vitals_data = []
        if 'sbp' in child_props and 'dbp' in child_props:
            vitals_data.append({'Vital Sign': 'Blood Pressure', 'Value': f"{child_props['sbp']}/{child_props['dbp']} mmHg"})
        if 'heartrate' in child_props:
            vitals_data.append({'Vital Sign': 'Heart Rate', 'Value': f"{child_props['heartrate']} bpm"})
        if 'resprate' in child_props:
            vitals_data.append({'Vital Sign': 'Respiratory Rate', 'Value': f"{child_props['resprate']} breaths/min"})
        if 'o2sat' in child_props:
            vitals_data.append({'Vital Sign': 'Oxygen Saturation', 'Value': f"{child_props['o2sat']}%"})
        if 'temperature' in child_props:
            vitals_data.append({'Vital Sign': 'Temperature', 'Value': f"{child_props['temperature']}°F"})
        if 'pain' in child_props:
            vitals_data.append({'Vital Sign': 'Pain Score', 'Value': str(child_props['pain'])})
        if 'acuity' in child_props:
            vitals_data.append({'Vital Sign': 'Acuity Level', 'Value': str(child_props['acuity'])})
        
        if vitals_data:
            st.markdown("**Triage Vitals:**")
            df_vitals = pd.DataFrame(vitals_data)
            st.dataframe(df_vitals, use_container_width=True, hide_index=True)
    
    st.markdown("---")


def render_hospital_admission(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render hospital admission"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            st.markdown(f"*Time gap of **{gap}** since last event*")
            st.markdown("---")
    
    # Header
    hadm_seq_num = props.get('hospital_admission_sequence_number')
    if hadm_seq_num is not None:
        st.markdown(f"### Hospital Admission #{hadm_seq_num}")
    else:
        st.markdown("### Hospital Admission")
    
    st.markdown(f"**Admission Time:** {format_timestamp(timestamp)}")
    
    admit_location = props.get('admission_location', 'N/A')
    admit_type = props.get('admission_type', 'N/A')
    provider = props.get('admit_provider_id', 'N/A')
    insurance = props.get('insurance', 'N/A')
    service = props.get('service', 'N/A')
    chief_complaint = props.get('chief_complaint', 'N/A')
    
    if hadm_seq_num is not None:
        st.write(f"This is the patient's **{ordinal_suffix(hadm_seq_num)}** hospital admission. "
                f"The patient was admitted from **{admit_location}** as an **{admit_type}** admission. "
                f"The admitting provider was **{provider}**. Insurance coverage: **{insurance}**. "
                f"Chief complaint: **{chief_complaint}**. The primary service provided was **{service}**.")
    else:
        st.write(f"The patient was admitted to the hospital from **{admit_location}** as an **{admit_type}** admission. "
                f"The admitting provider was **{provider}**. Insurance coverage: **{insurance}**. "
                f"Chief complaint: **{chief_complaint}**. The primary service provided was **{service}**.")
    
    # Demographics
    race = props.get('race', 'N/A')
    marital = props.get('marital_status', 'N/A')
    language = props.get('language', 'N/A')
    
    if race != 'N/A' or marital != 'N/A' or language != 'N/A':
        st.write(f"Race: **{race}**, Marital Status: **{marital}**, Language: **{language}**")
    
    # DRG Codes
    for child_props, _ in children_dict.get('DRG', []):
        drg_type = child_props.get('drg_type', 'Unknown')
        st.markdown(f"**{drg_type} DRG Code:**")
        st.write(f"• Code: {child_props.get('drg_code', 'N/A')}")
        st.write(f"• Description: {child_props.get('description', 'N/A')}")
        if 'drg_severity' in child_props:
            st.write(f"• Severity: {child_props['drg_severity']}")
        if 'drg_mortality' in child_props:
            st.write(f"• Mortality Risk: {child_props['drg_mortality']}")
    
    # Past History
    for child_props, _ in children_dict.get('PatientPastHistory', []):
        st.markdown("**Patient Past History:**")
        st.write(f"• Past Medical History: {child_props.get('past_medical_history', 'N/A')}")
        st.write(f"• Family History: {child_props.get('family_history', 'N/A')}")
        st.write(f"• Social History: {child_props.get('social_history', 'N/A')}")
    
    # HPI Summary
    for child_props, _ in children_dict.get('HPISummary', []):
        st.markdown("**History of Present Illness:**")
        st.write(child_props.get('summary', 'N/A'))
    
    # Admission Vitals - use dataframe
    for child_props, _ in children_dict.get('AdmissionVitals', []):
        st.markdown("**Admission Vital Signs:**")
        vitals_data = [
            {'Vital Sign': 'General Appearance', 'Value': child_props.get('General', 'N/A')},
            {'Vital Sign': 'Blood Pressure', 'Value': f"{child_props.get('Blood_Pressure', 'N/A')} mmHg"},
            {'Vital Sign': 'Heart Rate', 'Value': f"{child_props.get('Heart_Rate', 'N/A')} bpm"},
            {'Vital Sign': 'Respiratory Rate', 'Value': f"{child_props.get('Respiratory_Rate', 'N/A')} breaths/min"},
            {'Vital Sign': 'Temperature', 'Value': f"{child_props.get('Temperature', 'N/A')}°F"},
            {'Vital Sign': 'Oxygen Saturation', 'Value': child_props.get('SpO2', 'N/A')}
        ]
        df_vitals = pd.DataFrame(vitals_data)
        st.dataframe(df_vitals, width='stretch', hide_index=True)
    
    # Admission Labs - 3-column matrix format
    for child_props, _ in children_dict.get('AdmissionLabs', []):
        lab_tests = child_props.get('lab_tests', [])
        if lab_tests:
            st.markdown("**Admission Laboratory Results:**")
            if isinstance(lab_tests, list):
                labs_matrix = _create_matrix_dataframe(lab_tests)
                # Configure columns to hide headers - dynamically based on actual columns
                num_cols = len(labs_matrix.columns)
                column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                st.dataframe(labs_matrix, column_config=column_config, width='stretch', hide_index=True)
            else:
                st.write(lab_tests)
    
    # Admission Medications - 3-column matrix format
    for child_props, _ in children_dict.get('AdmissionMedications', []):
        medications = child_props.get('medications', [])
        if medications:
            st.markdown(f"**Admission Medications:** Total of **{len(medications)}** medications on admission:")
            if isinstance(medications, list):
                meds_matrix = _create_matrix_dataframe(medications)
                # Configure columns to hide headers - dynamically based on actual columns
                num_cols = len(meds_matrix.columns)
                column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                st.dataframe(meds_matrix, column_config=column_config, width='stretch', hide_index=True)
            else:
                st.write(medications)
    
    st.markdown("---")


def render_icu_stay(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render ICU stay"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            st.markdown(f"*Time gap of **{gap}** since last event*")
            st.markdown("---")
    
    careunit = props.get('careunit', 'N/A')
    first_careunit = props.get('first_careunit', careunit)
    last_careunit = props.get('last_careunit', careunit)
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    los = props.get('los', 'N/A')
    service = props.get('service_given', 'N/A')
    
    st.markdown(f"### ICU Admission - {careunit}")
    st.markdown(f"**ICU Admission Time:** {format_timestamp(timestamp)}")
    
    text = f"The patient was admitted to the **Intensive Care Unit ({careunit})**."
    if first_careunit != last_careunit:
        text += f" The patient was transferred within ICU from **{first_careunit}** to **{last_careunit}**."
    if service != 'N/A':
        text += f" Service provided: **{service}**."
    text += f" The patient remained in ICU until **{outtime}**, for a total ICU stay of **{period}**"
    if los != 'N/A':
        text += f" (Length of Stay: **{los} days**)"
    text += ". During this critical care period, intensive monitoring and interventions were performed."
    
    st.write(text)
    st.markdown("---")


def render_unit_admission(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render regular ward/unit admission - only admission info, no child events"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            st.markdown(f"*Time gap of **{gap}** since last event*")
            st.markdown("---")
    
    careunit = props.get('careunit', 'N/A')
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    service = props.get('service_given', 'N/A')
    
    st.markdown(f"### Unit Admission - {careunit}")
    st.markdown(f"**Admission Time:** {format_timestamp(timestamp)}")
    
    text = f"The patient was admitted to **{careunit}** ward."
    if service != 'N/A':
        text += f" Service provided: **{service}**."
    text += f" The patient stayed in this unit until **{outtime}**, for a total duration of **{period}**."
    
    st.write(text)
    st.markdown("---")


def parse_lab_result(result_str):
    """Parse a lab result string into components"""
    parts = result_str.split('=', 1)
    if len(parts) < 2:
        return None
    
    test_name = parts[0].strip()
    rest = parts[1].strip()
    
    value = ""
    ref_range = ""
    is_abnormal = "[abnormal]" in rest
    specimen = ""
    category = ""
    
    rest = rest.replace('[abnormal]', '').strip()
    
    if ',' in rest:
        temp_parts = rest.rsplit(',', 1)
        if len(temp_parts) == 2:
            category = temp_parts[1].strip()
            rest = temp_parts[0].strip()
            
            if ',' in rest:
                temp_parts2 = rest.rsplit(',', 1)
                if len(temp_parts2) == 2:
                    specimen = temp_parts2[1].strip()
                    rest = temp_parts2[0].strip()
    
    if '(ref:' in rest:
        ref_parts = rest.split('(ref:', 1)
        value_part = ref_parts[0].strip()
        ref_part = ref_parts[1].strip()
        
        if ')' in ref_part:
            ref_range = ref_part.split(')', 1)[0].strip()
        
        value = value_part
    else:
        value = rest
    
    return {
        'test_name': test_name,
        'value': value,
        'ref_range': ref_range,
        'is_abnormal': is_abnormal,
        'specimen': specimen,
        'category': category
    }


def render_lab_event(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render laboratory event with table format"""
    props = event['properties']
    timestamp = event['timestamp']
    
    abnormal_count = props.get('abnormal_count', 0)
    lab_count = props.get('lab_count', 0)
    lab_results = props.get('lab_results', [])
    
    st.markdown(f"**🧪 Laboratory Tests Performed:** {format_timestamp(timestamp)}")
    st.write(f"Total of **{lab_count}** tests performed, with **{abnormal_count}** abnormal result(s).")
    
    if lab_results:
        # Group by category
        grouped_results = {}
        
        for result in lab_results:
            parsed = parse_lab_result(result)
            if parsed:
                category = parsed['category'] if parsed['category'] else 'Other'
                
                if category not in grouped_results:
                    grouped_results[category] = []
                grouped_results[category].append(parsed)
        
        # Display tables for each category - show ALL results (no truncation)
        for category, results in grouped_results.items():
            if len(results) > 0:
                st.markdown(f"**{category}**")
                
                # Create table data - show ALL results (no truncation)
                table_data = []
                for result in results:
                    test_name = result['test_name']
                    value = result['value']
                    ref_range = result['ref_range'] if result['ref_range'] else 'N/A'
                    
                    # Mark abnormal values with indicator
                    if result['is_abnormal']:
                        value_display = f"🔴 {value}"
                    else:
                        value_display = value
                    
                    table_data.append({
                        'Test': test_name,
                        'Value': value_display,
                        'Reference Range': ref_range
                    })
                
                # Display as DataFrame - show ALL results
                df = pd.DataFrame(table_data)
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Test": st.column_config.TextColumn("Test", width="medium"),
                        "Value": st.column_config.TextColumn("Value", width="small"),
                        "Reference Range": st.column_config.TextColumn("Reference Range", width="medium")
                    }
                )
    
    st.markdown("---")


def group_microbiology_results(micro_results):
    """Group microbiology results by test type and specimen"""
    grouped = {}
    
    for result in micro_results:
        result = result.strip()
        if not result:
            continue
        
        # Parse the result format: "SPECIMEN: TEST → ORGANISM | ANTIBIOTIC=VALUE"
        if ':' in result and '→' in result:
            parts = result.split(':', 1)
            specimen = parts[0].strip()
            rest = parts[1].strip()
            
            if '→' in rest:
                test_parts = rest.split('→', 1)
                test_type = test_parts[0].strip()
                finding = test_parts[1].strip()
                
                key = f"{test_type}: {specimen}"
                
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(finding)
        else:
            if 'Other' not in grouped:
                grouped['Other'] = []
            grouped['Other'].append(result)
    
    return grouped


def render_microbiology_event(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render microbiology event with table format"""
    props = event['properties']
    timestamp = event['timestamp']
    
    micro_count = props.get('micro_count', 0)
    micro_results = props.get('micro_results', [])
    
    st.markdown(f"**🦠 Microbiology Tests Performed:** {format_timestamp(timestamp)}")
    st.write(f"Total of **{micro_count}** microbiology result(s):")
    
    if micro_results:
        # Group results by specimen and test type
        grouped = group_microbiology_results(micro_results)
        
        for test_key, findings in grouped.items():
            if test_key != 'Other':
                st.markdown(f"**{test_key}:**")
                
                # If there are multiple similar findings (like antibiotic sensitivities), create a table
                if len(findings) > 3 and all('|' in f for f in findings[:3]):
                    # This looks like antibiotic sensitivity data
                    organism = findings[0].split('|')[0].strip() if '|' in findings[0] else findings[0]
                    
                    st.write(f"*Organism: {organism}*")
                    
                    # Create table for sensitivities
                    table_data = []
                    for finding in findings:
                        if '|' in finding:
                            parts = finding.split('|')
                            if len(parts) > 1:
                                sensitivity = parts[1].strip()
                                if '=' in sensitivity:
                                    antibiotic, value = sensitivity.split('=', 1)
                                    table_data.append({
                                        'Antibiotic': antibiotic.strip(),
                                        'Sensitivity': value.strip()
                                    })
                    
                    if table_data:
                        df = pd.DataFrame(table_data)
                        st.dataframe(
                            df,
                            use_container_width=True,
                            hide_index=True
                        )
                else:
                    # Regular list format for non-repetitive findings
                    for finding in findings:
                        st.write(f"• {finding}")
            else:
                # Other findings that don't fit the pattern
                for finding in findings:
                    st.write(f"• {finding}")
    
    st.markdown("---")


def render_procedure(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render procedure - 3-column matrix format"""
    props = event['properties']
    timestamp = event['timestamp']
    
    procedure_count = props.get('procedure_count', 0)
    procedures = props.get('procedures', [])
    source = props.get('source', 'N/A')
    
    st.markdown(f"**🔬 Procedure(s) Performed:** {format_timestamp(timestamp)}")
    st.write(f"**{procedure_count}** procedure(s) performed (Source: {source}):")
    
    if procedures:
        if isinstance(procedures, list):
            proc_matrix = _create_matrix_dataframe(procedures)
            # Configure columns to hide headers - dynamically based on actual columns
            num_cols = len(proc_matrix.columns)
            column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
            st.dataframe(proc_matrix, column_config=column_config, width='stretch', hide_index=True)
        else:
            st.write(procedures)
    
    st.markdown("---")


def render_prescription(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render prescription - 3-column matrix format"""
    props = event['properties']
    timestamp = event['timestamp']
    
    medicine_count = props.get('medicine_count', 0)
    medicines = props.get('medicines', [])
    
    st.markdown(f"**💊 Prescriptions Ordered:** {format_timestamp(timestamp)}")
    st.write(f"**{medicine_count}** medication(s) prescribed:")
    
    if medicines:
        if isinstance(medicines, list):
            meds_matrix = _create_matrix_dataframe(medicines)
            # Configure columns to hide headers - dynamically based on actual columns
            num_cols = len(meds_matrix.columns)
            column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
            st.dataframe(meds_matrix, column_config=column_config, width='stretch', hide_index=True)
        else:
            st.write(medicines)
    
    st.markdown("---")


def render_administered_meds(event: dict, children_dict: dict, last_timestamp: Optional[datetime]):
    """Render administered medications - 3-column matrix format"""
    props = event['properties']
    timestamp = event['timestamp']
    
    medications = props.get('medications', [])
    med_count = props.get('medication_count', len(medications))
    
    st.markdown(f"**💉 Medications Administered:** {format_timestamp(timestamp)}")
    st.write(f"Total of **{med_count}** medication(s) administered:")
    
    if medications:
        if isinstance(medications, list):
            meds_matrix = _create_matrix_dataframe(medications)
            # Configure columns to hide headers - dynamically based on actual columns
            num_cols = len(meds_matrix.columns)
            column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
            st.dataframe(meds_matrix, column_config=column_config, width='stretch', hide_index=True)
        else:
            st.write(medications)
    
    st.markdown("---")


def render_discharge(event: dict, children_dict: dict, last_timestamp: Optional[datetime], connection: Neo4jConnection):
    """Render discharge information (matching create_patient_journey_pdf.py logic)"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            st.markdown(f"*Time gap of **{gap}** since last event*")
            st.markdown("---")
    
    st.markdown("### Discharge Summary")
    st.markdown(f"**Discharge Time:** {format_timestamp(timestamp)}")
    
    disposition = props.get('disposition', 'N/A')
    careunit = props.get('careunit', 'N/A')
    major_procedure = props.get('major_procedure', 'None')
    allergies = props.get('allergies', 'None')
    
    # If careunit is UNKNOWN or N/A, try to get it from the previous node (matching create_patient_journey_pdf.py)
    if careunit in ['UNKNOWN', 'Unknown', 'N/A', None]:
        event_id = props.get('event_id')
        if event_id:
            try:
                prev_query = """
                MATCH (prev)-[r]->(d:Discharge {event_id: $event_id})
                WHERE prev.careunit IS NOT NULL
                RETURN prev.careunit as careunit
                ORDER BY r LIMIT 1
                """
                results = connection.execute_query(prev_query, {"event_id": event_id})
                if results:
                    record = results[0]
                    resolved_careunit = record.get('careunit')
                    if resolved_careunit:
                        careunit = resolved_careunit
                        # Update event properties so PDF generation can use it
                        event['properties']['careunit'] = careunit
            except Exception as e:
                logger.warning(f"Could not resolve careunit from previous node: {e}")
                pass  # Keep original careunit if query fails
    
    st.write(f"The patient was discharged from **{careunit}** with disposition to **{disposition}**.")
    
    if major_procedure != 'None':
        st.write(f"**Major Procedure(s) During Admission:** {major_procedure}")
    
    st.warning(f"**Known Allergies:** {allergies}")
    
    # Detailed Allergies - 3-column matrix format
    allergy_nodes = children_dict.get('AllergyIdentified', [])
    if allergy_nodes:
        st.markdown("**Detailed Allergy List:**")
        allergy_list = []
        for child_props, _ in allergy_nodes:
            allergy_name = child_props.get('allergy_name', 'Unknown')
            if allergy_name and allergy_name not in allergy_list:
                allergy_list.append(allergy_name)
        
        if allergy_list:
            # Create matrix format (auto-determined columns)
            allergy_matrix = _create_matrix_dataframe(allergy_list)
            # Configure columns to hide headers - dynamically based on actual columns
            num_cols = len(allergy_matrix.columns)
            column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
            st.dataframe(allergy_matrix, column_config=column_config, width='stretch', hide_index=True)
    
    # Discharge Diagnoses - 3 separate dataframes
    for child_props, _ in children_dict.get('Diagnosis', []):
        st.markdown("**Discharge Diagnoses:**")
        
        primary_diagnoses = child_props.get('primary_diagnoses', [])
        secondary_diagnoses = child_props.get('secondary_diagnoses', [])
        complete_diagnosis = child_props.get('complete_diagnosis', [])
        
        # Primary Diagnoses - separate dataframe
        if primary_diagnoses:
            st.markdown("**Primary Diagnoses:**")
            if isinstance(primary_diagnoses, list):
                primary_list = primary_diagnoses
            else:
                primary_list = [primary_diagnoses]
            
            if primary_list:
                primary_matrix = _create_matrix_dataframe(primary_list)
                num_cols = len(primary_matrix.columns)
                column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                st.dataframe(primary_matrix, column_config=column_config, width='stretch', hide_index=True)
        
        # Secondary Diagnoses - separate dataframe
        if secondary_diagnoses:
            st.markdown("**Secondary Diagnoses:**")
            if isinstance(secondary_diagnoses, list):
                secondary_list = secondary_diagnoses
            else:
                secondary_list = [secondary_diagnoses]
            
            if secondary_list:
                secondary_matrix = _create_matrix_dataframe(secondary_list)
                num_cols = len(secondary_matrix.columns)
                column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                st.dataframe(secondary_matrix, column_config=column_config, width='stretch', hide_index=True)
        
        # Complete Diagnosis List - separate dataframe
        if complete_diagnosis:
            st.markdown("**Complete Diagnosis List:**")
            if isinstance(complete_diagnosis, list):
                complete_list = complete_diagnosis
            else:
                complete_list = [complete_diagnosis]
            
            if complete_list:
                complete_matrix = _create_matrix_dataframe(complete_list)
                num_cols = len(complete_matrix.columns)
                column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                st.dataframe(complete_matrix, column_config=column_config, width='stretch', hide_index=True)
    
    # Discharge Clinical Note
    for child_props, _ in children_dict.get('DischargeClinicalNote', []):
        hospital_course = child_props.get('hospital_course', '')
        discharge_instructions = child_props.get('discharge_instructions', '')
        activity_status = child_props.get('activity_status', 'N/A')
        level_of_consciousness = child_props.get('level_of_consciousness', 'N/A')
        mental_status = child_props.get('mental_status', 'N/A')
        code_status = child_props.get('code_status', 'N/A')
        antibiotic_plan = child_props.get('antibiotic_plan', '')
        microbiology_findings = child_props.get('microbiology_findings', '')
        
        if hospital_course:
            st.markdown("**Hospital Course:**")
            # Clean and format hospital course as proper paragraph
            # Handle both string and list formats
            if isinstance(hospital_course, list):
                # If it's a list, join with spaces
                hospital_course = ' '.join(str(item) for item in hospital_course if item)
            
            # Replace multiple whitespace/newlines with single space
            hospital_course_clean = re.sub(r'[\n\r]+', ' ', str(hospital_course))
            hospital_course_clean = re.sub(r'\s+', ' ', hospital_course_clean.strip())
            
            # Display as normal text paragraph
            st.write(hospital_course_clean)
        
        if microbiology_findings:
            st.markdown("**Microbiology Findings:**")
            # Format as paragraph
            if isinstance(microbiology_findings, list):
                microbiology_findings = ' '.join(str(item) for item in microbiology_findings if item)
            
            micro_clean = re.sub(r'[\n\r]+', ' ', str(microbiology_findings))
            micro_clean = re.sub(r'\s+', ' ', micro_clean.strip())
            
            # Display as normal text paragraph
            st.write(micro_clean)
        
        if antibiotic_plan:
            st.markdown("**Antibiotic Plan:**")
            # Format as paragraph
            if isinstance(antibiotic_plan, list):
                antibiotic_plan = ' '.join(str(item) for item in antibiotic_plan if item)
            
            abx_clean = re.sub(r'[\n\r]+', ' ', str(antibiotic_plan))
            abx_clean = re.sub(r'\s+', ' ', abx_clean.strip())
            
            # Display as normal text paragraph
            st.write(abx_clean)
        
        st.markdown("**Discharge Status:**")
        # Use table format for discharge status
        status_data = {
            'Status Type': ['Activity Status', 'Level of Consciousness', 'Mental Status', 'Code Status'],
            'Value': [activity_status, level_of_consciousness, mental_status, code_status]
        }
        df_status = pd.DataFrame(status_data)
        st.dataframe(
            df_status,
            width='stretch',
            hide_index=True
        )
        
        if discharge_instructions:
            st.markdown("**Discharge Instructions:**")
            # Format as paragraph
            if isinstance(discharge_instructions, list):
                discharge_instructions = ' '.join(str(item) for item in discharge_instructions if item)
            
            instructions_clean = re.sub(r'[\n\r]+', ' ', str(discharge_instructions))
            instructions_clean = re.sub(r'\s+', ' ', instructions_clean.strip())
            
            # Display as normal text paragraph
            st.write(instructions_clean)
    
    # Medication Changes - 3-column matrix format
    medication_list = []
    
    for child_props, _ in children_dict.get('MedicationStarted', []):
        medications = child_props.get('medications', [])
        if medications:
            for med in medications:
                medication_list.append(f"Started: {med}")
    
    for child_props, _ in children_dict.get('MedicationStopped', []):
        medications = child_props.get('medications', [])
        if medications:
            for med in medications:
                medication_list.append(f"Stopped: {med}")
    
    for child_props, _ in children_dict.get('MedicationToAvoid', []):
        medications = child_props.get('medications', [])
        if medications:
            for med in medications:
                medication_list.append(f"To Avoid: {med}")
    
    if medication_list:
        st.markdown("**Medication Changes:**")
        med_matrix = _create_matrix_dataframe(medication_list)
        # Configure columns to hide headers - dynamically based on actual columns
        num_cols = len(med_matrix.columns)
        column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
        st.dataframe(med_matrix, column_config=column_config, width='stretch', hide_index=True)
    
    st.markdown("---")


def render_patient_journey_tab(connection: Neo4jConnection):
    """
    Render the patient journey tab in Streamlit
    
    Args:
        connection: Neo4j connection object
    """
    st.markdown("### 📅 Chronological Patient Journey")
    st.info("Enter a patient ID to view their complete clinical journey in chronological order.")
    
    # Patient ID input
    patient_id = st.text_input(
        "Enter Patient ID:",
        placeholder="e.g., 10000032",
        key="journey_patient_id_input"
    )
    
    # Generate button
    generate_button = st.button("📊 Generate Journey Report", type="primary", use_container_width=True, key="generate_journey_button")
    
    # Check if we have a cached journey for this patient
    cached_journey_key = f"journey_{patient_id.strip()}" if patient_id else None
    cached_journey = st.session_state.get(cached_journey_key) if cached_journey_key else None
    
    # Display cached journey if available and no new generation requested
    if cached_journey and not generate_button and patient_id:
        journey_data = cached_journey
        st.info(f"📋 Showing previously generated journey for Patient {patient_id}")
        st.markdown("---")
        
        # Render the journey
        _render_journey_events(journey_data, connection)
        
        # Download PDF button
        st.markdown("---")
        try:
            from features.download_patient_journey import create_journey_pdf
            pdf_bytes = create_journey_pdf(journey_data)
            st.download_button(
                label="📄 Download Journey as PDF",
                data=pdf_bytes,
                file_name=f"patient_{patient_id.strip()}_Journey.pdf",
                mime="application/pdf",
                use_container_width=True
            )
        except Exception as e:
            logger.error(f"Error creating PDF: {e}", exc_info=True)
            st.error(f"Error creating PDF: {str(e)}")
        return
    
    # Generate journey
    if generate_button and patient_id:
        if not patient_id.strip().isdigit():
            st.error("Please enter a valid numeric patient ID")
            return
        
        with st.spinner("Extracting patient journey data..."):
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
                
                # Get journey data
                journey_data, error = get_patient_journey_data(connection, patient_id.strip())
                
                if error:
                    st.error(f"Error: {error}")
                    return
                
                if not journey_data or not journey_data.get('events'):
                    st.warning(f"No journey events found for patient {patient_id}")
                    return
                
                # Store in session state
                st.session_state[cached_journey_key] = journey_data
                
                st.success(f"✅ Journey report generated successfully! Found {len(journey_data['events'])} events.")
                st.markdown("---")
                
                # Render the journey
                _render_journey_events(journey_data, connection)
                
                # Download PDF button
                st.markdown("---")
                try:
                    from features.download_patient_journey import create_journey_pdf
                    pdf_bytes = create_journey_pdf(journey_data)
                    st.download_button(
                        label="📄 Download Journey as PDF",
                        data=pdf_bytes,
                        file_name=f"patient_{patient_id.strip()}_Journey.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                except Exception as e:
                    logger.error(f"Error creating PDF: {e}", exc_info=True)
                    st.error(f"Error creating PDF: {str(e)}")
                
            except Exception as e:
                st.error(f"Error generating journey: {str(e)}")
                logger.error(f"Error in journey generation: {e}", exc_info=True)
    
    elif generate_button and not patient_id:
        st.warning("Please enter a patient ID first.")


def _render_journey_events(journey_data: dict, connection: Neo4jConnection):
    """Render all journey events"""
    patient = journey_data.get('patient', {})
    patient_props = patient.get('properties', {})
    events = journey_data.get('events', [])
    
    # Patient Information
    st.markdown("#### 👤 Patient Information")
    gender = patient_props.get('gender', 'N/A')
    age = patient_props.get('anchor_age', 'N/A')
    race = patient_props.get('race', 'N/A')
    admissions = patient_props.get('total_number_of_admissions', 'N/A')
    
    st.write(f"A **{age}** year old **{gender.lower()}** patient had a total of **{admissions}** hospital admission(s) during the recorded period.")
    st.markdown("---")
    
    # Render events in chronological order
    last_timestamp = None
    
    for event in events:
        labels = event.get('labels', [])
        if not labels:
            continue
        
        label = labels[0]
        element_id = event.get('element_id', '')
        
        # Get child nodes
        children_dict = get_child_nodes(connection, element_id, event['properties'])
        
        # Render based on event type - all events in strict chronological order
        if label == 'EmergencyDepartment':
            render_ed_visit(event, children_dict, last_timestamp)
        elif label == 'HospitalAdmission':
            render_hospital_admission(event, children_dict, last_timestamp)
        elif label == 'ICUStay':
            render_icu_stay(event, children_dict, last_timestamp)
        elif label == 'UnitAdmission':
            render_unit_admission(event, children_dict, last_timestamp)
        elif label == 'LabEvent':
            render_lab_event(event, children_dict, last_timestamp)
        elif label == 'MicrobiologyEvent':
            render_microbiology_event(event, children_dict, last_timestamp)
        elif label == 'Procedures':
            render_procedure(event, children_dict, last_timestamp)
        elif label == 'Prescription':
            render_prescription(event, children_dict, last_timestamp)
        elif label == 'AdministeredMeds':
            render_administered_meds(event, children_dict, last_timestamp)
        elif label == 'Discharge':
            render_discharge(event, children_dict, last_timestamp, connection)
        elif label == 'ChartEvent':
            # ChartEvent - render as a simple event
            props = event['properties']
            timestamp = event['timestamp']
            if last_timestamp:
                gap = calculate_time_gap(last_timestamp, timestamp)
                if gap:
                    st.markdown(f"*Time gap of **{gap}** since last event*")
                    st.markdown("---")
            st.markdown(f"**📊 Chart Event:** {format_timestamp(timestamp)}")
            st.write(f"Chart event recorded.")
            st.markdown("---")
        elif label == 'PreviousPrescriptionMeds':
            # PreviousPrescriptionMeds - render similar to AdministeredMeds
            props = event['properties']
            timestamp = event['timestamp']
            medications = props.get('medications', [])
            med_count = props.get('medication_count', len(medications))
            
            if last_timestamp:
                gap = calculate_time_gap(last_timestamp, timestamp)
                if gap:
                    st.markdown(f"*Time gap of **{gap}** since last event*")
                    st.markdown("---")
            
            st.markdown(f"**💊 Previous Medications on Record:** {format_timestamp(timestamp)}")
            st.write(f"Total of **{med_count}** medication(s) documented:")
            
            if medications:
                if isinstance(medications, list):
                    meds_matrix = _create_matrix_dataframe(medications)
                    num_cols = len(meds_matrix.columns)
                    column_config = {str(i): st.column_config.TextColumn("", width="medium") for i in range(num_cols)}
                    st.dataframe(meds_matrix, column_config=column_config, width='stretch', hide_index=True)
                else:
                    st.write(medications)
            
            st.markdown("---")
        elif label == 'Transfer':
            # Transfer event
            props = event['properties']
            timestamp = event['timestamp']
            if last_timestamp:
                gap = calculate_time_gap(last_timestamp, timestamp)
                if gap:
                    st.markdown(f"*Time gap of **{gap}** since last event*")
                    st.markdown("---")
            st.markdown(f"### Transfer")
            st.markdown(f"**Transfer Time:** {format_timestamp(timestamp)}")
            careunit = props.get('careunit', 'N/A')
            st.write(f"Patient transferred to **{careunit}**.")
            st.markdown("---")
        else:
            # Unknown event type - render generically
            props = event['properties']
            timestamp = event['timestamp']
            if last_timestamp:
                gap = calculate_time_gap(last_timestamp, timestamp)
                if gap:
                    st.markdown(f"*Time gap of **{gap}** since last event*")
                    st.markdown("---")
            st.markdown(f"**{label}:** {format_timestamp(timestamp)}")
            st.markdown("---")
        
        last_timestamp = event['timestamp']

