# create_summarization_pdf.py
import logging
from neo4j import GraphDatabase
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, HRFlowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Neo4j configuration
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "10016742"

class NumberedCanvas(canvas.Canvas):
    """Custom canvas to add page numbers"""
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        self.setFont("Helvetica", 9)
        self.setFillColor(colors.grey)
        self.drawRightString(
            8 * inch, 0.5 * inch,
            f"Page {self._pageNumber} of {page_count}"
        )

def get_folder_name():
    """Read folder name from foldername.txt"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        foldername_path = os.path.join(script_dir, 'foldername.txt')
        with open(foldername_path, 'r') as f:
            folder_name = f.read().strip()
        logger.info(f"Using folder name: {folder_name}")
        return folder_name
    except Exception as e:
        logger.warning(f"Could not read folder name: {e}")
        return "default"

def extract_timestamp(node):
    """Extract the primary timestamp from a node based on its label"""
    props = node._properties
    label = list(node.labels)[0] if node.labels else "Unknown"
    
    timestamp_fields = {
        'HospitalAdmission': ['admittime', 'dischtime'],
        'EmergencyDepartment': ['intime', 'outtime'],
        'UnitAdmission': ['intime', 'outtime'],
        'ICUStay': ['intime', 'outtime'],
        'Transfer': ['intime', 'outtime'],
        'Discharge': ['outtime', 'intime'],
        'Prescription': ['starttime'],
        'Procedures': ['time'],
        'LabEvent': ['charttime'],
        'MicrobiologyEvent': ['charttime'],
        'ChartEvent': ['charttime'],
        'PreviousPrescriptionMeds': ['charttime'],
        'AdministeredMeds': ['charttime'],
        'InitialAssessment': ['charttime'] if 'charttime' in props else []
    }
    
    fields = timestamp_fields.get(label, [])
    
    for field in fields:
        if field in props and props[field]:
            try:
                timestamp_str = props[field]
                if isinstance(timestamp_str, str):
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except:
                continue
    
    return None

def get_child_nodes(session, parent_node):
    """Get child nodes that don't have timestamps"""
    node_id = parent_node.element_id
    
    query = """
    MATCH (parent)-[r]->(child)
    WHERE elementId(parent) = $node_id
    RETURN child, type(r) as relationship_type
    """
    
    results = session.run(query, node_id=node_id)
    children = []
    
    for record in results:
        child = record['child']
        rel_type = record['relationship_type']
        
        if extract_timestamp(child) is None:
            children.append((child, rel_type))
    
    return children

def setup_styles():
    """Setup custom paragraph styles"""
    styles = getSampleStyleSheet()
    
    # Title style
    styles.add(ParagraphStyle(
        name='CustomTitle',
        parent=styles['Heading1'],
        fontSize=22,
        textColor=colors.HexColor('#1a237e'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    ))
    
    # Patient header
    styles.add(ParagraphStyle(
        name='PatientHeader',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#0d47a1'),
        spaceAfter=20,
        spaceBefore=20,
        fontName='Helvetica-Bold'
    ))
    
    # Section header
    styles.add(ParagraphStyle(
        name='SectionHeader',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1565c0'),
        spaceAfter=12,
        spaceBefore=14,
        fontName='Helvetica-Bold',
        leading=18
    ))
    
    # Subsection header
    styles.add(ParagraphStyle(
        name='SubsectionHeader',
        parent=styles['Heading3'],
        fontSize=11,
        textColor=colors.HexColor('#1976d2'),
        spaceAfter=6,
        spaceBefore=8,
        fontName='Helvetica-Bold',
        leading=14
    ))
    
    # Body text
    styles.add(ParagraphStyle(
        name='CustomBody',
        parent=styles['BodyText'],
        fontSize=10,
        textColor=colors.black,
        spaceAfter=6,
        alignment=TA_JUSTIFY,
        fontName='Helvetica',
        leading=14
    ))
    
    # Event timestamp
    styles.add(ParagraphStyle(
        name='EventTime',
        parent=styles['Normal'],
        fontSize=11,
        spaceAfter=6,
        fontName='Helvetica-Bold',
        textColor=colors.HexColor('#d32f2f'),
        leading=14
    ))
    
    # Highlight text
    styles.add(ParagraphStyle(
        name='Highlight',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#c62828'),
        fontName='Helvetica-Bold',
        leading=14
    ))
    
    return styles

def clean_text(text):
    """Clean text by replacing ___ with appropriate placeholder"""
    if text:
        # Replace various forms of underscores with [data redacted] or appropriate placeholder
        text = text.replace('___', '[redacted]')
        text = text.replace('at ___ within', 'at [hospital contact] within')
    return text

def ordinal_suffix(n):
    """Convert number to ordinal string (e.g., 1 -> '1st', 2 -> '2nd', 3 -> '3rd')"""
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])

def format_patient_info(patient_node, styles, story):
    """Format patient information section"""
    props = patient_node._properties
    
    story.append(Paragraph(
        f"PATIENT INFORMATION - Subject ID: {props.get('subject_id', 'N/A')}", 
        styles['PatientHeader']
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#0d47a1'), 
                            spaceAfter=10, spaceBefore=5))
    
    gender = props.get('gender', 'N/A')
    age = props.get('anchor_age', 'N/A')
    race = props.get('race', 'N/A')
    admissions = props.get('total_number_of_admissions', 'N/A')
    
    text = f"""
    A <b>{age}</b> year old <b>{gender.lower()}</b> patient had a total of 
    <b>{admissions}</b> hospital admission(s) during the recorded period.
    """
    
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))

def format_ed_visit(node, timestamp, children_dict, styles, story):
    """Format Emergency Department visit"""
    props = node._properties
    
    # Get ED sequence number
    ed_seq_num = props.get('ed_seq_num', None)
    
    # Format header with sequence number if available
    if ed_seq_num is not None:
        header_text = f"Emergency Department Visit #{ed_seq_num}"
    else:
        header_text = "Emergency Department Visit"
    
    story.append(Paragraph(header_text, styles['SectionHeader']))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1565c0')))
    story.append(Paragraph(
        f"<b>Admission Time:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['EventTime']
    ))
    
    arrival = props.get('arrival_transport', 'N/A')
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    disposition = props.get('disposition', 'N/A')
    
    # Build description text with sequence context
    if ed_seq_num is not None:
        text = f"""
        This is the patient's <b>{ordinal_suffix(ed_seq_num)}</b> Emergency Department visit. 
        The patient arrived via <b>{arrival}</b>. 
        The patient departed at <b>{outtime}</b>, staying for a duration of <b>{period}</b>.
        """
    else:
        text = f"""
        The patient arrived at the Emergency Department via <b>{arrival}</b>. 
        The patient departed at <b>{outtime}</b>, staying for a duration of <b>{period}</b>.
        """
    story.append(Paragraph(text, styles['CustomBody']))
    
    # Check if patient was discharged from ED without admission
    ed_discharge_keywords = ['HOME', 'DISCHARGED', 'AGAINST ADVICE', 'LEFT', 'AMA']
    is_ed_discharge = any(keyword in disposition.upper() for keyword in ed_discharge_keywords) if disposition != 'N/A' else False
    
    if is_ed_discharge:
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            f"<b>ED Disposition:</b> <font color='#d32f2f'>{disposition}</font> - Patient was discharged directly from Emergency Department without hospital admission.", 
            styles['Highlight']
        ))
        story.append(Spacer(1, 4))
    
    # Initial diagnosis
    for child, rel_type in children_dict.get('Diagnosis', []):
        if child._properties.get('ed_diagnosis') == 'True':
            diagnoses = child._properties.get('complete_diagnosis', [])
            if diagnoses:
                story.append(Spacer(1, 6))
                story.append(Paragraph("<b>Initial Diagnosis:</b>", styles['SubsectionHeader']))
                for diag in diagnoses:
                    story.append(Paragraph(f"• {diag}", styles['CustomBody']))
    
    # Initial assessment
    for child, rel_type in children_dict.get('InitialAssessment', []):
        child_props = child._properties
        story.append(Spacer(1, 6))
        story.append(Paragraph("<b>Initial Assessment:</b>", styles['SubsectionHeader']))
        
        # Parse chief complaint to separate transfer/arrival method from actual complaints
        chief_complaint_raw = clean_text(child_props.get('chiefcomplaint', 'N/A'))
        
        # Check if chief complaint contains transfer-related keywords
        transfer_keywords = ['Transfer', 'TRANSFER', 'Transferred']
        arrival_method = None
        actual_complaints = []
        
        if chief_complaint_raw and chief_complaint_raw != 'N/A':
            # Split by comma to get individual items
            complaint_items = [item.strip() for item in chief_complaint_raw.split(',')]
            
            for item in complaint_items:
                # Check if this item is a transfer/arrival method
                if any(keyword in item for keyword in transfer_keywords):
                    arrival_method = item
                else:
                    actual_complaints.append(item)
        
        # Display arrival method if present
        if arrival_method:
            story.append(Paragraph(
                f"<b>Patient Transfer Status:</b> {arrival_method} - Patient was transferred from another healthcare facility (e.g., Extended Care, Rehabilitation Center, or other Hospital)", 
                styles['CustomBody']
            ))
        
        # Display actual chief complaints
        if actual_complaints:
            complaints_text = ', '.join(actual_complaints)
            story.append(Paragraph(f"<b>Chief Complaint:</b> {complaints_text}", styles['CustomBody']))
        elif not arrival_method:
            # If no transfer and no complaints, show original
            story.append(Paragraph(f"<b>Chief Complaint:</b> {chief_complaint_raw}", styles['CustomBody']))
        
        story.append(Spacer(1, 4))
        
        # Create table for triage vitals
        vitals_data = []
        if 'sbp' in child_props and 'dbp' in child_props:
            vitals_data.append(['Blood Pressure', f"{child_props['sbp']}/{child_props['dbp']} mmHg"])
        if 'heartrate' in child_props:
            vitals_data.append(['Heart Rate', f"{child_props['heartrate']} bpm"])
        if 'resprate' in child_props:
            vitals_data.append(['Respiratory Rate', f"{child_props['resprate']} breaths/min"])
        if 'o2sat' in child_props:
            vitals_data.append(['Oxygen Saturation', f"{child_props['o2sat']}%"])
        if 'temperature' in child_props:
            vitals_data.append(['Temperature', f"{child_props['temperature']}°F"])
        if 'pain' in child_props:
            vitals_data.append(['Pain Score', str(child_props['pain'])])
        if 'acuity' in child_props:
            vitals_data.append(['Acuity Level', str(child_props['acuity'])])
        
        if vitals_data:
            t = Table(vitals_data, colWidths=[2.2*inch, 2.2*inch])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ffe0b2')),  # Light Orange
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 0), (-1, -1), [colors.HexColor('#ffe0b2'), colors.HexColor('#fff8f0')]),
                ('VALIGN', (0, 0), (-1, -1), 'TOP')
            ]))
            story.append(t)
    
    # If this is an ED discharge (no admission), add discharge summary section
    if is_ed_discharge:
        story.append(Spacer(1, 8))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#d32f2f'), 
                                spaceAfter=6, spaceBefore=6))
        story.append(Paragraph("<b>Emergency Department Discharge Summary</b>", styles['SubsectionHeader']))
        
        # Show ED diagnosis as final diagnosis for discharge
        for child, rel_type in children_dict.get('Diagnosis', []):
            if child._properties.get('ed_diagnosis') == 'True':
                diagnoses = child._properties.get('complete_diagnosis', [])
                if diagnoses:
                    story.append(Spacer(1, 4))
                    story.append(Paragraph("<b>Final ED Diagnosis:</b>", styles['Highlight']))
                    for diag in diagnoses:
                        story.append(Paragraph(f"• {diag}", styles['CustomBody']))
        
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            f"<b>Treatment:</b> The patient received evaluation and treatment in the Emergency Department. "
            f"Medical staff determined the patient was stable for discharge to <b>{disposition}</b>.", 
            styles['CustomBody']
        ))
        
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "<b>Follow-up Instructions:</b> Patient advised to follow up with primary care physician or return to ED if symptoms worsen.", 
            styles['CustomBody']
        ))
        
        story.append(Spacer(1, 8))
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey))
    
    story.append(Spacer(1, 12))

def format_administered_meds(node, timestamp, styles, story):
    """Format administered medications"""
    props = node._properties
    
    story.append(Paragraph(
        f'<font color="#2e7d32"><b>Medications Administered:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    medications = props.get('medications', [])
    med_count = props.get('medication_count', len(medications))
    
    story.append(Paragraph(
        f"Total of <b>{med_count}</b> medication(s) administered:", 
        styles['CustomBody']
    ))
    
    for med in medications:
        story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))

def format_hospital_admission(node, timestamp, children_dict, styles, story):
    """Format hospital admission"""
    props = node._properties
    
    # Get hospital admission sequence number
    hadm_seq_num = props.get('hospital_admission_sequence_number', None)
    
    # Format header with sequence number if available
    if hadm_seq_num is not None:
        header_text = f"Hospital Admission #{hadm_seq_num}"
    else:
        header_text = "Hospital Admission"
    
    story.append(Paragraph(header_text, styles['SectionHeader']))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1565c0')))
    story.append(Paragraph(
        f"<b>Admission Time:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['EventTime']
    ))
    
    admit_location = props.get('admission_location', 'N/A')
    admit_type = props.get('admission_type', 'N/A')
    provider = props.get('admit_provider_id', 'N/A')
    insurance = props.get('insurance', 'N/A')
    service = props.get('service', 'N/A')
    chief_complaint = props.get('chief_complaint', 'N/A')
    
    # Build description text with sequence context
    if hadm_seq_num is not None:
        text = f"""
        This is the patient's <b>{ordinal_suffix(hadm_seq_num)}</b> hospital admission. 
        The patient was admitted from <b>{admit_location}</b> as an 
        <b>{admit_type}</b> admission. The admitting provider was <b>{provider}</b>. 
        Insurance coverage: <b>{insurance}</b>. Chief complaint: <b>{chief_complaint}</b>. 
        The primary service provided was <b>{service}</b>.
        """
    else:
        text = f"""
        The patient was admitted to the hospital from <b>{admit_location}</b> as an 
        <b>{admit_type}</b> admission. The admitting provider was <b>{provider}</b>. 
        Insurance coverage: <b>{insurance}</b>. Chief complaint: <b>{chief_complaint}</b>. 
        The primary service provided was <b>{service}</b>.
        """
    story.append(Paragraph(text, styles['CustomBody']))
    
    # Demographics
    race = props.get('race', 'N/A')
    marital = props.get('marital_status', 'N/A')
    language = props.get('language', 'N/A')
    
    if race != 'N/A' or marital != 'N/A' or language != 'N/A':
        story.append(Spacer(1, 6))
        demo_text = f"Race: <b>{race}</b>, Marital Status: <b>{marital}</b>, Language: <b>{language}</b>"
        story.append(Paragraph(demo_text, styles['CustomBody']))
    
    # DRG Codes
    for child, rel_type in children_dict.get('DRG', []):
        child_props = child._properties
        drg_type = child_props.get('drg_type', 'Unknown')
        
        story.append(Spacer(1, 8))
        story.append(Paragraph(f"<b>{drg_type} DRG Code:</b>", styles['SubsectionHeader']))
        
        desc = child_props.get('description', 'N/A')
        code = child_props.get('drg_code', 'N/A')
        
        story.append(Paragraph(f"<b>Code:</b> {code}", styles['CustomBody']))
        story.append(Paragraph(f"<b>Description:</b> {desc}", styles['CustomBody']))
        
        if 'drg_severity' in child_props:
            story.append(Paragraph(
                f"<b>Severity:</b> {child_props['drg_severity']}", 
                styles['CustomBody']
            ))
        if 'drg_mortality' in child_props:
            story.append(Paragraph(
                f"<b>Mortality Risk:</b> {child_props['drg_mortality']}", 
                styles['CustomBody']
            ))
    
    # Past History
    for child, rel_type in children_dict.get('PatientPastHistory', []):
        child_props = child._properties
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Patient Past History:</b>", styles['SubsectionHeader']))
        
        pmh = child_props.get('past_medical_history', 'N/A')
        fh = child_props.get('family_history', 'N/A')
        sh = child_props.get('social_history', 'N/A')
        
        story.append(Paragraph(f"<b>Past Medical History:</b> {pmh}", styles['CustomBody']))
        story.append(Paragraph(f"<b>Family History:</b> {fh}", styles['CustomBody']))
        story.append(Paragraph(f"<b>Social History:</b> {sh}", styles['CustomBody']))
    
    # HPI Summary
    for child, rel_type in children_dict.get('HPISummary', []):
        child_props = child._properties
        summary = child_props.get('summary', 'N/A')
        
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>History of Present Illness:</b>", styles['SubsectionHeader']))
        story.append(Paragraph(summary, styles['CustomBody']))
    
    # Admission Vitals
    for child, rel_type in children_dict.get('AdmissionVitals', []):
        child_props = child._properties
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Admission Vital Signs:</b>", styles['SubsectionHeader']))
        
        bp = child_props.get('Blood_Pressure', 'N/A')
        hr = child_props.get('Heart_Rate', 'N/A')
        rr = child_props.get('Respiratory_Rate', 'N/A')
        temp = child_props.get('Temperature', 'N/A')
        spo2 = child_props.get('SpO2', 'N/A')
        general = child_props.get('General', 'N/A')
        
        # Create table for vitals
        vitals_data = [
            ['General Appearance', general],
            ['Blood Pressure', f'{bp} mmHg'],
            ['Heart Rate', f'{hr} bpm'],
            ['Respiratory Rate', f'{rr} breaths/min'],
            ['Temperature', f'{temp}°F'],
            ['Oxygen Saturation', spo2]
        ]
        
        t = Table(vitals_data, colWidths=[2.2*inch, 3.2*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#bbdefb')),  # Light Blue
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 7),
            ('TOPPADDING', (0, 0), (-1, -1), 7),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#e3f2fd')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP')
        ]))
        story.append(t)
    
    # Admission Labs
    for child, rel_type in children_dict.get('AdmissionLabs', []):
        child_props = child._properties
        lab_tests = child_props.get('lab_tests', [])
        
        if lab_tests:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Admission Laboratory Results:</b>", styles['SubsectionHeader']))
            
            for test in lab_tests:
                story.append(Paragraph(f"• {test}", styles['CustomBody']))
    
    # Admission Medications
    for child, rel_type in children_dict.get('AdmissionMedications', []):
        child_props = child._properties
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Admission Medications:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(f"Total of <b>{len(medications)}</b> medications on admission:", styles['CustomBody']))
            
            for med in medications:
                story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    story.append(Spacer(1, 12))

def format_unit_admission(node, timestamp, children_dict, styles, story):
    """Format regular ward/unit admission (non-ICU)"""
    props = node._properties
    
    careunit = props.get('careunit', 'N/A')
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    service = props.get('service_given', 'N/A')
    
    story.append(Paragraph(f"Unit Admission - {careunit}", styles['SectionHeader']))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1565c0')))
    story.append(Paragraph(
        f"<b>Admission Time:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['EventTime']
    ))
    
    text = f"""
    The patient was admitted to <b>{careunit}</b> ward.
    """
    if service != 'N/A':
        text += f" Service provided: <b>{service}</b>."
    
    text += f""" The patient stayed in this unit until <b>{outtime}</b>, 
    for a total duration of <b>{period}</b>. During this time, various 
    procedures, laboratory tests, and microbiology events were performed.
    """
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))

def format_icu_stay(node, timestamp, children_dict, styles, story):
    """Format ICU stay"""
    props = node._properties
    
    careunit = props.get('careunit', 'N/A')
    first_careunit = props.get('first_careunit', careunit)
    last_careunit = props.get('last_careunit', careunit)
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    los = props.get('los', 'N/A')
    service = props.get('service_given', 'N/A')
    
    story.append(Paragraph(f"ICU Admission - {careunit}", styles['SectionHeader']))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#d32f2f')))
    story.append(Paragraph(
        f"<b>ICU Admission Time:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['EventTime']
    ))
    
    text = f"""
    The patient was admitted to the <b>Intensive Care Unit ({careunit})</b>.
    """
    
    if first_careunit != last_careunit:
        text += f" The patient was transferred within ICU from <b>{first_careunit}</b> to <b>{last_careunit}</b>."
    
    if service != 'N/A':
        text += f" Service provided: <b>{service}</b>."
    
    text += f""" The patient remained in ICU until <b>{outtime}</b>, 
    for a total ICU stay of <b>{period}</b>"""
    
    if los != 'N/A':
        text += f" (Length of Stay: <b>{los} days</b>)"
    
    text += """. During this critical care period, intensive monitoring and interventions were performed 
    including procedures, laboratory tests, and microbiology events.
    """
    
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))

def parse_lab_result(result_str):
    """Parse a lab result string into components"""
    # Format: "Test Name=Value (ref: min-max) [abnormal] Specimen, Category"
    # or: "Test Name=Value Specimen, Category"
    
    parts = result_str.split('=', 1)
    if len(parts) < 2:
        return None
    
    test_name = parts[0].strip()
    rest = parts[1].strip()
    
    # Extract value, reference range, abnormal flag, specimen, category
    value = ""
    ref_range = ""
    is_abnormal = "[abnormal]" in rest
    specimen = ""
    category = ""
    
    # Remove abnormal flag
    rest = rest.replace('[abnormal]', '').strip()
    
    # Extract specimen and category (at the end)
    if ',' in rest:
        # Split from the right to get category
        temp_parts = rest.rsplit(',', 1)
        if len(temp_parts) == 2:
            category = temp_parts[1].strip()
            rest = temp_parts[0].strip()
            
            # Check if there's another comma for specimen
            if ',' in rest:
                temp_parts2 = rest.rsplit(',', 1)
                if len(temp_parts2) == 2:
                    specimen = temp_parts2[1].strip()
                    rest = temp_parts2[0].strip()
    
    # Extract reference range
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

def format_lab_event(node, timestamp, styles, story):
    """Format laboratory event with table"""
    props = node._properties
    
    abnormal_count = props.get('abnormal_count', 0)
    lab_count = props.get('lab_count', 0)
    lab_results = props.get('lab_results', [])
    
    story.append(Paragraph(
        f'<font color="#1565c0"><b>Laboratory Tests Performed:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"Total of <b>{lab_count}</b> tests performed, with <b>{abnormal_count}</b> abnormal result(s).", 
        styles['CustomBody']
    ))
    
    if lab_results:
        story.append(Spacer(1, 6))
        
        # Group by category
        grouped_results = {}
        
        for result in lab_results:
            parsed = parse_lab_result(result)
            if parsed:
                category = parsed['category'] if parsed['category'] else 'Other'
                
                if category not in grouped_results:
                    grouped_results[category] = []
                grouped_results[category].append(parsed)
        
        # Create tables for each category
        for category, results in grouped_results.items():
            if len(results) > 0:
                story.append(Paragraph(f"<b>{category}</b>", styles['CustomBody']))
                story.append(Spacer(1, 3))
                
                # Create table data
                table_data = [['Test', 'Value', 'Reference Range']]
                
                for result in results:
                    test_name = result['test_name']
                    value = result['value']
                    ref_range = result['ref_range'] if result['ref_range'] else 'N/A'
                    
                    # Format with red color for abnormal
                    if result['is_abnormal']:
                        value_cell = Paragraph(f'<font color="red"><b>{value}</b></font>', styles['CustomBody'])
                    else:
                        value_cell = value
                    
                    table_data.append([test_name, value_cell, ref_range])
                
                # Create table
                col_widths = [2.6*inch, 1.5*inch, 1.6*inch]
                t = Table(table_data, colWidths=col_widths)
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e8f5e9')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1b5e20')),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ('TOPPADDING', (0, 0), (-1, -1), 5),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fafafa')]),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP')
                ]))
                
                story.append(t)
                story.append(Spacer(1, 8))
    
    story.append(Spacer(1, 8))

def group_microbiology_results(micro_results):
    """Group microbiology results by test type and specimen"""
    grouped = {}
    
    for result in micro_results:
        result = clean_text(result)
        
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
            # If it doesn't match the pattern, add as standalone
            if 'Other' not in grouped:
                grouped['Other'] = []
            grouped['Other'].append(result)
    
    return grouped

def format_microbiology_event(node, timestamp, styles, story):
    """Format microbiology event"""
    props = node._properties
    
    micro_count = props.get('micro_count', 0)
    micro_results = props.get('micro_results', [])
    
    story.append(Paragraph(
        f'<font color="#7b1fa2"><b>Microbiology Tests Performed:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"Total of <b>{micro_count}</b> microbiology result(s):", 
        styles['CustomBody']
    ))
    
    if micro_results:
        story.append(Spacer(1, 4))
        
        # Group results by specimen and test type
        grouped = group_microbiology_results(micro_results)
        
        for test_key, findings in grouped.items():
            if test_key != 'Other':
                story.append(Paragraph(f"<b>{test_key}:</b>", styles['CustomBody']))
                
                # If there are multiple similar findings (like antibiotic sensitivities), create a table
                if len(findings) > 3 and all('|' in f for f in findings[:3]):
                    # This looks like antibiotic sensitivity data
                    # Extract organism name from first finding
                    organism = findings[0].split('|')[0].strip() if '|' in findings[0] else findings[0]
                    
                    story.append(Paragraph(f"  Organism: <i>{organism}</i>", styles['CustomBody']))
                    
                    # Create table for sensitivities
                    table_data = [['Antibiotic', 'Sensitivity']]
                    
                    for finding in findings:
                        if '|' in finding:
                            parts = finding.split('|')
                            if len(parts) > 1:
                                sensitivity = parts[1].strip()
                                if '=' in sensitivity:
                                    antibiotic, value = sensitivity.split('=', 1)
                                    table_data.append([antibiotic.strip(), value.strip()])
                    
                    if len(table_data) > 1:
                        t = Table(table_data, colWidths=[3.2*inch, 1.8*inch])
                        t.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e1bee7')),  # Light Purple
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#6a1b9a')),
                            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 10),
                            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                            ('FONTSIZE', (0, 1), (-1, -1), 9),
                            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                            ('TOPPADDING', (0, 0), (-1, -1), 6),
                            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f3e5f5')])
                        ]))
                        story.append(Spacer(1, 4))
                        story.append(t)
                        story.append(Spacer(1, 4))
                else:
                    # Regular list format for non-repetitive findings
                    for finding in findings:
                        story.append(Paragraph(f"  • {finding}", styles['CustomBody']))
            else:
                # Other findings that don't fit the pattern
                for finding in findings:
                    story.append(Paragraph(f"• {finding}", styles['CustomBody']))
            
            story.append(Spacer(1, 4))
    
    story.append(Spacer(1, 8))

def format_procedure(node, timestamp, styles, story):
    """Format procedure"""
    props = node._properties
    
    procedure_count = props.get('procedure_count', 0)
    procedures = props.get('procedures', [])
    source = props.get('source', 'N/A')
    
    story.append(Paragraph(
        f"<b>Procedure(s) Performed:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"<b>{procedure_count}</b> procedure(s) performed (Source: {source}):", 
        styles['CustomBody']
    ))
    
    for proc in procedures:
        story.append(Paragraph(f"• {proc}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))

def format_prescription(node, timestamp, styles, story):
    """Format prescription"""
    props = node._properties
    
    medicine_count = props.get('medicine_count', 0)
    medicines = props.get('medicines', [])
    
    story.append(Paragraph(
        f'<font color="#2e7d32"><b>Prescriptions Ordered:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"<b>{medicine_count}</b> medication(s) prescribed:", 
        styles['CustomBody']
    ))
    
    for med in medicines:
        story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))

def format_previous_meds(node, timestamp, styles, story):
    """Format previous prescription medications"""
    props = node._properties
    
    medication_count = props.get('medication_count', 0)
    medications = props.get('medications', [])
    
    story.append(Paragraph(
        f'<font color="#2e7d32"><b>Previous Medications on Record:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"<b>{medication_count}</b> medication(s) documented:", 
        styles['CustomBody']
    ))
    
    # Group by classification if possible
    for med in medications:
        story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))

def format_discharge(node, timestamp, children_dict, styles, story, session=None):
    """Format discharge information"""
    props = node._properties
    
    story.append(Paragraph("Discharge Summary", styles['SectionHeader']))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#1565c0')))
    story.append(Paragraph(
        f"<b>Discharge Time:</b> {timestamp.strftime('%B %d, %Y at %I:%M %p')}", 
        styles['EventTime']
    ))
    
    disposition = props.get('disposition', 'N/A')
    careunit = props.get('careunit', 'N/A')
    major_procedure = props.get('major_procedure', 'None')
    allergies = props.get('allergies', 'None')
    
    # If careunit is UNKNOWN or N/A, try to get it from the previous node
    if careunit in ['UNKNOWN', 'Unknown', 'N/A', None] and session:
        event_id = props.get('event_id')
        if event_id:
            try:
                prev_query = """
                MATCH (prev)-[r]->(d:Discharge {event_id: $event_id})
                WHERE prev.careunit IS NOT NULL
                RETURN prev.careunit as careunit
                ORDER BY r LIMIT 1
                """
                result = session.run(prev_query, event_id=event_id)
                record = result.single()
                if record and record['careunit']:
                    careunit = record['careunit']
            except:
                pass  # Keep original careunit if query fails
    
    text = f"""
    The patient was discharged from <b>{careunit}</b> with disposition to <b>{disposition}</b>. 
    """
    story.append(Paragraph(text, styles['CustomBody']))
    
    if major_procedure != 'None':
        story.append(Paragraph(
            f"<b>Major Procedure(s) During Admission:</b> {major_procedure}", 
            styles['CustomBody']
        ))
    
    story.append(Paragraph(f"<b>Known Allergies:</b> {allergies}", styles['Highlight']))
    
    # Detailed Allergies from AllergyIdentified nodes
    allergy_nodes = children_dict.get('AllergyIdentified', [])
    if allergy_nodes:
        story.append(Spacer(1, 6))
        story.append(Paragraph("<b>Detailed Allergy List:</b>", styles['SubsectionHeader']))
        allergy_list = []
        for child, rel_type in allergy_nodes:
            allergy_name = child._properties.get('allergy_name', 'Unknown')
            if allergy_name and allergy_name not in allergy_list:
                allergy_list.append(allergy_name)
        
        if allergy_list:
            for allergy in allergy_list:
                story.append(Paragraph(f"• {allergy}", styles['Highlight']))
        story.append(Spacer(1, 4))
    
    # Discharge Diagnoses
    for child, rel_type in children_dict.get('Diagnosis', []):
        child_props = child._properties
        
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Discharge Diagnoses:</b>", styles['SubsectionHeader']))
        
        primary_diagnoses = child_props.get('primary_diagnoses', [])
        secondary_diagnoses = child_props.get('secondary_diagnoses', [])
        
        if primary_diagnoses:
            story.append(Paragraph("<b>Primary Diagnoses:</b>", styles['Highlight']))
            if isinstance(primary_diagnoses, list):
                for diag in primary_diagnoses:
                    story.append(Paragraph(f"• {diag}", styles['CustomBody']))
            else:
                story.append(Paragraph(f"• {primary_diagnoses}", styles['CustomBody']))
        
        if secondary_diagnoses:
            story.append(Spacer(1, 4))
            story.append(Paragraph("<b>Secondary Diagnoses:</b>", styles['CustomBody']))
            if isinstance(secondary_diagnoses, list):
                for diag in secondary_diagnoses:
                    story.append(Paragraph(f"• {diag}", styles['CustomBody']))
            else:
                story.append(Paragraph(f"• {secondary_diagnoses}", styles['CustomBody']))
        
        complete_diagnosis = child_props.get('complete_diagnosis', [])
        if complete_diagnosis and len(complete_diagnosis) > 0:
            story.append(Spacer(1, 4))
            story.append(Paragraph("<b>Complete Diagnosis List:</b>", styles['CustomBody']))
            for diag in complete_diagnosis:
                story.append(Paragraph(f"• {diag}", styles['CustomBody']))
    
    # Discharge Clinical Note
    for child, rel_type in children_dict.get('DischargeClinicalNote', []):
        child_props = child._properties
        
        hospital_course = clean_text(child_props.get('hospital_course', ''))
        discharge_instructions = clean_text(child_props.get('discharge_instructions', ''))
        activity_status = clean_text(child_props.get('activity_status', 'N/A'))
        code_status = clean_text(child_props.get('code_status', 'N/A'))
        level_of_consciousness = clean_text(child_props.get('level_of_consciousness', 'N/A'))
        mental_status = clean_text(child_props.get('mental_status', 'N/A'))
        antibiotic_plan = clean_text(child_props.get('antibiotic_plan', ''))
        microbiology_findings = clean_text(child_props.get('microbiology_findings', ''))
        
        if hospital_course:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Hospital Course:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(hospital_course, styles['CustomBody']))
        
        if microbiology_findings:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Microbiology Findings:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(microbiology_findings, styles['CustomBody']))
        
        if antibiotic_plan:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Antibiotic Plan:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(antibiotic_plan, styles['CustomBody']))
        
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Discharge Status:</b>", styles['SubsectionHeader']))
        
        # Create table for discharge status
        status_data = [
            ['Activity Status', activity_status],
            ['Level of Consciousness', level_of_consciousness],
            ['Mental Status', mental_status],
            ['Code Status', code_status]
        ]
        
        t = Table(status_data, colWidths=[2.2*inch, 4*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#fff9c4')),  # Light Yellow
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 7),
            ('TOPPADDING', (0, 0), (-1, -1), 7),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fffde7')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP')
        ]))
        story.append(t)
        
        if discharge_instructions:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Discharge Instructions:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(discharge_instructions, styles['CustomBody']))
    
    # Medication Changes
    for child, rel_type in children_dict.get('MedicationStarted', []):
        child_props = child._properties
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>New Medications Started:</b>", styles['SubsectionHeader']))
            for med in medications:
                story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    for child, rel_type in children_dict.get('MedicationStopped', []):
        child_props = child._properties
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Medications Stopped:</b>", styles['SubsectionHeader']))
            for med in medications:
                story.append(Paragraph(f"• {med}", styles['CustomBody']))
    
    for child, rel_type in children_dict.get('MedicationToAvoid', []):
        child_props = child._properties
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Medications to Avoid:</b>", styles['Highlight']))
            for med in medications:
                story.append(Paragraph(f"• {med}", styles['Highlight']))
    
    story.append(Spacer(1, 20))

def calculate_time_gap(last_timestamp, current_timestamp):
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

def generate_patient_report(session, subject_id, patient_node, styles):
    """Generate a single patient's report content"""
    logger.info(f"Processing patient {subject_id}...")
    
    story = []
    
    # Title page for this patient
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph(
        "CLINICAL PATIENT JOURNEY", 
        styles['CustomTitle']
    ))
    story.append(Spacer(1, 0.3*inch))
    story.append(HRFlowable(width="100%", thickness=3, color=colors.HexColor('#1a237e')))
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph(
        f"Patient ID: {subject_id}", 
        styles['PatientHeader']
    ))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", 
        styles['CustomBody']
    ))
    story.append(Paragraph(
        f"Database: {DATABASE}", 
        styles['CustomBody']
    ))
    story.append(PageBreak())
    
    # Get all nodes related to this patient
    nodes_query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    WITH p
    OPTIONAL MATCH (p)-[*]->(n)
    WHERE n.name IS NOT NULL
    RETURN DISTINCT n
    """
    
    results = session.run(nodes_query, subject_id=str(subject_id))
    
    # Collect and sort nodes by timestamp
    nodes_with_timestamps = []
    
    for record in results:
        node = record['n']
        if node is None:
            continue
        
        timestamp = extract_timestamp(node)
        if timestamp:
            nodes_with_timestamps.append((node, timestamp))
    
    nodes_with_timestamps.sort(key=lambda x: x[1])
    
    # Format patient information
    format_patient_info(patient_node, styles, story)
    
    # Process each event in chronological order
    last_timestamp = None
    last_event_type = None
    
    for node, timestamp in nodes_with_timestamps:
        label = list(node.labels)[0] if node.labels else "Unknown"
        
        # Calculate time gap
        if last_timestamp and label in ['EmergencyDepartment', 'HospitalAdmission']:
            gap = calculate_time_gap(last_timestamp, timestamp)
            if gap:
                story.append(HRFlowable(
                    width="100%", 
                    thickness=0.5, 
                    color=colors.lightgrey,
                    spaceBefore=10,
                    spaceAfter=10
                ))
                story.append(Paragraph(
                    f"<i>Time gap of <b>{gap}</b> since last event</i>", 
                    styles['CustomBody']
                ))
                story.append(Spacer(1, 10))
        
        # Get child nodes
        children = get_child_nodes(session, node)
        children_dict = {}
        for child, rel_type in children:
            child_label = list(child.labels)[0] if child.labels else "Unknown"
            if child_label not in children_dict:
                children_dict[child_label] = []
            children_dict[child_label].append((child, rel_type))
        
        # Format based on node type
        if label == 'EmergencyDepartment':
            format_ed_visit(node, timestamp, children_dict, styles, story)
        elif label == 'AdministeredMeds':
            format_administered_meds(node, timestamp, styles, story)
        elif label == 'HospitalAdmission':
            format_hospital_admission(node, timestamp, children_dict, styles, story)
        elif label == 'ICUStay':
            format_icu_stay(node, timestamp, children_dict, styles, story)
        elif label == 'UnitAdmission':
            format_unit_admission(node, timestamp, children_dict, styles, story)
        elif label == 'LabEvent':
            format_lab_event(node, timestamp, styles, story)
        elif label == 'MicrobiologyEvent':
            format_microbiology_event(node, timestamp, styles, story)
        elif label == 'Procedures':
            format_procedure(node, timestamp, styles, story)
        elif label == 'Prescription':
            format_prescription(node, timestamp, styles, story)
        elif label == 'PreviousPrescriptionMeds':
            format_previous_meds(node, timestamp, styles, story)
        elif label == 'Discharge':
            format_discharge(node, timestamp, children_dict, styles, story, session)
            story.append(PageBreak())
        
        last_timestamp = timestamp
        last_event_type = label
    
    return story


def generate_patient_summary_pdf(subject_id=None):
    """Generate comprehensive patient summary PDF for a specific patient"""
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    folder_name = get_folder_name()
    
    # Create output directory if it doesn't exist
    output_dir = "Patient_Reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    try:
        with driver.session() as session:
            # If no subject_id provided, get it from user input
            if subject_id is None:
                # First, show available patients
                logger.info("Fetching available patients...")
                patient_list_query = """
                MATCH (p:Patient)
                RETURN p.subject_id as subject_id, p.gender as gender, 
                       p.anchor_age as age, p.total_number_of_admissions as admissions
                ORDER BY p.subject_id
                """
                
                patient_list_results = session.run(patient_list_query)
                patient_list = [(record['subject_id'], record['gender'], 
                               record['age'], record['admissions']) 
                               for record in patient_list_results]
                
                if not patient_list:
                    logger.error("No patients found in database!")
                    print("\n❌ No patients found in database!")
                    return
                
                # Display available patients
                print("\n" + "="*80)
                print("AVAILABLE PATIENTS IN DATABASE")
                print("="*80)
                print(f"{'Subject ID':<15} {'Gender':<10} {'Age':<10} {'Admissions':<15}")
                print("-"*80)
                for pid, gender, age, admissions in patient_list:
                    # Ensure consistent string representation
                    pid_str = str(pid) if pid else "N/A"
                    gender_str = str(gender) if gender else "N/A"
                    age_str = str(age) if age else "N/A"
                    admissions_str = str(admissions) if admissions else "N/A"
                    print(f"{pid_str:<15} {gender_str:<10} {age_str:<10} {admissions_str:<15}")
                print("="*80)
                
                # Get user input
                print("\nEnter the Subject ID of the patient for whom you want to generate a report.")
                subject_id = input("Subject ID: ").strip()
                
                if not subject_id:
                    logger.error("No subject ID provided!")
                    print("\n❌ No subject ID provided. Exiting.")
                    return
            
            logger.info(f"Generating report for patient {subject_id}...")
            print(f"\n🔍 Looking for patient {subject_id}...")
            
            # Get specific patient - try to handle both string and integer subject_ids
            patient_query = """
            MATCH (p:Patient)
            WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
            RETURN p.subject_id as subject_id, p
            """
            
            patient_result = session.run(patient_query, subject_id=str(subject_id))
            patient_record = patient_result.single()
            
            if not patient_record:
                logger.error(f"Patient with subject_id {subject_id} not found!")
                print(f"\n❌ Patient with subject_id {subject_id} not found in database!")
                print(f"   Debug: Searched for subject_id = '{subject_id}' (type: {type(subject_id).__name__})")
                return
            
            patient_node = patient_record['p']
            
            # Get the actual subject_id from the database (in case of type differences)
            actual_subject_id = str(patient_record['subject_id'])
            
            # Create subfolder for this patient
            patient_folder = os.path.join(output_dir, actual_subject_id)
            if not os.path.exists(patient_folder):
                os.makedirs(patient_folder)
                logger.info(f"Created patient folder: {patient_folder}")
            
            # Generate filename with new naming convention
            output_filename = os.path.join(
                patient_folder, 
                f"{actual_subject_id}_Patients_Journey.pdf"
            )
            
            # Check if file exists and inform user
            if os.path.exists(output_filename):
                print(f"\n⚠️  Existing report found: {output_filename}")
                print("   This will be replaced with the new report.")
            
            print(f"\n📊 Generating clinical summary report...")
            
            styles = setup_styles()
            
            # Setup PDF for this patient
            doc = SimpleDocTemplate(
                output_filename,
                pagesize=letter,
                rightMargin=0.75*inch,
                leftMargin=0.75*inch,
                topMargin=1*inch,
                bottomMargin=0.75*inch
            )
            
            # Generate story for this patient using the actual subject_id from database
            story = generate_patient_report(session, actual_subject_id, patient_node, styles)
            
            # Build PDF
            logger.info(f"Building PDF for patient {actual_subject_id}...")
            print(f"📝 Compiling report...")
            doc.build(story, canvasmaker=NumberedCanvas)
            
            logger.info(f"PDF generated successfully: {output_filename}")
            print(f"\n✅ Report generated successfully!")
            print(f"📁 Patient Folder: {os.path.abspath(patient_folder)}")
            print(f"📄 Filename: {actual_subject_id}_Patients_Journey.pdf")
            print(f"📏 File size: {os.path.getsize(output_filename) / 1024:.2f} KB")
            
    except Exception as e:
        logger.error(f"Error generating PDF: {e}", exc_info=True)
        print(f"\n❌ Error generating PDF: {e}")
        raise
    finally:
        driver.close()

def generate_multiple_patients():
    """Generate reports for multiple patients interactively"""
    while True:
        generate_patient_summary_pdf()
        
        print("\n" + "="*80)
        response = input("Would you like to generate another report? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("\n👋 Thank you! Exiting report generation.")
            break
        print("\n")

if __name__ == "__main__":
    logger.info("Starting patient summary PDF generation...")
    print("\n" + "="*80)
    print("CLINICAL PATIENT SUMMARY REPORT GENERATOR")
    print("="*80)
    
    try:
        generate_multiple_patients()
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation interrupted by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    
    logger.info("PDF generation session complete!")

