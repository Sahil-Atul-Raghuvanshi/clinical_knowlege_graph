"""
PDF Generation Module for Patient Journey
Creates formatted PDF reports from patient journey data using ReportLab
Based on create_patient_journey_pdf.py logic
"""
import logging
import io
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, HRFlowable
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.pdfgen import canvas

logger = logging.getLogger(__name__)


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
        text = text.replace('___', '[redacted]')
        text = text.replace('at ___ within', 'at [hospital contact] within')
    return text


def sanitize_html_tags(text):
    """Sanitize HTML tags in text for ReportLab compatibility"""
    if not text:
        return text
    text = text.replace('<br>', '<br/>')
    text = text.replace('<BR>', '<br/>')
    text = text.replace('<br />', '<br/>')
    text = text.replace('<BR />', '<br/>')
    return text


def ordinal_suffix(n):
    """Convert number to ordinal string (e.g., 1 -> '1st', 2 -> '2nd', 3 -> '3rd')"""
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])


def format_patient_info(patient_props: dict, styles, story):
    """Format patient information section"""
    story.append(Paragraph(
        f"PATIENT INFORMATION - Subject ID: {patient_props.get('subject_id', 'N/A')}", 
        styles['PatientHeader']
    ))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#0d47a1'), 
                            spaceAfter=10, spaceBefore=5))
    
    gender = patient_props.get('gender', 'N/A')
    age = patient_props.get('anchor_age', 'N/A')
    admissions = patient_props.get('total_number_of_admissions', 'N/A')
    
    text = f"""
    A <b>{age}</b> year old <b>{gender.lower()}</b> patient had a total of 
    <b>{admissions}</b> hospital admission(s) during the recorded period.
    """
    
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))


def format_ed_visit(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format Emergency Department visit"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                                    spaceBefore=10, spaceAfter=10))
            story.append(Paragraph(f"<i>Time gap of <b>{gap}</b> since last event</i>", styles['CustomBody']))
            story.append(Spacer(1, 10))
    
    # Header
    ed_seq_num = props.get('ed_seq_num')
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
    
    # Check if ED discharge
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
    for child_props, _ in children_dict.get('Diagnosis', []):
        if child_props.get('ed_diagnosis') == 'True':
            diagnoses = child_props.get('complete_diagnosis', [])
            if diagnoses:
                story.append(Spacer(1, 6))
                story.append(Paragraph("<b>Initial Diagnosis:</b>", styles['SubsectionHeader']))
                for diag in diagnoses:
                    story.append(Paragraph(f"• {diag}", styles['CustomBody']))
    
    # Initial assessment
    for child_props, _ in children_dict.get('InitialAssessment', []):
        story.append(Spacer(1, 6))
        story.append(Paragraph("<b>Initial Assessment:</b>", styles['SubsectionHeader']))
        
        chief_complaint_raw = clean_text(child_props.get('chiefcomplaint', 'N/A'))
        
        # Parse chief complaint
        transfer_keywords = ['Transfer', 'TRANSFER', 'Transferred']
        arrival_method = None
        actual_complaints = []
        
        if chief_complaint_raw and chief_complaint_raw != 'N/A':
            complaint_items = [item.strip() for item in chief_complaint_raw.split(',')]
            for item in complaint_items:
                if any(keyword in item for keyword in transfer_keywords):
                    arrival_method = item
                else:
                    actual_complaints.append(item)
        
        if arrival_method:
            story.append(Paragraph(
                f"<b>Patient Transfer Status:</b> {arrival_method} - Patient was transferred from another healthcare facility (e.g., Extended Care, Rehabilitation Center, or other Hospital)", 
                styles['CustomBody']
            ))
        
        if actual_complaints:
            complaints_text = ', '.join(actual_complaints)
            story.append(Paragraph(f"<b>Chief Complaint:</b> {complaints_text}", styles['CustomBody']))
        elif not arrival_method:
            story.append(Paragraph(f"<b>Chief Complaint:</b> {chief_complaint_raw}", styles['CustomBody']))
        
        story.append(Spacer(1, 4))
        
        # Vitals table
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
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#ffe0b2')),
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
    
    # ED discharge summary
    if is_ed_discharge:
        story.append(Spacer(1, 8))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#d32f2f'), 
                                spaceAfter=6, spaceBefore=6))
        story.append(Paragraph("<b>Emergency Department Discharge Summary</b>", styles['SubsectionHeader']))
        
        for child_props, _ in children_dict.get('Diagnosis', []):
            if child_props.get('ed_diagnosis') == 'True':
                diagnoses = child_props.get('complete_diagnosis', [])
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


def format_hospital_admission(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format hospital admission"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                                    spaceBefore=10, spaceAfter=10))
            story.append(Paragraph(f"<i>Time gap of <b>{gap}</b> since last event</i>", styles['CustomBody']))
            story.append(Spacer(1, 10))
    
    # Header
    hadm_seq_num = props.get('hospital_admission_sequence_number')
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
    for child_props, _ in children_dict.get('DRG', []):
        child_props = child_props if isinstance(child_props, dict) else {}
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
    for child_props, _ in children_dict.get('PatientPastHistory', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Patient Past History:</b>", styles['SubsectionHeader']))
        
        pmh = child_props.get('past_medical_history', 'N/A')
        fh = child_props.get('family_history', 'N/A')
        sh = child_props.get('social_history', 'N/A')
        
        story.append(Paragraph(f"<b>Past Medical History:</b> {pmh}", styles['CustomBody']))
        story.append(Paragraph(f"<b>Family History:</b> {fh}", styles['CustomBody']))
        story.append(Paragraph(f"<b>Social History:</b> {sh}", styles['CustomBody']))
    
    # HPI Summary
    for child_props, _ in children_dict.get('HPISummary', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        summary = child_props.get('summary', 'N/A')
        
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>History of Present Illness:</b>", styles['SubsectionHeader']))
        story.append(Paragraph(summary, styles['CustomBody']))
    
    # Admission Vitals
    for child_props, _ in children_dict.get('AdmissionVitals', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Admission Vital Signs:</b>", styles['SubsectionHeader']))
        
        bp = child_props.get('Blood_Pressure', 'N/A')
        hr = child_props.get('Heart_Rate', 'N/A')
        rr = child_props.get('Respiratory_Rate', 'N/A')
        temp = child_props.get('Temperature', 'N/A')
        spo2 = child_props.get('SpO2', 'N/A')
        general = child_props.get('General', 'N/A')
        
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
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#bbdefb')),
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
    for child_props, _ in children_dict.get('AdmissionLabs', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        lab_tests = child_props.get('lab_tests', [])
        
        if lab_tests:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Admission Laboratory Results:</b>", styles['SubsectionHeader']))
            
            for test in lab_tests:
                story.append(Paragraph(f"• {test}", styles['CustomBody']))
    
    # Admission Medications
    for child_props, _ in children_dict.get('AdmissionMedications', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Admission Medications:</b>", styles['SubsectionHeader']))
            story.append(Paragraph(f"Total of <b>{len(medications)}</b> medications on admission:", styles['CustomBody']))
            
            for med in medications:
                story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    story.append(Spacer(1, 12))


def format_icu_stay(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format ICU stay - only admission info, no child events"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                                    spaceBefore=10, spaceAfter=10))
            story.append(Paragraph(f"<i>Time gap of <b>{gap}</b> since last event</i>", styles['CustomBody']))
            story.append(Spacer(1, 10))
    
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
    
    text += """. During this critical care period, intensive monitoring and interventions were performed.
    """
    
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))


def format_unit_admission(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format regular ward/unit admission - only admission info, no child events"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Time gap
    if last_timestamp:
        gap = calculate_time_gap(last_timestamp, timestamp)
        if gap:
            story.append(HRFlowable(width="100%", thickness=0.5, color=colors.lightgrey,
                                    spaceBefore=10, spaceAfter=10))
            story.append(Paragraph(f"<i>Time gap of <b>{gap}</b> since last event</i>", styles['CustomBody']))
            story.append(Spacer(1, 10))
    
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
    for a total duration of <b>{period}</b>.
    """
    story.append(Paragraph(text, styles['CustomBody']))
    story.append(Spacer(1, 12))


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


def format_lab_event(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format laboratory event with table"""
    props = event['properties']
    timestamp = event['timestamp']
    
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
                
                table_data = [['Test', 'Value', 'Reference Range']]
                
                for result in results:
                    test_name = result['test_name']
                    value = result['value']
                    ref_range = result['ref_range'] if result['ref_range'] else 'N/A'
                    
                    if result['is_abnormal']:
                        value_cell = Paragraph(f'<font color="red"><b>{value}</b></font>', styles['CustomBody'])
                    else:
                        value_cell = value
                    
                    table_data.append([test_name, value_cell, ref_range])
                
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


def format_microbiology_event(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format microbiology event"""
    props = event['properties']
    timestamp = event['timestamp']
    
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
        
        grouped = group_microbiology_results(micro_results)
        
        for test_key, findings in grouped.items():
            if test_key != 'Other':
                story.append(Paragraph(f"<b>{test_key}:</b>", styles['CustomBody']))
                
                if len(findings) > 3 and all('|' in f for f in findings[:3]):
                    organism = findings[0].split('|')[0].strip() if '|' in findings[0] else findings[0]
                    
                    story.append(Paragraph(f"  Organism: <i>{organism}</i>", styles['CustomBody']))
                    
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
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e1bee7')),
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
                    for finding in findings:
                        story.append(Paragraph(f"  • {finding}", styles['CustomBody']))
            else:
                for finding in findings:
                    story.append(Paragraph(f"• {finding}", styles['CustomBody']))
            
            story.append(Spacer(1, 4))
    
    story.append(Spacer(1, 8))


def format_procedure(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format procedure"""
    props = event['properties']
    timestamp = event['timestamp']
    
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


def format_prescription(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format prescription"""
    props = event['properties']
    timestamp = event['timestamp']
    
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
        story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))


def format_administered_meds(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format administered medications (matching create_patient_journey_pdf.py)"""
    props = event['properties']
    timestamp = event['timestamp']
    
    medications = props.get('medications', [])
    med_count = props.get('medication_count', len(medications))
    
    story.append(Paragraph(
        f'<font color="#2e7d32"><b>Medications Administered:</b></font> {timestamp.strftime("%B %d, %Y at %I:%M %p")}', 
        styles['SubsectionHeader']
    ))
    
    story.append(Paragraph(
        f"Total of <b>{med_count}</b> medication(s) administered:", 
        styles['CustomBody']
    ))
    
    for med in medications:
        story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))


def format_previous_meds(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime]):
    """Format previous prescription medications (matching create_patient_journey_pdf.py)"""
    props = event['properties']
    timestamp = event['timestamp']
    
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
        story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    story.append(Spacer(1, 8))


def format_discharge(event: dict, children_dict: dict, styles, story, last_timestamp: Optional[datetime], journey_data: Dict[str, Any] = None):
    """Format discharge information (matching create_patient_journey_pdf.py)"""
    props = event['properties']
    timestamp = event['timestamp']
    
    # Note: Time gap is handled in main loop (matching create_patient_journey_pdf.py)
    
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
    
    # If careunit is UNKNOWN or N/A, try to get it from previous events (matching create_patient_journey_pdf.py logic)
    if careunit in ['UNKNOWN', 'Unknown', 'N/A', None] and journey_data:
        # Look backwards through events to find the most recent event with a careunit
        events = journey_data.get('events', [])
        current_event_idx = None
        for idx, e in enumerate(events):
            if (e.get('properties', {}).get('event_id') == props.get('event_id') or
                (e.get('labels', []) and e['labels'][0] == 'Discharge' and 
                 e.get('timestamp') == timestamp)):
                current_event_idx = idx
                break
        
        if current_event_idx is not None:
            # Look backwards from current event to find careunit
            for i in range(current_event_idx - 1, -1, -1):
                prev_event = events[i]
                prev_props = prev_event.get('properties', {})
                prev_careunit = prev_props.get('careunit')
                if prev_careunit and prev_careunit not in ['UNKNOWN', 'Unknown', 'N/A', None]:
                    careunit = prev_careunit
                    break
    
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
    
    # Detailed Allergies
    allergy_nodes = children_dict.get('AllergyIdentified', [])
    if allergy_nodes:
        story.append(Spacer(1, 6))
        story.append(Paragraph("<b>Detailed Allergy List:</b>", styles['SubsectionHeader']))
        allergy_list = []
        for child_props, _ in allergy_nodes:
            child_props = child_props if isinstance(child_props, dict) else {}
            allergy_name = child_props.get('allergy_name', 'Unknown')
            if allergy_name and allergy_name not in allergy_list:
                allergy_list.append(allergy_name)
        
        if allergy_list:
            for allergy in allergy_list:
                story.append(Paragraph(f"• {allergy}", styles['Highlight']))
        story.append(Spacer(1, 4))
    
    # Discharge Diagnoses
    for child_props, _ in children_dict.get('Diagnosis', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        
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
    for child_props, _ in children_dict.get('DischargeClinicalNote', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        
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
            # Format as paragraph (matching create_patient_journey_pdf.py - simple paragraph, no complex formatting)
            story.append(Paragraph(hospital_course, styles['CustomBody']))
        
        if microbiology_findings:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Microbiology Findings:</b>", styles['SubsectionHeader']))
            # Format as paragraph (matching create_patient_journey_pdf.py)
            story.append(Paragraph(microbiology_findings, styles['CustomBody']))
        
        if antibiotic_plan:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Antibiotic Plan:</b>", styles['SubsectionHeader']))
            # Format as paragraph (matching create_patient_journey_pdf.py)
            story.append(Paragraph(antibiotic_plan, styles['CustomBody']))
        
        story.append(Spacer(1, 8))
        story.append(Paragraph("<b>Discharge Status:</b>", styles['SubsectionHeader']))
        
        status_data = [
            ['Activity Status', activity_status],
            ['Level of Consciousness', level_of_consciousness],
            ['Mental Status', mental_status],
            ['Code Status', code_status]
        ]
        
        t = Table(status_data, colWidths=[2.2*inch, 4*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#fff9c4')),
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
            # Format as paragraph (matching create_patient_journey_pdf.py)
            story.append(Paragraph(discharge_instructions, styles['CustomBody']))
    
    # Medication Changes
    for child_props, _ in children_dict.get('MedicationStarted', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>New Medications Started:</b>", styles['SubsectionHeader']))
            for med in medications:
                story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    for child_props, _ in children_dict.get('MedicationStopped', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Medications Stopped:</b>", styles['SubsectionHeader']))
            for med in medications:
                story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['CustomBody']))
    
    for child_props, _ in children_dict.get('MedicationToAvoid', []):
        child_props = child_props if isinstance(child_props, dict) else {}
        medications = child_props.get('medications', [])
        
        if medications:
            story.append(Spacer(1, 8))
            story.append(Paragraph("<b>Medications to Avoid:</b>", styles['Highlight']))
            for med in medications:
                story.append(Paragraph(f"• {sanitize_html_tags(med)}", styles['Highlight']))
    
    story.append(Spacer(1, 20))


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


def ensure_datetime(timestamp):
    """Ensure timestamp is a datetime object, converting from string if needed"""
    if isinstance(timestamp, datetime):
        return timestamp
    elif isinstance(timestamp, str):
        try:
            # Try ISO format first
            if 'T' in timestamp:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            # Try standard format
            return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        except:
            try:
                # Try ISO format without timezone
                return datetime.fromisoformat(timestamp)
            except:
                logger.warning(f"Could not parse timestamp: {timestamp}")
                return None
    return None


def create_journey_pdf(journey_data: Dict[str, Any]) -> bytes:
    """
    Create a well-formatted PDF from the journey data
    
    Args:
        journey_data: Dictionary containing patient journey data with patient and events
        
    Returns:
        PDF file as bytes for download
    """
    logger.info("Creating PDF from journey data...")
    
    # Create PDF in memory
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=1*inch,
        bottomMargin=0.75*inch
    )
    
    styles = setup_styles()
    story = []
    
    # Title page
    story.append(Spacer(1, 2*inch))
    story.append(Paragraph("CLINICAL PATIENT JOURNEY", styles['CustomTitle']))
    story.append(Spacer(1, 0.3*inch))
    story.append(HRFlowable(width="100%", thickness=3, color=colors.HexColor('#1a237e')))
    story.append(Spacer(1, 0.3*inch))
    
    patient = journey_data.get('patient', {})
    patient_props = patient.get('properties', {})
    subject_id = patient_props.get('subject_id', 'N/A')
    
    story.append(Paragraph(f"Patient ID: {subject_id}", styles['PatientHeader']))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}", 
        styles['CustomBody']
    ))
    story.append(PageBreak())
    
    # Patient Information
    format_patient_info(patient_props, styles, story)
    
    # Process events in chronological order (matching create_patient_journey_pdf.py logic)
    events = journey_data.get('events', [])
    last_timestamp = None
    last_event_type = None
    
    # Note: Child nodes are not available in journey_data structure
    # The format functions will work with empty children_dict
    children_dict = {}
    
    for event in events:
        labels = event.get('labels', [])
        if not labels:
            continue
        
        label = labels[0]
        timestamp = ensure_datetime(event.get('timestamp'))
        
        if timestamp is None:
            logger.warning(f"Skipping event {label} due to invalid timestamp")
            continue
        
        # Update event with datetime object for formatting functions
        event['timestamp'] = timestamp
        
        # Calculate time gap (matching create_patient_journey_pdf.py - only for EmergencyDepartment and HospitalAdmission)
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
        
        # Format based on node type (matching create_patient_journey_pdf.py order and logic)
        if label == 'EmergencyDepartment':
            format_ed_visit(event, children_dict, styles, story, last_timestamp)
        elif label == 'AdministeredMeds':
            format_administered_meds(event, children_dict, styles, story, last_timestamp)
        elif label == 'HospitalAdmission':
            format_hospital_admission(event, children_dict, styles, story, last_timestamp)
        elif label == 'ICUStay':
            format_icu_stay(event, children_dict, styles, story, last_timestamp)
        elif label == 'UnitAdmission':
            format_unit_admission(event, children_dict, styles, story, last_timestamp)
        elif label == 'LabEvent':
            format_lab_event(event, children_dict, styles, story, last_timestamp)
        elif label == 'MicrobiologyEvent':
            format_microbiology_event(event, children_dict, styles, story, last_timestamp)
        elif label == 'Procedures':
            format_procedure(event, children_dict, styles, story, last_timestamp)
        elif label == 'Prescription':
            format_prescription(event, children_dict, styles, story, last_timestamp)
        elif label == 'PreviousPrescriptionMeds':
            format_previous_meds(event, children_dict, styles, story, last_timestamp)
        elif label == 'Discharge':
            format_discharge(event, children_dict, styles, story, last_timestamp, journey_data)
            story.append(PageBreak())
        else:
            # Unknown event type - log warning but don't crash
            logger.warning(f"Unknown event type: {label}")
        
        last_timestamp = timestamp
        last_event_type = label
    
    # Build PDF
    doc.build(story, canvasmaker=NumberedCanvas)
    
    # Get PDF bytes
    pdf_bytes = buffer.getvalue()
    buffer.close()
    
    logger.info("PDF created successfully")
    return pdf_bytes

