"""
PDF Generation Module for Patient Comparisons
Creates formatted PDF reports from JSON comparison data using ReportLab
"""
import logging
import re
import io
from datetime import datetime
from typing import Dict, Any
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

logger = logging.getLogger(__name__)


def convert_markdown_to_reportlab(text: str) -> str:
    """
    Convert markdown formatting to ReportLab markup
    
    Args:
        text: Text with markdown formatting
        
    Returns:
        Text with ReportLab markup tags
    """
    if not text:
        return text
    
    # Convert **bold** to <b>bold</b>
    text = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', text)
    
    # Convert *italic* to <i>italic</i>
    text = re.sub(r'\*([^\*]+)\*', r'<i>\1</i>', text)
    
    # Convert __underline__ to <u>underline</u>
    text = re.sub(r'__([^_]+)__', r'<u>\1</u>', text)
    
    return text


def create_pdf_from_comparison_json(comparison_json: Dict[str, Any]) -> bytes:
    """
    Create a well-formatted PDF from the JSON comparison
    
    Args:
        comparison_json: Dictionary containing the patient comparison
        
    Returns:
        PDF file as bytes for download
    """
    logger.info("Creating PDF from JSON comparison...")
    
    # Create PDF in memory
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=0.75*inch,
        bottomMargin=0.75*inch
    )
    
    # Define styles
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1a4d80'),
        spaceAfter=30,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1a4d80'),
        spaceAfter=10,
        spaceBefore=15,
        fontName='Helvetica-Bold'
    )
    
    subheading_style = ParagraphStyle(
        'CustomSubHeading',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#2c5aa0'),
        spaceAfter=8,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    )
    
    body_style = ParagraphStyle(
        'CustomBody',
        parent=styles['BodyText'],
        fontSize=10,
        leading=14,
        alignment=TA_JUSTIFY,
        spaceAfter=10
    )
    
    bullet_style = ParagraphStyle(
        'CustomBullet',
        parent=styles['BodyText'],
        fontSize=10,
        leading=14,
        leftIndent=20,
        spaceAfter=5
    )
    
    # Build document content
    content = []
    
    # Title
    content.append(Paragraph("Patient Comparison Report", title_style))
    content.append(Spacer(1, 0.1*inch))
    
    # Patient Information Box
    patient1_id = comparison_json.get('patient1_id', 'N/A')
    patient2_id = comparison_json.get('patient2_id', 'N/A')
    
    patient_data = [
        ['Patient 1 ID:', patient1_id],
        ['Patient 2 ID:', patient2_id],
        ['Report Generated:', datetime.now().strftime('%Y-%m-%d %H:%M')]
    ]
    
    patient_table = Table(patient_data, colWidths=[1.5*inch, 4.5*inch])
    patient_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e6f2ff')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
        ('ALIGN', (1, 0), (1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
    ]))
    
    content.append(patient_table)
    content.append(Spacer(1, 0.2*inch))
    
    # Comparison Summary
    if comparison_json.get('comparison_summary'):
        content.append(Paragraph("Comparison Summary", heading_style))
        summary = convert_markdown_to_reportlab(comparison_json['comparison_summary'])
        content.append(Paragraph(summary, body_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Demographics Comparison
    if comparison_json.get('demographics_comparison'):
        demo_comp = comparison_json['demographics_comparison']
        content.append(Paragraph("Demographics Comparison", heading_style))
        
        if demo_comp.get('similarities'):
            content.append(Paragraph("Similarities:", subheading_style))
            for similarity in demo_comp['similarities']:
                similarity_text = convert_markdown_to_reportlab(similarity)
                content.append(Paragraph(f"• {similarity_text}", bullet_style))
        
        if demo_comp.get('differences'):
            content.append(Paragraph("Differences:", subheading_style))
            for difference in demo_comp['differences']:
                difference_text = convert_markdown_to_reportlab(difference)
                content.append(Paragraph(f"• {difference_text}", bullet_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Presentation Comparison
    if comparison_json.get('presentation_comparison'):
        pres_comp = comparison_json['presentation_comparison']
        content.append(Paragraph("Presentation Comparison", heading_style))
        
        if pres_comp.get('similarities'):
            content.append(Paragraph("Similarities:", subheading_style))
            for similarity in pres_comp['similarities']:
                similarity_text = convert_markdown_to_reportlab(similarity)
                content.append(Paragraph(f"• {similarity_text}", bullet_style))
        
        if pres_comp.get('differences'):
            content.append(Paragraph("Differences:", subheading_style))
            for difference in pres_comp['differences']:
                difference_text = convert_markdown_to_reportlab(difference)
                content.append(Paragraph(f"• {difference_text}", bullet_style))
        
        if pres_comp.get('temporal_differences'):
            content.append(Paragraph("Temporal Differences:", subheading_style))
            temporal_diff = convert_markdown_to_reportlab(pres_comp['temporal_differences'])
            content.append(Paragraph(temporal_diff, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Diagnoses Comparison
    if comparison_json.get('diagnoses_comparison'):
        diag_comp = comparison_json['diagnoses_comparison']
        content.append(Paragraph("Diagnoses Comparison", heading_style))
        
        if diag_comp.get('common_diagnoses'):
            content.append(Paragraph("Common Diagnoses:", subheading_style))
            for diag in diag_comp['common_diagnoses']:
                diag_text = convert_markdown_to_reportlab(diag)
                content.append(Paragraph(f"• {diag_text}", bullet_style))
        
        if diag_comp.get('unique_to_patient1'):
            content.append(Paragraph(f"Unique to Patient {patient1_id}:", subheading_style))
            for diag in diag_comp['unique_to_patient1']:
                diag_text = convert_markdown_to_reportlab(diag)
                content.append(Paragraph(f"• {diag_text}", bullet_style))
        
        if diag_comp.get('unique_to_patient2'):
            content.append(Paragraph(f"Unique to Patient {patient2_id}:", subheading_style))
            for diag in diag_comp['unique_to_patient2']:
                diag_text = convert_markdown_to_reportlab(diag)
                content.append(Paragraph(f"• {diag_text}", bullet_style))
        
        if diag_comp.get('severity_comparison'):
            content.append(Paragraph("Severity Comparison:", subheading_style))
            severity = convert_markdown_to_reportlab(diag_comp['severity_comparison'])
            content.append(Paragraph(severity, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Clinical Course Comparison
    if comparison_json.get('clinical_course_comparison'):
        course_comp = comparison_json['clinical_course_comparison']
        content.append(Paragraph("Clinical Course Comparison", heading_style))
        
        if course_comp.get('similarities'):
            content.append(Paragraph("Similarities:", subheading_style))
            for similarity in course_comp['similarities']:
                similarity_text = convert_markdown_to_reportlab(similarity)
                content.append(Paragraph(f"• {similarity_text}", bullet_style))
        
        if course_comp.get('differences'):
            content.append(Paragraph("Differences:", subheading_style))
            for difference in course_comp['differences']:
                difference_text = convert_markdown_to_reportlab(difference)
                content.append(Paragraph(f"• {difference_text}", bullet_style))
        
        if course_comp.get('temporal_sequence_comparison'):
            content.append(Paragraph("Temporal Sequence Comparison:", subheading_style))
            temporal_seq = convert_markdown_to_reportlab(course_comp['temporal_sequence_comparison'])
            temporal_seq = temporal_seq.replace('\n', '<br/>')
            content.append(Paragraph(temporal_seq, body_style))
        
        if course_comp.get('length_of_stay_comparison'):
            content.append(Paragraph("Length of Stay Comparison:", subheading_style))
            los = convert_markdown_to_reportlab(course_comp['length_of_stay_comparison'])
            content.append(Paragraph(los, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Procedures Comparison
    if comparison_json.get('procedures_comparison'):
        proc_comp = comparison_json['procedures_comparison']
        content.append(Paragraph("Procedures Comparison", heading_style))
        
        if proc_comp.get('common_procedures'):
            content.append(Paragraph("Common Procedures:", subheading_style))
            for proc in proc_comp['common_procedures']:
                proc_text = convert_markdown_to_reportlab(proc)
                content.append(Paragraph(f"• {proc_text}", bullet_style))
        
        if proc_comp.get('unique_to_patient1'):
            content.append(Paragraph(f"Unique to Patient {patient1_id}:", subheading_style))
            for proc in proc_comp['unique_to_patient1']:
                proc_text = convert_markdown_to_reportlab(proc)
                content.append(Paragraph(f"• {proc_text}", bullet_style))
        
        if proc_comp.get('unique_to_patient2'):
            content.append(Paragraph(f"Unique to Patient {patient2_id}:", subheading_style))
            for proc in proc_comp['unique_to_patient2']:
                proc_text = convert_markdown_to_reportlab(proc)
                content.append(Paragraph(f"• {proc_text}", bullet_style))
        
        if proc_comp.get('timing_comparison'):
            content.append(Paragraph("Timing Comparison:", subheading_style))
            timing = convert_markdown_to_reportlab(proc_comp['timing_comparison'])
            content.append(Paragraph(timing, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Medications Comparison
    if comparison_json.get('medications_comparison'):
        med_comp = comparison_json['medications_comparison']
        content.append(Paragraph("Medications Comparison", heading_style))
        
        if med_comp.get('common_medications'):
            content.append(Paragraph("Common Medications:", subheading_style))
            for med in med_comp['common_medications']:
                med_text = convert_markdown_to_reportlab(med)
                content.append(Paragraph(f"• {med_text}", bullet_style))
        
        if med_comp.get('unique_to_patient1'):
            content.append(Paragraph(f"Unique to Patient {patient1_id}:", subheading_style))
            for med in med_comp['unique_to_patient1']:
                med_text = convert_markdown_to_reportlab(med)
                content.append(Paragraph(f"• {med_text}", bullet_style))
        
        if med_comp.get('unique_to_patient2'):
            content.append(Paragraph(f"Unique to Patient {patient2_id}:", subheading_style))
            for med in med_comp['unique_to_patient2']:
                med_text = convert_markdown_to_reportlab(med)
                content.append(Paragraph(f"• {med_text}", bullet_style))
        
        if med_comp.get('timing_comparison'):
            content.append(Paragraph("Timing Comparison:", subheading_style))
            timing = convert_markdown_to_reportlab(med_comp['timing_comparison'])
            content.append(Paragraph(timing, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Lab Findings Comparison
    if comparison_json.get('lab_findings_comparison'):
        lab_comp = comparison_json['lab_findings_comparison']
        content.append(Paragraph("Laboratory Findings Comparison", heading_style))
        
        if lab_comp.get('similar_abnormalities'):
            content.append(Paragraph("Similar Abnormalities:", subheading_style))
            for finding in lab_comp['similar_abnormalities']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        if lab_comp.get('unique_abnormalities_patient1'):
            content.append(Paragraph(f"Unique to Patient {patient1_id}:", subheading_style))
            for finding in lab_comp['unique_abnormalities_patient1']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        if lab_comp.get('unique_abnormalities_patient2'):
            content.append(Paragraph(f"Unique to Patient {patient2_id}:", subheading_style))
            for finding in lab_comp['unique_abnormalities_patient2']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        if lab_comp.get('temporal_patterns'):
            content.append(Paragraph("Temporal Patterns:", subheading_style))
            temporal = convert_markdown_to_reportlab(lab_comp['temporal_patterns'])
            content.append(Paragraph(temporal, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Microbiology Comparison
    if comparison_json.get('microbiology_comparison'):
        micro_comp = comparison_json['microbiology_comparison']
        content.append(Paragraph("Microbiology Comparison", heading_style))
        
        if micro_comp.get('common_findings'):
            content.append(Paragraph("Common Findings:", subheading_style))
            for finding in micro_comp['common_findings']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        if micro_comp.get('unique_to_patient1'):
            content.append(Paragraph(f"Unique to Patient {patient1_id}:", subheading_style))
            for finding in micro_comp['unique_to_patient1']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        if micro_comp.get('unique_to_patient2'):
            content.append(Paragraph(f"Unique to Patient {patient2_id}:", subheading_style))
            for finding in micro_comp['unique_to_patient2']:
                finding_text = convert_markdown_to_reportlab(finding)
                content.append(Paragraph(f"• {finding_text}", bullet_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Outcomes Comparison
    if comparison_json.get('outcomes_comparison'):
        outcome_comp = comparison_json['outcomes_comparison']
        content.append(Paragraph("Outcomes Comparison", heading_style))
        
        if outcome_comp.get('discharge_comparison'):
            content.append(Paragraph("Discharge Comparison:", subheading_style))
            discharge = convert_markdown_to_reportlab(outcome_comp['discharge_comparison'])
            content.append(Paragraph(discharge, body_style))
        
        if outcome_comp.get('recovery_trajectory'):
            content.append(Paragraph("Recovery Trajectory:", subheading_style))
            recovery = convert_markdown_to_reportlab(outcome_comp['recovery_trajectory'])
            content.append(Paragraph(recovery, body_style))
        
        if outcome_comp.get('key_differences'):
            content.append(Paragraph("Key Differences:", subheading_style))
            for diff in outcome_comp['key_differences']:
                diff_text = convert_markdown_to_reportlab(diff)
                content.append(Paragraph(f"• {diff_text}", bullet_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Temporal Analysis
    if comparison_json.get('temporal_analysis'):
        temp_analysis = comparison_json['temporal_analysis']
        content.append(Paragraph("Temporal Analysis", heading_style))
        
        if temp_analysis.get('event_sequence_comparison'):
            content.append(Paragraph("Event Sequence Comparison:", subheading_style))
            event_seq = convert_markdown_to_reportlab(temp_analysis['event_sequence_comparison'])
            event_seq = event_seq.replace('\n', '<br/>')
            content.append(Paragraph(event_seq, body_style))
        
        if temp_analysis.get('critical_timepoints'):
            content.append(Paragraph("Critical Timepoints:", subheading_style))
            timepoints = convert_markdown_to_reportlab(temp_analysis['critical_timepoints'])
            content.append(Paragraph(timepoints, body_style))
        
        if temp_analysis.get('timing_patterns'):
            content.append(Paragraph("Timing Patterns:", subheading_style))
            patterns = convert_markdown_to_reportlab(temp_analysis['timing_patterns'])
            content.append(Paragraph(patterns, body_style))
        
        content.append(Spacer(1, 0.1*inch))
    
    # Clinical Insights
    if comparison_json.get('clinical_insights'):
        insights = comparison_json['clinical_insights']
        content.append(Paragraph("Clinical Insights", heading_style))
        
        if insights.get('why_similar'):
            content.append(Paragraph("Why These Patients Are Similar:", subheading_style))
            why_similar = convert_markdown_to_reportlab(insights['why_similar'])
            content.append(Paragraph(why_similar, body_style))
        
        if insights.get('why_different'):
            content.append(Paragraph("Why These Patients Differ:", subheading_style))
            why_different = convert_markdown_to_reportlab(insights['why_different'])
            content.append(Paragraph(why_different, body_style))
        
        if insights.get('lessons_learned'):
            content.append(Paragraph("Lessons Learned:", subheading_style))
            lessons = convert_markdown_to_reportlab(insights['lessons_learned'])
            content.append(Paragraph(lessons, body_style))
    
    # Build PDF
    doc.build(content)
    
    # Get PDF bytes
    pdf_bytes = buffer.getvalue()
    buffer.close()
    
    logger.info("PDF created successfully")
    return pdf_bytes

