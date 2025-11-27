"""
PDF Generation Module for Patient Summaries
Creates formatted PDF reports from JSON summaries using ReportLab
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


def create_pdf_from_json(summary_json: Dict[str, Any]) -> bytes:
    """
    Create a well-formatted PDF from the JSON summary
    
    Args:
        summary_json: Dictionary containing the patient summary
        
    Returns:
        PDF file as bytes for download
    """
    logger.info("Creating PDF from JSON summary...")
    
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
    content.append(Paragraph("Clinical Summary Report", title_style))
    content.append(Spacer(1, 0.1*inch))
    
    # Patient Information Box
    patient_demo = summary_json.get('patient_demographics', {})
    patient_data = [
        ['Patient ID:', summary_json.get('patient_id', 'N/A')],
        ['Age:', patient_demo.get('age', 'N/A')],
        ['Gender:', patient_demo.get('gender', 'N/A')],
        ['Race:', patient_demo.get('race', 'N/A')],
        ['Total Admissions:', patient_demo.get('total_admissions', 'N/A')],
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
    
    # Executive Summary
    if summary_json.get('executive_summary'):
        content.append(Paragraph("Executive Summary", heading_style))
        exec_summary = convert_markdown_to_reportlab(summary_json['executive_summary'])
        content.append(Paragraph(exec_summary, body_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Chief Complaints and Presentation
    if summary_json.get('chief_complaints_and_presentation'):
        content.append(Paragraph("Chief Complaints and Presentation", heading_style))
        complaints = convert_markdown_to_reportlab(summary_json['chief_complaints_and_presentation'])
        content.append(Paragraph(complaints, body_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Key Diagnoses
    if summary_json.get('key_diagnoses'):
        content.append(Paragraph("Key Diagnoses", heading_style))
        for diagnosis in summary_json['key_diagnoses']:
            diagnosis_text = convert_markdown_to_reportlab(diagnosis)
            content.append(Paragraph(f"• {diagnosis_text}", bullet_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Clinical Course
    if summary_json.get('clinical_course'):
        content.append(Paragraph("Clinical Course", heading_style))
        clinical_course = convert_markdown_to_reportlab(summary_json['clinical_course'])
        # Replace line breaks with <br/> for proper PDF formatting
        clinical_course = clinical_course.replace('\n', '<br/>')
        content.append(Paragraph(clinical_course, body_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Significant Procedures
    if summary_json.get('significant_procedures'):
        content.append(Paragraph("Significant Procedures", heading_style))
        for procedure in summary_json['significant_procedures']:
            procedure_text = convert_markdown_to_reportlab(procedure)
            content.append(Paragraph(f"• {procedure_text}", bullet_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Key Lab Findings
    if summary_json.get('key_lab_findings'):
        content.append(Paragraph("Key Laboratory Findings", heading_style))
        for finding in summary_json['key_lab_findings']:
            finding_text = convert_markdown_to_reportlab(finding)
            content.append(Paragraph(f"• {finding_text}", bullet_style))
        content.append(Spacer(1, 0.1*inch))
    
    # Microbiology Findings
    if summary_json.get('microbiology_findings') and len(summary_json['microbiology_findings']) > 0:
        has_data = any(finding for finding in summary_json['microbiology_findings'] if finding.strip())
        if has_data:
            content.append(Paragraph("Microbiology Findings", heading_style))
            for finding in summary_json['microbiology_findings']:
                if finding.strip():
                    finding_text = convert_markdown_to_reportlab(finding)
                    content.append(Paragraph(f"• {finding_text}", bullet_style))
            content.append(Spacer(1, 0.1*inch))
    
    # Medications
    if summary_json.get('medications'):
        meds = summary_json['medications']
        if any([meds.get('started'), meds.get('stopped'), meds.get('to_avoid')]):
            content.append(Paragraph("Medication Management", heading_style))
            
            if meds.get('started'):
                content.append(Paragraph("Medications Started:", subheading_style))
                for med in meds['started']:
                    med_text = convert_markdown_to_reportlab(med)
                    content.append(Paragraph(f"• {med_text}", bullet_style))
            
            if meds.get('stopped'):
                content.append(Paragraph("Medications Stopped:", subheading_style))
                for med in meds['stopped']:
                    med_text = convert_markdown_to_reportlab(med)
                    content.append(Paragraph(f"• {med_text}", bullet_style))
            
            if meds.get('to_avoid'):
                content.append(Paragraph("Medications to Avoid:", subheading_style))
                for med in meds['to_avoid']:
                    med_text = convert_markdown_to_reportlab(med)
                    content.append(Paragraph(f"• {med_text}", bullet_style))
            
            content.append(Spacer(1, 0.1*inch))
    
    # Discharge Summary
    if summary_json.get('discharge_summary'):
        discharge = summary_json['discharge_summary']
        content.append(Paragraph("Discharge Summary", heading_style))
        
        discharge_data = []
        if discharge.get('disposition'):
            discharge_data.append(['Disposition:', discharge['disposition']])
        if discharge.get('condition'):
            discharge_data.append(['Condition:', discharge['condition']])
        if discharge.get('activity_status'):
            discharge_data.append(['Activity Status:', discharge['activity_status']])
        
        if discharge_data:
            discharge_table = Table(discharge_data, colWidths=[1.5*inch, 4.5*inch])
            discharge_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
                ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
                ('RIGHTPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            content.append(discharge_table)
            content.append(Spacer(1, 0.1*inch))
        
        if discharge.get('follow_up_instructions'):
            content.append(Paragraph("Follow-up Instructions:", subheading_style))
            follow_up = convert_markdown_to_reportlab(discharge['follow_up_instructions'])
            content.append(Paragraph(follow_up, body_style))
            content.append(Spacer(1, 0.1*inch))
    
    # Clinical Significance
    if summary_json.get('clinical_significance'):
        content.append(Paragraph("Clinical Significance", heading_style))
        clinical_sig = convert_markdown_to_reportlab(summary_json['clinical_significance'])
        content.append(Paragraph(clinical_sig, body_style))
    
    # Build PDF
    doc.build(content)
    
    # Get PDF bytes
    pdf_bytes = buffer.getvalue()
    buffer.close()
    
    logger.info("PDF created successfully")
    return pdf_bytes

