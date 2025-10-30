# create_4_page_pdf.py
import logging
import os
import json
import re
import tempfile
import shutil
from datetime import datetime
from neo4j import GraphDatabase
import google.generativeai as genai
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Neo4j configuration
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "10016742"

# Configure Gemini API
API_KEY = 'AIzaSyDG9VKp-kD1y0-xT-5ivV7Ldni-YDR-wOk'
if not API_KEY:
    raise ValueError("Please set GEMINI_API_KEY")

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-pro')

def extract_graph_structure(session, subject_id):
    """Extract complete graph structure with nodes, relationships, and attributes"""
    logger.info(f"Extracting graph structure for patient {subject_id}...")
    
    # Query to get patient and all connected nodes with relationships
    query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    WITH p
    OPTIONAL MATCH path = (p)-[r*1..3]->(n)
    WITH p, relationships(path) as rels, nodes(path) as nodeList
    UNWIND range(0, size(rels)-1) as idx
    WITH p, rels[idx] as rel, nodeList[idx] as startNode, nodeList[idx+1] as endNode
    RETURN DISTINCT
        labels(startNode) as start_labels,
        properties(startNode) as start_props,
        type(rel) as relationship_type,
        labels(endNode) as end_labels,
        properties(endNode) as end_props
    """
    
    results = session.run(query, subject_id=str(subject_id))
    
    # Build structured graph data
    graph_data = {
        "patient_id": str(subject_id),
        "nodes": {},
        "relationships": []
    }
    
    # Get patient node separately
    patient_query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    RETURN labels(p) as labels, properties(p) as props
    """
    
    patient_result = session.run(patient_query, subject_id=str(subject_id))
    patient_record = patient_result.single()
    
    if patient_record:
        graph_data["patient"] = {
            "labels": list(patient_record['labels']),
            "properties": dict(patient_record['props'])
        }
    
    # Process relationships and nodes
    for record in results:
        start_labels = list(record['start_labels']) if record['start_labels'] else []
        start_props = dict(record['start_props']) if record['start_props'] else {}
        rel_type = record['relationship_type']
        end_labels = list(record['end_labels']) if record['end_labels'] else []
        end_props = dict(record['end_props']) if record['end_props'] else {}
        
        # Store nodes
        if start_labels and start_props:
            node_key = f"{start_labels[0]}_{start_props.get('name', 'unknown')}"
            if node_key not in graph_data["nodes"]:
                graph_data["nodes"][node_key] = {
                    "labels": start_labels,
                    "properties": start_props
                }
        
        if end_labels and end_props:
            node_key = f"{end_labels[0]}_{end_props.get('name', 'unknown')}"
            if node_key not in graph_data["nodes"]:
                graph_data["nodes"][node_key] = {
                    "labels": end_labels,
                    "properties": end_props
                }
        
        # Store relationship
        if rel_type and start_labels and end_labels:
            graph_data["relationships"].append({
                "from": {
                    "label": start_labels[0] if start_labels else "Unknown",
                    "name": start_props.get('name', 'unknown')
                },
                "relationship": rel_type,
                "to": {
                    "label": end_labels[0] if end_labels else "Unknown",
                    "name": end_props.get('name', 'unknown'),
                    "properties": end_props
                }
            })
    
    logger.info(f"Extracted {len(graph_data['nodes'])} nodes and {len(graph_data['relationships'])} relationships")
    return graph_data

def clean_json_string(json_str):
    """Clean JSON string by removing trailing commas and other common issues"""
    # Remove trailing commas before closing braces/brackets
    json_str = re.sub(r',\s*}', '}', json_str)
    json_str = re.sub(r',\s*]', ']', json_str)
    
    return json_str

def convert_markdown_to_reportlab(text):
    """Convert markdown formatting to ReportLab markup"""
    if not text:
        return text
    
    # Convert **bold** to <b>bold</b>
    text = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', text)
    
    # Convert *italic* to <i>italic</i>
    text = re.sub(r'\*([^\*]+)\*', r'<i>\1</i>', text)
    
    # Convert __underline__ to <u>underline</u>
    text = re.sub(r'__([^_]+)__', r'<u>\1</u>', text)
    
    return text

def get_llm_summary(graph_data, subject_id):
    """Send graph structure to LLM and get structured JSON summary"""
    logger.info("Sending graph structure to LLM for summarization...")
    
    # Convert graph data to JSON string for prompt
    graph_json_str = json.dumps(graph_data, indent=2, default=str)
    
    prompt = f"""You are a medical summarization expert. You will receive a KNOWLEDGE GRAPH representing a patient's clinical journey from a Neo4j database. The graph contains nodes (entities) and relationships showing how different clinical events are connected.

KNOWLEDGE GRAPH STRUCTURE:
- "patient": Core patient information with demographics
- "nodes": All clinical entities (admissions, diagnoses, procedures, lab tests, medications, etc.) with their properties
- "relationships": Connections showing how entities relate (e.g., Patient -> HAS_ADMISSION -> HospitalAdmission)

YOUR TASK:
Analyze this knowledge graph and create a comprehensive yet concise 1000-word clinical summary.

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

    try:
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
        
        # Clean JSON string to remove trailing commas
        logger.info("Cleaning JSON response...")
        response_text = clean_json_string(response_text)
        
        logger.info("Parsing LLM response as JSON...")
        summary_json = json.loads(response_text)
        
        logger.info("Successfully received and parsed LLM summary")
        return summary_json
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}")
        logger.error(f"Response text (first 1000 chars): {response_text[:1000]}...")
        
        # Try to save the problematic JSON for debugging in temp directory
        debug_filename = os.path.join(
            tempfile.gettempdir(),
            f"debug_json_{subject_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        try:
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(response_text)
            logger.error(f"Full response saved to {debug_filename} for debugging")
        except:
            pass
        
        raise
    except Exception as e:
        logger.error(f"Error getting LLM summary: {e}")
        raise

def create_pdf_from_json(summary_json, output_filename):
    """Create a well-formatted PDF from the JSON summary"""
    logger.info("Creating PDF from JSON summary...")
    
    doc = SimpleDocTemplate(
        output_filename,
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
    logger.info(f"PDF created successfully: {output_filename}")

def generate_summarized_pdf(subject_id=None):
    """Main function to generate summarized PDF report"""
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    output_dir = "Patient_Reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    # Create temporary directory for intermediate files
    temp_dir = tempfile.mkdtemp(prefix="patient_report_")
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        with driver.session() as session:
            if subject_id is None:
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
                
                print("\n" + "="*80)
                print("AVAILABLE PATIENTS IN DATABASE")
                print("="*80)
                print(f"{'Subject ID':<15} {'Gender':<10} {'Age':<10} {'Admissions':<15}")
                print("-"*80)
                for pid, gender, age, admissions in patient_list:
                    pid_str = str(pid) if pid else "N/A"
                    gender_str = str(gender) if gender else "N/A"
                    age_str = str(age) if age else "N/A"
                    admissions_str = str(admissions) if admissions else "N/A"
                    print(f"{pid_str:<15} {gender_str:<10} {age_str:<10} {admissions_str:<15}")
                print("="*80)
                
                print("\nEnter the Subject ID of the patient for whom you want to generate a summarized PDF report.")
                subject_id = input("Subject ID: ").strip()
                
                if not subject_id:
                    logger.error("No subject ID provided!")
                    print("\n❌ No subject ID provided. Exiting.")
                    return
            
            logger.info(f"Generating report for patient {subject_id}...")
            print(f"\n🔍 Looking for patient {subject_id}...")
            
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
                return
            
            patient_node = patient_record['p']
            actual_subject_id = str(patient_record['subject_id'])
            
            # Create subfolder for this patient
            patient_folder = os.path.join(output_dir, actual_subject_id)
            if not os.path.exists(patient_folder):
                os.makedirs(patient_folder)
                logger.info(f"Created patient folder: {patient_folder}")
            
            # Extract graph structure directly
            print(f"\n📊 Extracting knowledge graph structure...")
            graph_data = extract_graph_structure(session, actual_subject_id)
            
            # Save graph data to temporary directory
            graph_filename = os.path.join(
                temp_dir,
                f"Patient_{actual_subject_id}_Graph_Data.json"
            )
            with open(graph_filename, 'w', encoding='utf-8') as f:
                json.dump(graph_data, f, indent=2, default=str, ensure_ascii=False)
            logger.info(f"Graph data saved to temp: {graph_filename}")
            
            # Get LLM summary from graph structure
            print(f"🤖 Sending knowledge graph to AI for summarization (this may take a minute)...")
            summary_json = get_llm_summary(graph_data, actual_subject_id)
            
            # Save JSON summary to temporary directory
            json_filename = os.path.join(
                temp_dir,
                f"Patient_{actual_subject_id}_Summarized.json"
            )
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(summary_json, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON summary saved to temp: {json_filename}")
            
            # Create PDF with new naming convention
            pdf_filename = os.path.join(
                patient_folder,
                f"{actual_subject_id}_Summary.pdf"
            )
            
            print(f"📄 Creating formatted PDF...")
            create_pdf_from_json(summary_json, pdf_filename)
            
            # Clean up temporary files
            logger.info(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            print(f"\n✅ Summarized report generated successfully!")
            print(f"📁 Patient Folder: {os.path.abspath(patient_folder)}")
            print(f"📄 Filename: {actual_subject_id}_Summary.pdf")
            print(f"📏 PDF size: {os.path.getsize(pdf_filename) / 1024:.2f} KB")
            print(f"🧹 Temporary files cleaned up")
            
    except Exception as e:
        logger.error(f"Error generating summarized PDF: {e}", exc_info=True)
        print(f"\n❌ Error generating summarized PDF: {e}")
        # Clean up temp directory even on error
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info(f"Cleaned up temporary directory after error")
        raise
    finally:
        driver.close()

def generate_multiple_patients():
    """Generate reports for multiple patients interactively"""
    while True:
        generate_summarized_pdf()
        
        print("\n" + "="*80)
        response = input("Would you like to generate another report? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("\n👋 Thank you! Exiting report generation.")
            break
        print("\n")

if __name__ == "__main__":
    logger.info("Starting summarized PDF generation...")
    print("\n" + "="*80)
    print("AI-POWERED CLINICAL SUMMARY PDF GENERATOR")
    print("="*80)
    print("This tool extracts knowledge graph structure from Neo4j and uses AI")
    print("to create a concise 1000-word summary of patient clinical data")
    print("="*80)
    
    try:
        generate_multiple_patients()
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation interrupted by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    
    logger.info("PDF generation session complete!")

