# create_patient_reports.py
"""
Combined script to generate both detailed patient journey PDFs and AI-powered summary PDFs
for all patients in the Neo4j database.
"""
import logging
import os
import sys
from neo4j import GraphDatabase
from datetime import datetime

# Import functions from the individual PDF generators
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import from create_patient_journey_pdf
from create_patient_journey_pdf import (
    setup_styles as setup_journey_styles,
    generate_patient_report as generate_journey_report,
    NumberedCanvas
)

# Import from create_summary_pdf
from create_summary_pdf import (
    extract_graph_structure,
    get_llm_summary,
    create_pdf_from_json
)

from reportlab.platypus import SimpleDocTemplate
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
import tempfile
import shutil

# Configure logging - Only show warnings and errors for cleaner output
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Suppress Neo4j driver logs
logging.getLogger('neo4j').setLevel(logging.ERROR)
logging.getLogger('neo4j.io').setLevel(logging.ERROR)

# Neo4j configuration
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "clinicalknowledgegraph"

# Output directory
OUTPUT_DIR = "Patient_Reports"

def get_all_patients(session):
    """Fetch all patients from the database"""
    query = """
    MATCH (p:Patient)
    RETURN p.subject_id as subject_id, p.gender as gender, 
           p.anchor_age as age, p.total_number_of_admissions as admissions, p
    ORDER BY p.subject_id
    """
    
    results = session.run(query)
    patients = []
    
    for record in results:
        patients.append({
            'subject_id': str(record['subject_id']),
            'gender': record['gender'],
            'age': record['age'],
            'admissions': record['admissions'],
            'node': record['p']
        })
    
    return patients

def generate_journey_pdf_for_patient(session, patient_data, patient_folder):
    """Generate detailed patient journey PDF"""
    subject_id = patient_data['subject_id']
    patient_node = patient_data['node']
    
    print(f"  📊 Creating detailed patient journey...", end=" ", flush=True)
    
    try:
        # Setup styles
        styles = setup_journey_styles()
        
        # Output filename
        output_filename = os.path.join(
            patient_folder,
            f"{subject_id}_Patients_Journey.pdf"
        )
        
        # Setup PDF document
        doc = SimpleDocTemplate(
            output_filename,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=1*inch,
            bottomMargin=0.75*inch
        )
        
        # Generate story (content)
        story = generate_journey_report(session, subject_id, patient_node, styles)
        
        # Build PDF
        doc.build(story, canvasmaker=NumberedCanvas)
        
        file_size = os.path.getsize(output_filename) / 1024
        print(f"✅ ({file_size:.2f} KB)")
        
        return True
        
    except Exception as e:
        logger.error(f"Error generating journey PDF for patient {subject_id}: {e}")
        print(f"❌ Error: {str(e)[:100]}")
        return False

def generate_summary_pdf_for_patient(session, patient_data, patient_folder, temp_dir):
    """Generate AI-powered summary PDF"""
    subject_id = patient_data['subject_id']
    
    print(f"  🤖 Creating AI summary...", end=" ", flush=True)
    
    try:
        # Extract graph structure
        graph_data = extract_graph_structure(session, subject_id)
        
        # Save graph data to temporary directory (for debugging if needed)
        graph_filename = os.path.join(
            temp_dir,
            f"Patient_{subject_id}_Graph_Data.json"
        )
        
        import json
        with open(graph_filename, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, default=str, ensure_ascii=False)
        
        # Get LLM summary
        summary_json = get_llm_summary(graph_data, subject_id)
        
        # Save JSON summary to temporary directory
        json_filename = os.path.join(
            temp_dir,
            f"Patient_{subject_id}_Summarized.json"
        )
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(summary_json, f, indent=2, ensure_ascii=False)
        
        # Create PDF
        pdf_filename = os.path.join(
            patient_folder,
            f"{subject_id}_Summary.pdf"
        )
        
        create_pdf_from_json(summary_json, pdf_filename)
        
        file_size = os.path.getsize(pdf_filename) / 1024
        print(f"✅ ({file_size:.2f} KB)")
        
        return True
        
    except Exception as e:
        logger.error(f"Error generating summary PDF for patient {subject_id}: {e}")
        print(f"❌ Error: {str(e)[:100]}")
        return False

def generate_reports_for_all_patients():
    """Main function to generate both reports for all patients"""
    
    print("\n" + "="*80)
    print("AUTOMATED PATIENT REPORT GENERATOR")
    print("="*80)
    print("This tool will generate both detailed journey and AI-powered summary PDFs")
    print("for ALL patients in the database.")
    print("="*80 + "\n")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    # Create temporary directory for intermediate files
    temp_dir = tempfile.mkdtemp(prefix="patient_reports_")
    
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    
    try:
        with driver.session() as session:
            # Get all patients
            patients = get_all_patients(session)
            
            if not patients:
                print("❌ No patients found in database!")
                return
            
            print(f"Found {len(patients)} patient(s) in database.\n")
            
            # Confirm with user
            response = input(f"Generate reports for all {len(patients)} patients? (yes/no): ").strip().lower()
            
            if response not in ['yes', 'y']:
                print("\n❌ Report generation cancelled by user.")
                return
            
            print("\n" + "="*80)
            print("Starting report generation...")
            print("="*80 + "\n")
            
            # Statistics
            total_patients = len(patients)
            successful_journey = 0
            successful_summary = 0
            failed = 0
            
            start_time = datetime.now()
            
            # Process each patient
            for idx, patient_data in enumerate(patients, 1):
                subject_id = patient_data['subject_id']
                
                print(f"[{idx}/{total_patients}] Processing Patient {subject_id}")
                print("-" * 80)
                
                # Create patient folder
                patient_folder = os.path.join(OUTPUT_DIR, subject_id)
                if not os.path.exists(patient_folder):
                    os.makedirs(patient_folder)
                
                # Generate journey PDF
                journey_success = generate_journey_pdf_for_patient(
                    session, 
                    patient_data, 
                    patient_folder
                )
                
                if journey_success:
                    successful_journey += 1
                
                # Generate summary PDF
                summary_success = generate_summary_pdf_for_patient(
                    session,
                    patient_data,
                    patient_folder,
                    temp_dir
                )
                
                if summary_success:
                    successful_summary += 1
                
                # Track failures
                if not (journey_success and summary_success):
                    failed += 1
                
                print()  # Blank line between patients
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Print summary
            print("="*80)
            print("REPORT GENERATION SUMMARY")
            print("="*80)
            print(f"Total Patients: {total_patients}")
            print(f"Successful Journey PDFs: {successful_journey}/{total_patients}")
            print(f"Successful Summary PDFs: {successful_summary}/{total_patients}")
            print(f"Patients with Errors: {failed}/{total_patients}")
            print(f"Total Time: {duration}")
            print(f"Output Directory: {os.path.abspath(OUTPUT_DIR)}")
            print("="*80)
            
    except Exception as e:
        logger.error(f"Error in report generation: {e}", exc_info=True)
        print(f"\n❌ Fatal error: {e}")
        
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        
        driver.close()

if __name__ == "__main__":
    try:
        generate_reports_for_all_patients()
    except KeyboardInterrupt:
        print("\n\n⚠️  Report generation interrupted by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
    
    print("\n👋 Exiting...\n")

