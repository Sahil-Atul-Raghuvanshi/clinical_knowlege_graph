# create_summarization_txt.py
import logging
from neo4j import GraphDatabase
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Neo4j configuration
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "10016742"

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

def clean_text(text):
    """Clean text by replacing ___ with appropriate placeholder"""
    if text:
        text = text.replace('___', '[redacted]')
        text = text.replace('at ___ within', 'at [hospital contact] within')
    return text

def format_patient_info(patient_node, lines):
    """Format patient information section"""
    props = patient_node._properties
    
    gender = props.get('gender', 'N/A')
    age = props.get('anchor_age', 'N/A')
    race = props.get('race', 'N/A')
    admissions = props.get('total_number_of_admissions', 'N/A')
    
    lines.append(f"PATIENT: ID={props.get('subject_id', 'N/A')}, Age={age}, Gender={gender}, Race={race}, TotalAdmissions={admissions}.")

def format_ed_visit(node, timestamp, children_dict, lines):
    """Format Emergency Department visit"""
    props = node._properties
    
    arrival = props.get('arrival_transport', 'N/A')
    outtime = props.get('outtime', 'N/A')
    period = props.get('period', 'N/A')
    disposition = props.get('disposition', 'N/A')
    
    ed_text = f"ED_VISIT: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Arrival={arrival}, Departed={outtime}, Duration={period}, Disposition={disposition}."
    
    ed_discharge_keywords = ['HOME', 'DISCHARGED', 'AGAINST ADVICE', 'LEFT', 'AMA']
    is_ed_discharge = any(keyword in disposition.upper() for keyword in ed_discharge_keywords) if disposition != 'N/A' else False
    
    if is_ed_discharge:
        ed_text += f" DirectEDDischarge=True."
    
    for child, rel_type in children_dict.get('Diagnosis', []):
        if child._properties.get('ed_diagnosis') == 'True':
            diagnoses = child._properties.get('complete_diagnosis', [])
            if diagnoses:
                ed_text += f" InitialDiagnoses: {'; '.join(diagnoses)}."
    
    for child, rel_type in children_dict.get('InitialAssessment', []):
        child_props = child._properties
        
        chief_complaint_raw = clean_text(child_props.get('chiefcomplaint', 'N/A'))
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
            ed_text += f" TransferFrom={arrival_method}."
        
        if actual_complaints:
            ed_text += f" ChiefComplaint={', '.join(actual_complaints)}."
        elif not arrival_method:
            ed_text += f" ChiefComplaint={chief_complaint_raw}."
        
        vitals = []
        if 'sbp' in child_props and 'dbp' in child_props:
            vitals.append(f"BP={child_props['sbp']}/{child_props['dbp']}")
        if 'heartrate' in child_props:
            vitals.append(f"HR={child_props['heartrate']}")
        if 'resprate' in child_props:
            vitals.append(f"RR={child_props['resprate']}")
        if 'o2sat' in child_props:
            vitals.append(f"O2Sat={child_props['o2sat']}")
        if 'temperature' in child_props:
            vitals.append(f"Temp={child_props['temperature']}")
        if 'pain' in child_props:
            vitals.append(f"Pain={child_props['pain']}")
        if 'acuity' in child_props:
            vitals.append(f"Acuity={child_props['acuity']}")
        
        if vitals:
            ed_text += f" TriageVitals: {', '.join(vitals)}."
    
    lines.append(ed_text)

def format_administered_meds(node, timestamp, lines):
    """Format administered medications"""
    props = node._properties
    medications = props.get('medications', [])
    med_count = props.get('medication_count', len(medications))
    lines.append(f"MEDS_ADMINISTERED: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Count={med_count}, Meds=[{'; '.join(medications)}].")

def format_hospital_admission(node, timestamp, children_dict, lines):
    """Format hospital admission"""
    props = node._properties
    
    admit_text = f"HOSPITAL_ADMISSION: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, From={props.get('admission_location', 'N/A')}, Type={props.get('admission_type', 'N/A')}, Provider={props.get('admit_provider_id', 'N/A')}, Insurance={props.get('insurance', 'N/A')}, Service={props.get('service', 'N/A')}, ChiefComplaint={props.get('chief_complaint', 'N/A')}, Race={props.get('race', 'N/A')}, Marital={props.get('marital_status', 'N/A')}, Language={props.get('language', 'N/A')}."
    
    for child, rel_type in children_dict.get('DRG', []):
        child_props = child._properties
        drg_info = f" DRG_{child_props.get('drg_type', 'Unknown')}: Code={child_props.get('drg_code', 'N/A')}, Desc={child_props.get('description', 'N/A')}"
        if 'drg_severity' in child_props:
            drg_info += f", Severity={child_props['drg_severity']}"
        if 'drg_mortality' in child_props:
            drg_info += f", Mortality={child_props['drg_mortality']}"
        admit_text += drg_info + "."
    
    for child, rel_type in children_dict.get('PatientPastHistory', []):
        child_props = child._properties
        admit_text += f" PastHistory: PMH={child_props.get('past_medical_history', 'N/A')}, FH={child_props.get('family_history', 'N/A')}, SH={child_props.get('social_history', 'N/A')}."
    
    for child, rel_type in children_dict.get('HPISummary', []):
        child_props = child._properties
        admit_text += f" HPI: {child_props.get('summary', 'N/A')}."
    
    for child, rel_type in children_dict.get('AdmissionVitals', []):
        child_props = child._properties
        admit_text += f" AdmitVitals: General={child_props.get('General', 'N/A')}, BP={child_props.get('Blood_Pressure', 'N/A')}, HR={child_props.get('Heart_Rate', 'N/A')}, RR={child_props.get('Respiratory_Rate', 'N/A')}, Temp={child_props.get('Temperature', 'N/A')}, SpO2={child_props.get('SpO2', 'N/A')}."
    
    for child, rel_type in children_dict.get('AdmissionLabs', []):
        child_props = child._properties
        lab_tests = child_props.get('lab_tests', [])
        if lab_tests:
            admit_text += f" AdmitLabs: [{'; '.join(lab_tests)}]."
    
    for child, rel_type in children_dict.get('AdmissionMedications', []):
        child_props = child._properties
        medications = child_props.get('medications', [])
        if medications:
            admit_text += f" AdmitMeds: [{'; '.join(medications)}]."
    
    lines.append(admit_text)

def format_unit_admission(node, timestamp, children_dict, lines):
    """Format regular ward/unit admission (non-ICU)"""
    props = node._properties
    lines.append(f"UNIT_ADMISSION: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Unit={props.get('careunit', 'N/A')}, OutTime={props.get('outtime', 'N/A')}, Duration={props.get('period', 'N/A')}, Service={props.get('service_given', 'N/A')}.")

def format_icu_stay(node, timestamp, children_dict, lines):
    """Format ICU stay"""
    props = node._properties
    icu_text = f"ICU_STAY: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Unit={props.get('careunit', 'N/A')}, FirstUnit={props.get('first_careunit', props.get('careunit', 'N/A'))}, LastUnit={props.get('last_careunit', props.get('careunit', 'N/A'))}, OutTime={props.get('outtime', 'N/A')}, Duration={props.get('period', 'N/A')}, LOS={props.get('los', 'N/A')}, Service={props.get('service_given', 'N/A')}."
    lines.append(icu_text)

def format_lab_event(node, timestamp, lines):
    """Format laboratory event - only abnormal values"""
    props = node._properties
    lab_results = props.get('lab_results', [])
    
    # Filter only abnormal results
    abnormal_results = [result for result in lab_results if '[abnormal]' in result.lower()]
    
    # Only add if there are abnormal results
    if abnormal_results:
        lines.append(f"LAB_EVENT: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, TotalTests={props.get('lab_count', 0)}, AbnormalResults=[{'; '.join(abnormal_results)}].")
    else:
        # If no abnormal results, just note that labs were done
        lines.append(f"LAB_EVENT: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, TotalTests={props.get('lab_count', 0)}, AbnormalResults=None.")

def format_microbiology_event(node, timestamp, lines):
    """Format microbiology event"""
    props = node._properties
    micro_results = props.get('micro_results', [])
    cleaned_results = [clean_text(r) for r in micro_results]
    lines.append(f"MICRO_EVENT: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Count={props.get('micro_count', 0)}, Results=[{'; '.join(cleaned_results)}].")

def format_procedure(node, timestamp, lines):
    """Format procedure"""
    props = node._properties
    procedures = props.get('procedures', [])
    lines.append(f"PROCEDURES: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Count={props.get('procedure_count', 0)}, Source={props.get('source', 'N/A')}, Procs=[{'; '.join(procedures)}].")

def format_prescription(node, timestamp, lines):
    """Format prescription"""
    props = node._properties
    medicines = props.get('medicines', [])
    lines.append(f"PRESCRIPTIONS: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Count={props.get('medicine_count', 0)}, Meds=[{'; '.join(medicines)}].")

def format_previous_meds(node, timestamp, lines):
    """Format previous prescription medications"""
    props = node._properties
    medications = props.get('medications', [])
    lines.append(f"PREVIOUS_MEDS: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, Count={props.get('medication_count', 0)}, Meds=[{'; '.join(medications)}].")

def format_discharge(node, timestamp, children_dict, lines):
    """Format discharge information"""
    props = node._properties
    
    discharge_text = f"DISCHARGE: Time={timestamp.strftime('%Y-%m-%d %H:%M')}, From={props.get('careunit', 'N/A')}, Disposition={props.get('disposition', 'N/A')}, MajorProc={props.get('major_procedure', 'None')}, Allergies={props.get('allergies', 'None')}."
    
    allergy_nodes = children_dict.get('AllergyIdentified', [])
    if allergy_nodes:
        allergy_list = []
        for child, rel_type in allergy_nodes:
            allergy_name = child._properties.get('allergy_name', 'Unknown')
            if allergy_name and allergy_name not in allergy_list:
                allergy_list.append(allergy_name)
        if allergy_list:
            discharge_text += f" DetailedAllergies=[{'; '.join(allergy_list)}]."
    
    for child, rel_type in children_dict.get('Diagnosis', []):
        child_props = child._properties
        primary_diagnoses = child_props.get('primary_diagnoses', [])
        secondary_diagnoses = child_props.get('secondary_diagnoses', [])
        complete_diagnosis = child_props.get('complete_diagnosis', [])
        
        if primary_diagnoses:
            if isinstance(primary_diagnoses, list):
                discharge_text += f" PrimaryDx=[{'; '.join(primary_diagnoses)}]."
            else:
                discharge_text += f" PrimaryDx=[{primary_diagnoses}]."
        
        if secondary_diagnoses:
            if isinstance(secondary_diagnoses, list):
                discharge_text += f" SecondaryDx=[{'; '.join(secondary_diagnoses)}]."
            else:
                discharge_text += f" SecondaryDx=[{secondary_diagnoses}]."
        
        if complete_diagnosis and len(complete_diagnosis) > 0:
            discharge_text += f" CompleteDx=[{'; '.join(complete_diagnosis)}]."
    
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
            discharge_text += f" HospitalCourse: {hospital_course}."
        if microbiology_findings:
            discharge_text += f" MicroFindings: {microbiology_findings}."
        if antibiotic_plan:
            discharge_text += f" AntibioticPlan: {antibiotic_plan}."
        
        discharge_text += f" DischargeStatus: Activity={activity_status}, LOC={level_of_consciousness}, Mental={mental_status}, Code={code_status}."
        
        if discharge_instructions:
            discharge_text += f" Instructions: {discharge_instructions}."
    
    for child, rel_type in children_dict.get('MedicationStarted', []):
        medications = child._properties.get('medications', [])
        if medications:
            discharge_text += f" MedsStarted=[{'; '.join(medications)}]."
    
    for child, rel_type in children_dict.get('MedicationStopped', []):
        medications = child._properties.get('medications', [])
        if medications:
            discharge_text += f" MedsStopped=[{'; '.join(medications)}]."
    
    for child, rel_type in children_dict.get('MedicationToAvoid', []):
        medications = child._properties.get('medications', [])
        if medications:
            discharge_text += f" MedsToAvoid=[{'; '.join(medications)}]."
    
    lines.append(discharge_text)

def calculate_time_gap(last_timestamp, current_timestamp):
    """Calculate time gap between events"""
    if last_timestamp and current_timestamp:
        delta = current_timestamp - last_timestamp
        days = delta.days
        hours = delta.seconds // 3600
        minutes = (delta.seconds % 3600) // 60
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        
        return " ".join(parts) if parts else "<1m"
    return None

def generate_patient_report(session, subject_id, patient_node):
    """Generate a single patient's report content"""
    logger.info(f"Processing patient {subject_id}...")
    
    lines = []
    
    lines.append(f"REPORT: PatientID={subject_id}, Generated={datetime.now().strftime('%Y-%m-%d %H:%M')}, Database={DATABASE}.")
    
    nodes_query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    WITH p
    OPTIONAL MATCH (p)-[*]->(n)
    WHERE n.name IS NOT NULL
    RETURN DISTINCT n
    """
    
    results = session.run(nodes_query, subject_id=str(subject_id))
    
    nodes_with_timestamps = []
    
    for record in results:
        node = record['n']
        if node is None:
            continue
        
        timestamp = extract_timestamp(node)
        if timestamp:
            nodes_with_timestamps.append((node, timestamp))
    
    nodes_with_timestamps.sort(key=lambda x: x[1])
    
    format_patient_info(patient_node, lines)
    
    last_timestamp = None
    
    for node, timestamp in nodes_with_timestamps:
        label = list(node.labels)[0] if node.labels else "Unknown"
        
        if last_timestamp and label in ['EmergencyDepartment', 'HospitalAdmission']:
            gap = calculate_time_gap(last_timestamp, timestamp)
            if gap:
                lines.append(f"[GAP: {gap}]")
        
        children = get_child_nodes(session, node)
        children_dict = {}
        for child, rel_type in children:
            child_label = list(child.labels)[0] if child.labels else "Unknown"
            if child_label not in children_dict:
                children_dict[child_label] = []
            children_dict[child_label].append((child, rel_type))
        
        if label == 'EmergencyDepartment':
            format_ed_visit(node, timestamp, children_dict, lines)
        elif label == 'AdministeredMeds':
            format_administered_meds(node, timestamp, lines)
        elif label == 'HospitalAdmission':
            format_hospital_admission(node, timestamp, children_dict, lines)
        elif label == 'ICUStay':
            format_icu_stay(node, timestamp, children_dict, lines)
        elif label == 'UnitAdmission':
            format_unit_admission(node, timestamp, children_dict, lines)
        elif label == 'LabEvent':
            format_lab_event(node, timestamp, lines)
        elif label == 'MicrobiologyEvent':
            format_microbiology_event(node, timestamp, lines)
        elif label == 'Procedures':
            format_procedure(node, timestamp, lines)
        elif label == 'Prescription':
            format_prescription(node, timestamp, lines)
        elif label == 'PreviousPrescriptionMeds':
            format_previous_meds(node, timestamp, lines)
        elif label == 'Discharge':
            format_discharge(node, timestamp, children_dict, lines)
        
        last_timestamp = timestamp
    
    return lines

def generate_patient_summary_txt(subject_id=None):
    """Generate comprehensive patient summary TXT for a specific patient"""
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    folder_name = get_folder_name()
    
    output_dir = "Patient_Reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
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
                
                print("\nEnter the Subject ID of the patient for whom you want to generate a report.")
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
            
            output_filename = os.path.join(
                output_dir, 
                f"Patient_{actual_subject_id}_Clinical_Summary.txt"
            )
            
            if os.path.exists(output_filename):
                print(f"\n⚠️  Existing report found: {output_filename}")
                print("   This will be replaced with the new report.")
            
            print(f"\n📊 Generating clinical summary text report...")
            
            lines = generate_patient_report(session, actual_subject_id, patient_node)
            
            logger.info(f"Writing text file for patient {actual_subject_id}...")
            print(f"📝 Writing report...")
            
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(' '.join(lines))
            
            logger.info(f"TXT generated successfully: {output_filename}")
            print(f"\n✅ Report generated successfully!")
            print(f"📄 Location: {os.path.abspath(output_filename)}")
            print(f"📏 File size: {os.path.getsize(output_filename) / 1024:.2f} KB")
            
    except Exception as e:
        logger.error(f"Error generating TXT: {e}", exc_info=True)
        print(f"\n❌ Error generating TXT: {e}")
        raise
    finally:
        driver.close()

def generate_multiple_patients():
    """Generate reports for multiple patients interactively"""
    while True:
        generate_patient_summary_txt()
        
        print("\n" + "="*80)
        response = input("Would you like to generate another report? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("\n👋 Thank you! Exiting report generation.")
            break
        print("\n")

if __name__ == "__main__":
    logger.info("Starting patient summary TXT generation...")
    print("\n" + "="*80)
    print("CLINICAL PATIENT SUMMARY TEXT REPORT GENERATOR")
    print("="*80)
    
    try:
        generate_multiple_patients()
    except KeyboardInterrupt:
        print("\n\n⚠️  Generation interrupted by user.")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
    
    logger.info("TXT generation session complete!")

