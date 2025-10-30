import pandas as pd
import json
import os

def flatten_json_data(json_str):
    """
    Flatten the JSON data at a high level, extracting key fields from each section
    """
    if pd.isna(json_str) or json_str is None:
        return {}
    
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    
    flattened = {}
    
    # Administrative Information
    admin_info = data.get('Administrative Information', {})
    if admin_info:
        flattened['sex'] = admin_info.get('Sex')
        flattened['service'] = admin_info.get('Service')
        flattened['allergies'] = ', '.join(admin_info.get('Allergies', [])) if admin_info.get('Allergies') else None
    
    # Clinical Summary
    clinical_summary = data.get('Clinical Summary', {})
    if clinical_summary:
        flattened['chief_complaint'] = clinical_summary.get('Chief Complaint')
        flattened['major_procedure'] = clinical_summary.get('Major Surgical or Invasive Procedure')
        
        pmh = clinical_summary.get('Past Medical History', [])
        flattened['past_medical_history'] = ', '.join(pmh) if isinstance(pmh, list) else pmh
        
        flattened['social_history'] = clinical_summary.get('Social History')
        flattened['family_history'] = clinical_summary.get('Family History')
    
    # History of Present Illness
    hpi = data.get('History of Present Illness', {})
    if hpi:
        flattened['hpi_summary'] = hpi.get('Summary')
    
    # Physical Examination - Aggregate Vitals into strings
    phys_exam = data.get('Physical Examination', {})
    
    # Admission Vitals (combine ED and Admission exam vitals)
    admission_vitals_list = []
    
    # ED Vitals
    if hpi:
        ed_findings = hpi.get('ED Findings', {})
        if ed_findings and isinstance(ed_findings, dict):
            ed_vitals = ed_findings.get('Vitals', {})
            if ed_vitals:
                if ed_vitals.get('Temperature'):
                    admission_vitals_list.append(f"Temperature: {ed_vitals.get('Temperature')}")
                if ed_vitals.get('Heart Rate'):
                    admission_vitals_list.append(f"Heart Rate: {ed_vitals.get('Heart Rate')}")
                if ed_vitals.get('Blood Pressure'):
                    admission_vitals_list.append(f"Blood Pressure: {ed_vitals.get('Blood Pressure')}")
                if ed_vitals.get('Respiratory Rate'):
                    admission_vitals_list.append(f"Respiratory Rate: {ed_vitals.get('Respiratory Rate')}")
                if ed_vitals.get('SpO2'):
                    admission_vitals_list.append(f"SpO2: {ed_vitals.get('SpO2')}")
    
    # Admission Exam Vitals
    if phys_exam:
        admission_exam = phys_exam.get('Admission Exam', {})
        if admission_exam:
            admission_vitals = admission_exam.get('Vitals', {})
            if admission_vitals:
                if admission_vitals.get('Temperature'):
                    admission_vitals_list.append(f"Temperature: {admission_vitals.get('Temperature')}")
                if admission_vitals.get('Heart Rate'):
                    admission_vitals_list.append(f"Heart Rate: {admission_vitals.get('Heart Rate')}")
                if admission_vitals.get('Blood Pressure'):
                    admission_vitals_list.append(f"Blood Pressure: {admission_vitals.get('Blood Pressure')}")
                if admission_vitals.get('Respiratory Rate'):
                    admission_vitals_list.append(f"Respiratory Rate: {admission_vitals.get('Respiratory Rate')}")
                if admission_vitals.get('SpO2'):
                    admission_vitals_list.append(f"SpO2: {admission_vitals.get('SpO2')}")
            
            if admission_exam.get('General'):
                admission_vitals_list.append(f"General: {admission_exam.get('General')}")
    
    flattened['admission_vitals'] = ', '.join(admission_vitals_list) if admission_vitals_list else None
    
    # Discharge Vitals
    discharge_vitals_list = []
    if phys_exam:
        discharge_exam = phys_exam.get('Discharge Exam', {})
        if discharge_exam:
            discharge_vitals = discharge_exam.get('Vitals', {})
            if discharge_vitals:
                if discharge_vitals.get('Temperature'):
                    discharge_vitals_list.append(f"Temperature: {discharge_vitals.get('Temperature')}")
                if discharge_vitals.get('Heart Rate'):
                    discharge_vitals_list.append(f"Heart Rate: {discharge_vitals.get('Heart Rate')}")
                if discharge_vitals.get('Blood Pressure'):
                    discharge_vitals_list.append(f"Blood Pressure: {discharge_vitals.get('Blood Pressure')}")
                if discharge_vitals.get('Respiratory Rate'):
                    discharge_vitals_list.append(f"Respiratory Rate: {discharge_vitals.get('Respiratory Rate')}")
                if discharge_vitals.get('SpO2'):
                    discharge_vitals_list.append(f"SpO2: {discharge_vitals.get('SpO2')}")
            
            if discharge_exam.get('General'):
                discharge_vitals_list.append(f"General: {discharge_exam.get('General')}")
    
    flattened['discharge_vitals'] = ', '.join(discharge_vitals_list) if discharge_vitals_list else None
    
    # Pertinent Results - Aggregate Labs into strings
    pertinent_results = data.get('Pertinent Results', {})
    
    # Admission Labs
    admission_labs_list = []
    if pertinent_results:
        admission_labs = pertinent_results.get('Admission Labs', {})
        if admission_labs:
            # Handle both direct dict and nested 'description' dict
            if 'description' in admission_labs:
                admission_labs = admission_labs['description']
            
            if admission_labs.get('WBC'):
                admission_labs_list.append(f"WBC: {admission_labs.get('WBC')}")
            if admission_labs.get('Hemoglobin'):
                admission_labs_list.append(f"Hemoglobin: {admission_labs.get('Hemoglobin')}")
            if admission_labs.get('Platelets'):
                admission_labs_list.append(f"Platelets: {admission_labs.get('Platelets')}")
            if admission_labs.get('Creatinine'):
                admission_labs_list.append(f"Creatinine: {admission_labs.get('Creatinine')}")
            if admission_labs.get('Glucose'):
                admission_labs_list.append(f"Glucose: {admission_labs.get('Glucose')}")
            
            troponin = admission_labs.get('Troponin') or admission_labs.get('Troponin-T')
            if troponin:
                admission_labs_list.append(f"Troponin: {troponin}")
            
            if admission_labs.get('Lactate'):
                admission_labs_list.append(f"Lactate: {admission_labs.get('Lactate')}")
            if admission_labs.get('BUN'):
                admission_labs_list.append(f"BUN: {admission_labs.get('BUN')}")
            if admission_labs.get('Sodium'):
                admission_labs_list.append(f"Sodium: {admission_labs.get('Sodium')}")
            if admission_labs.get('Potassium'):
                admission_labs_list.append(f"Potassium: {admission_labs.get('Potassium')}")
    
    flattened['admission_labs'] = ', '.join(admission_labs_list) if admission_labs_list else None
    
    # Discharge Labs
    discharge_labs_list = []
    if pertinent_results:
        discharge_labs = pertinent_results.get('Discharge Labs', {})
        if discharge_labs:
            # Handle both direct dict and nested 'description' dict
            if 'description' in discharge_labs:
                discharge_labs = discharge_labs['description']
            
            if discharge_labs.get('WBC'):
                discharge_labs_list.append(f"WBC: {discharge_labs.get('WBC')}")
            if discharge_labs.get('Hemoglobin'):
                discharge_labs_list.append(f"Hemoglobin: {discharge_labs.get('Hemoglobin')}")
            if discharge_labs.get('Hematocrit'):
                discharge_labs_list.append(f"Hematocrit: {discharge_labs.get('Hematocrit')}")
            if discharge_labs.get('Creatinine'):
                discharge_labs_list.append(f"Creatinine: {discharge_labs.get('Creatinine')}")
            if discharge_labs.get('Glucose'):
                discharge_labs_list.append(f"Glucose: {discharge_labs.get('Glucose')}")
            if discharge_labs.get('BUN'):
                discharge_labs_list.append(f"BUN: {discharge_labs.get('BUN')}")
            if discharge_labs.get('Sodium'):
                discharge_labs_list.append(f"Sodium: {discharge_labs.get('Sodium')}")
            if discharge_labs.get('Potassium'):
                discharge_labs_list.append(f"Potassium: {discharge_labs.get('Potassium')}")
    
    flattened['discharge_labs'] = ', '.join(discharge_labs_list) if discharge_labs_list else None
    
    # Microbiology - Summary
    microbiology = data.get('Microbiology', {})
    if microbiology:
        # Handle both direct dict and nested 'description' string
        if 'description' in microbiology:
            flattened['microbiology_findings'] = microbiology['description']
        else:
            # Combine all microbiology findings into a single field
            micro_findings = []
            for key, value in microbiology.items():
                if value:
                    micro_findings.append(f"{key}: {value}")
            flattened['microbiology_findings'] = '; '.join(micro_findings) if micro_findings else None
    else:
        # Always include the field, even if None
        flattened['microbiology_findings'] = None
    
    # Imaging Studies - Count and summary
    imaging_studies = data.get('Imaging Studies', [])
    if imaging_studies and isinstance(imaging_studies, list):
        flattened['imaging_count'] = len(imaging_studies)
        # Get study types
        study_types = [img.get('Study Type', '') for img in imaging_studies if img.get('Study Type')]
        flattened['imaging_studies'] = ', '.join(study_types) if study_types else None
    
    # Hospital Course
    hospital_course = data.get('Hospital Course', {})
    if hospital_course:
        # Handle both dict with sub-keys and dict with 'description'
        if isinstance(hospital_course, dict):
            if 'description' in hospital_course:
                flattened['hospital_course'] = hospital_course['description']
            else:
                # Combine all problems from hospital course
                course_items = []
                for key, value in hospital_course.items():
                    if value and isinstance(value, str):
                        course_items.append(f"{key}: {value}")
                flattened['hospital_course'] = '; '.join(course_items) if course_items else None
    
    # Transitional Issues
    transitional = data.get('Transitional Issues', {})
    if transitional:
        flattened['antibiotic_plan'] = transitional.get('Antibiotic Plan')
        flattened['code_status'] = transitional.get('Code Status')
        
        # Medication changes
        med_changes = transitional.get('Medication Changes', {})
        if med_changes:
            started = med_changes.get('Start', [])
            stopped = med_changes.get('Stop', [])
            avoided = med_changes.get('Avoid', [])
            
            flattened['medications_started'] = ', '.join(started) if started else None
            flattened['medications_stopped'] = ', '.join(stopped) if stopped else None
            flattened['medications_to_avoid'] = ', '.join(avoided) if avoided else None
    
    # Medications
    medications = data.get('Medications', {})
    if medications:
        admission_meds = medications.get('On Admission', [])
        discharge_meds = medications.get('On Discharge', [])
        
        flattened['admission_medications'] = ', '.join(admission_meds) if admission_meds else None
        flattened['discharge_medications'] = ', '.join(discharge_meds) if discharge_meds else None
    
    # Discharge Information
    discharge_info = data.get('Discharge Information', {})
    if discharge_info:
        flattened['disposition'] = discharge_info.get('Disposition')
        flattened['facility_name'] = discharge_info.get('Facility Name')
        
        primary_dx = discharge_info.get('Primary Diagnoses', [])
        flattened['primary_diagnoses'] = ', '.join(primary_dx) if isinstance(primary_dx, list) else primary_dx
        
        secondary_dx = discharge_info.get('Secondary Diagnoses', [])
        flattened['secondary_diagnoses'] = ', '.join(secondary_dx) if isinstance(secondary_dx, list) else secondary_dx
        
        condition = discharge_info.get('Condition', {})
        if condition:
            flattened['mental_status'] = condition.get('Mental Status')
            flattened['level_of_consciousness'] = condition.get('Level of Consciousness')
            flattened['activity_status'] = condition.get('Activity Status')
        
        flattened['discharge_instructions'] = discharge_info.get('Discharge Instructions')
    
    return flattened


def process_clinical_notes_csv(input_csv, output_csv):
    """
    Process the CSV file with JSON data and create a flattened version
    """
    print(f"Reading input CSV: {input_csv}")
    df = pd.read_csv(input_csv)
    
    print(f"Total rows: {len(df)}")
    print(f"Columns in input: {list(df.columns)}")
    
    # Check which notes are already flattened
    processed_note_ids = set()
    if os.path.exists(output_csv):
        print(f"\nOutput CSV exists. Checking for already flattened notes...")
        try:
            existing_df = pd.read_csv(output_csv)
            processed_note_ids = set(existing_df['note_id'].tolist())
            print(f"Found {len(processed_note_ids)} already flattened notes")
        except Exception as e:
            print(f"Warning: Could not read existing output CSV: {e}")
            print("Starting fresh...")
    
    # Filter to only unprocessed notes
    df_to_process = df[~df['note_id'].isin(processed_note_ids)].copy()
    
    total_notes = len(df)
    already_processed = len(processed_note_ids)
    to_process = len(df_to_process)
    
    print(f"\nTotal notes in input: {total_notes}")
    print(f"Already flattened: {already_processed}")
    print(f"Remaining to flatten: {to_process}")
    
    if to_process == 0:
        print("\n" + "="*60)
        print("All notes already flattened! Nothing to do.")
        print("="*60)
        return
    
    # Counter for statistics
    successful = 0
    failed = 0
    
    # Determine if we need to write headers (first time) or append
    write_header = not os.path.exists(output_csv)
    
    # Apply flattening function to each row
    print("\nFlattening JSON data...")
    
    for idx, row in df_to_process.iterrows():
        print(f"\nProcessing note {successful + failed + 1}/{to_process}")
        print(f"Note ID: {row['note_id']}")
        
        try:
            # Start with original columns (except json_data)
            row_data = {
                'note_id': row['note_id'],
                'subject_id': row['subject_id'],
                'hadm_id': row['hadm_id'],
                'note_type': row['note_type'],
                'note_seq': row['note_seq'],
                'charttime': row['charttime'],
                'storetime': row['storetime']
            }
            
            # Add flattened JSON fields
            flattened = flatten_json_data(row['json_data'])
            row_data.update(flattened)
            
            # Create a single-row DataFrame
            output_df = pd.DataFrame([row_data])
            
            # Append to CSV immediately
            output_df.to_csv(output_csv, mode='a', header=write_header, index=False)
            write_header = False  # After first write, don't write headers again
            
            successful += 1
            print(f"[SUCCESS] Successfully flattened and saved note {row['note_id']}")
            
        except Exception as e:
            failed += 1
            print(f"[ERROR] Failed to flatten note {row['note_id']}: {e}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("FLATTENING COMPLETE")
    print("="*60)
    print(f"Total notes in dataset: {total_notes}")
    print(f"Already flattened (before this run): {already_processed}")
    print(f"Processed in this run: {successful + failed}")
    print(f"Successfully flattened: {successful}")
    print(f"Failed: {failed}")
    print(f"Total flattened now: {already_processed + successful}")
    print(f"Output saved to: {output_csv}")
    print("="*60)


def main():
    # Define file paths
    input_csv = "Filtered_Data/note/discharge_clinical_note_json.csv"
    output_csv = "Filtered_Data/note/discharge_clinical_note_flattened.csv"
    
    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input CSV not found at {input_csv}")
        return
    
    # Process the clinical notes
    process_clinical_notes_csv(input_csv, output_csv)


if __name__ == "__main__":
    main()

