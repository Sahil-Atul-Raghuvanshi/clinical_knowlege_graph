import pandas as pd
import os
import shutil
from pathlib import Path

def create_directory_structure():
    """Create the same directory structure in Filtered_Data as in Complete_Data"""
    base_path = Path("Filtered_Data")
    
    # Create main directories
    directories = ["hosp", "icu", "ed", "note"]
    for dir_name in directories:
        dir_path = base_path / dir_name
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {dir_path}")

def filter_csv_by_subject_ids_chunked(input_file, output_file, subject_ids, chunk_size=10000):
    """Filter a CSV file by multiple subject_ids using chunked reading for large files"""
    try:
        print(f"Processing: {input_file}")
        
        # Check if file exists and get its size
        if not os.path.exists(input_file):
            print(f"File not found: {input_file}")
            return False
            
        file_size = os.path.getsize(input_file) / (1024 * 1024)  # Size in MB
        print(f"File size: {file_size:.1f} MB")
        
        # Read first chunk to check columns
        first_chunk = pd.read_csv(input_file, nrows=1)
        
        # Check if subject_id column exists
        if 'subject_id' not in first_chunk.columns:
            print(f"No 'subject_id' column found in {input_file} - copying entire file")
            # Copy the entire file to the filtered folder
            shutil.copy2(input_file, output_file)
            print(f"[SUCCESS] Copied entire file {input_file} to {output_file}")
            return True
        
        # Process file in chunks
        filtered_rows = []
        total_rows = 0
        
        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            total_rows += len(chunk)
            # Filter for all subject_ids in the list
            filtered_chunk = chunk[chunk['subject_id'].isin(subject_ids)]
            if len(filtered_chunk) > 0:
                filtered_rows.append(filtered_chunk)
            
            # Progress indicator for large files
            if total_rows % (chunk_size * 10) == 0:
                print(f"  Processed {total_rows:,} rows...")
        
        # Combine all filtered chunks
        if filtered_rows:
            filtered_df = pd.concat(filtered_rows, ignore_index=True)
            filtered_df.to_csv(output_file, index=False)
            print(f"[SUCCESS] Filtered {len(filtered_df)} rows from {total_rows:,} total rows in {input_file}")
        else:
            # Create empty file with headers
            first_chunk.iloc[0:0].to_csv(output_file, index=False)
            print(f"[SUCCESS] No matching rows found in {input_file} (processed {total_rows:,} rows)")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error processing {input_file}: {str(e)}")
        return False

def main():
    """Main function to filter all CSV files by multiple subject_ids"""
    # Define the list of subject_ids to filter
    # You can modify this list to include any subject_ids you want
    subject_ids = [
    10000032, 10001217, 10001725, 10002428, 10002495, 10002930, 10003046, 10003400,
    10004235, 10004422, 10004457, 10004720, 10004733, 10005348, 10005817, 10005866,
    10005909, 10006053, 10006580, 10007058, 10007795, 10007818, 10007928, 10008287,
    10008454, 10009035, 10009049, 10009628, 10010471, 10010867, 10011398, 10012552,
    10012853, 10013049, 10014078, 10014354, 10014729, 10015272, 10015860, 10015931,
    10016150, 10016742, 10016810, 10017492, 10018081, 10018328, 10018423, 10018501,
    10018845, 10019003, 10019172, 10019385, 10019568, 10019777, 10019917, 10020187,
    10020306, 10020640, 10020740, 10020786, 10020944, 10021118, 10021312, 10021487,
    10021666, 10021938, 10022017, 10022041, 10022281, 10022880, 10023117, 10023239,
    10023771, 10024043, 10025463, 10025612, 10026255, 10026354, 10026406, 10027445,
    10027602, 10029291, 10029484, 10031404, 10031757, 10032725, 10035185, 10035631,
    10036156, 10037861, 10037928, 10037975, 10038081, 10038933, 10038992, 10038999,
    10039708, 10039831, 10039997, 10040025
    ] # Add more subject_ids as needed, e.g., [10016742, 10002495, 10003456]
    
    print(f"\n{'='*80}")
    print(f"FILTERING DATA FOR SUBJECT IDs: {subject_ids}")
    print(f"Source: Complete_Data/")
    print(f"Destination: Filtered_Data/")
    print(f"{'='*80}\n")
    
    # Create directory structure
    create_directory_structure()
    
    # Define CSV files used in the scripts (organized by directory)
    # Files NOT in this list will be skipped during filtering
    files_to_process = {
        "Complete_Data\\hosp": [
            "admissions.csv",           # Scripts: 1_add_patient_nodes, 2_patient_flow, 10_add_provider_nodes
            "transfers.csv",            # Scripts: 2_patient_flow
            "services.csv",             # Scripts: 2_patient_flow
            "prescriptions.csv",        # Scripts: 4_add_prescription_nodes
            "microbiologyevents.csv",   # Scripts: 9_add_micro_biology_events
            "drgcodes.csv",             # Scripts: 8_add_drg_codes
            "labevents.csv",            # Scripts: 7_add_labevent_nodes
            "d_labitems.csv",           # Scripts: 7_add_labevent_nodes (lookup table)
            "diagnoses_icd.csv",        # Scripts: 6_add_diagnosis_nodes
            "d_icd_diagnoses.csv",      # Scripts: 6_add_diagnosis_nodes (lookup table)
            "procedures_icd.csv",       # Scripts: 5_add_procedure_nodes
            "d_icd_procedures.csv",     # Scripts: 5_add_procedure_nodes (lookup table)
            "patients.csv",             # Scripts: 1_add_patient_nodes
        ],
        "Complete_Data\\icu": [
            "chartevents.csv",          # Scripts: 50_add_chart_events
            "d_items.csv",              # Scripts: 5_add_procedure_nodes, 50_add_chart_events (lookup table)
            "procedureevents.csv",      # Scripts: 5_add_procedure_nodes
            "icustays.csv",             # Scripts: 3_add_icu_stays_label
        ],
        "Complete_Data\\ed": [
            "edstays.csv",              # Scripts: 2_patient_flow
            "medrecon.csv",             # Scripts: 4_add_prescription_nodes
            "pyxis.csv",                # Scripts: 4_add_prescription_nodes
            "triage.csv",               # Scripts: 11_add_assessment_nodes
            "diagnosis.csv",            # Scripts: 6_add_diagnosis_nodes
        ],
        "Complete_Data\\note": [
            "discharge.csv",            # Scripts: 48_convert_text_clinical_node_to_json (input)
            # Note: discharge_clinical_note_json.csv and discharge_clinical_note_flattened.csv 
            # are generated files, not source files to be filtered
        ],
    }
    
    total_files_processed = 0
    total_files_successful = 0
    total_files_skipped = 0
    
    for source_dir, csv_files_to_filter in files_to_process.items():
        target_dir = source_dir.replace("Complete_Data", "Filtered_Data")
        
        print(f"\nProcessing directory: {source_dir}")
        print(f"-" * 80)
        
        # Check if source directory exists
        if not os.path.exists(source_dir):
            print(f"Source directory {source_dir} does not exist, skipping...")
            continue
        
        # Get all CSV files in the source directory
        all_csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
        
        if not all_csv_files:
            print(f"No CSV files found in {source_dir}")
            continue
        
        print(f"Found {len(all_csv_files)} total CSV files")
        print(f"Processing {len(csv_files_to_filter)} files used in scripts")
        
        # Report files that will be skipped
        skipped_files = [f for f in all_csv_files if f not in csv_files_to_filter]
        if skipped_files:
            print(f"Skipping {len(skipped_files)} unused files: {', '.join(skipped_files)}")
            total_files_skipped += len(skipped_files)
        
        for csv_file in csv_files_to_filter:
            input_path = os.path.join(source_dir, csv_file)
            output_path = os.path.join(target_dir, csv_file)
            
            # Check if file exists before processing
            if not os.path.exists(input_path):
                print(f"[WARNING] Expected file not found: {csv_file}")
                continue
            
            total_files_processed += 1
            
            if filter_csv_by_subject_ids_chunked(input_path, output_path, subject_ids):
                total_files_successful += 1
    
    print(f"\n{'='*80}")
    print(f"=== SUMMARY ===")
    print(f"{'='*80}")
    print(f"Subject IDs filtered: {subject_ids}")
    print(f"Total files processed: {total_files_processed}")
    print(f"Successfully processed: {total_files_successful}")
    print(f"Failed: {total_files_processed - total_files_successful}")
    print(f"Files skipped (not used in scripts): {total_files_skipped}")
    print(f"Data saved to Filtered_Data/ (filtered by subject_ids or copied entirely if no subject_id column)")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
