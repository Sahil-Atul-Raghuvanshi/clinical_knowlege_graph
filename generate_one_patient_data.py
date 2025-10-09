import pandas as pd
import os
import json
from pathlib import Path
import shutil

def create_one_patient_data(target_subject_id=10035631):
    """
    Filter data for a specific subject_id from Batch_Data folder
    and save filtered results to One_Patient folder.
    """
    source_folder = "Batch_Data"
    dest_folder = "One_Patient"
    
    # Create destination folder if it doesn't exist
    Path(dest_folder).mkdir(exist_ok=True)
    print(f"Created/verified destination folder: {dest_folder}")
    
    # Load schema to understand which files have subject_id
    with open("Scripts/schema.json", "r") as f:
        schemas = json.load(f)
    
    # Files that contain subject_id and need filtering
    files_with_subject_id = []
    # Files that don't contain subject_id (dictionary/reference files)
    reference_files = []
    
    for schema in schemas:
        filename = schema["file_name"]
        if "subject_id" in schema["columns"]:
            files_with_subject_id.append(filename)
        else:
            reference_files.append(filename)
    
    print(f"\nFiles with subject_id to filter: {files_with_subject_id}")
    print(f"Reference files to copy as-is: {reference_files}")
    
    # Process files with subject_id filtering
    total_records = 0
    for filename in files_with_subject_id:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            print(f"\nProcessing {filename}...")
            
            # Read the CSV file
            df = pd.read_csv(source_path)
            print(f"  Original records: {len(df)}")
            
            # Filter for the target subject_id
            filtered_df = df[df['subject_id'] == target_subject_id]
            print(f"  Filtered records: {len(filtered_df)}")
            
            # Save filtered data
            filtered_df.to_csv(dest_path, index=False)
            total_records += len(filtered_df)
            
            if len(filtered_df) > 0:
                print(f"  ✓ Saved to {dest_path}")
            else:
                print(f"  ⚠ No records found for subject_id {target_subject_id}")
                
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {str(e)}")
    
    # Copy reference files (no filtering needed)
    print(f"\nCopying reference files...")
    for filename in reference_files:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            shutil.copy2(source_path, dest_path)
            print(f"  ✓ Copied {filename}")
        except Exception as e:
            print(f"  ✗ Error copying {filename}: {str(e)}")
    
    print(f"\n" + "="*50)
    print(f"SUMMARY")
    print(f"="*50)
    print(f"Target Subject ID: {target_subject_id}")
    print(f"Total filtered records: {total_records}")
    print(f"Files processed: {len(files_with_subject_id)} filtered + {len(reference_files)} copied")
    print(f"Output directory: {dest_folder}/")
    
    # List all output files
    output_files = list(Path(dest_folder).glob("*.csv"))
    print(f"\nGenerated files:")
    for file_path in sorted(output_files):
        file_size = file_path.stat().st_size
        if file_size < 1024:
            size_str = f"{file_size} B"
        elif file_size < 1024*1024:
            size_str = f"{file_size/1024:.1f} KB"
        else:
            size_str = f"{file_size/(1024*1024):.1f} MB"
        print(f"  - {file_path.name} ({size_str})")

if __name__ == "__main__":
    create_one_patient_data(target_subject_id=10035631)
