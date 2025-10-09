import pandas as pd
import os
import json
from pathlib import Path
import shutil

def create_one_patient_one_admission_data(target_subject_id=10035631, target_hadm_id=29462354):
    """
    Filter data for a specific subject_id and hadm_id from Batch_Data folder
    and save filtered results to One_Patient_One_Admission folder.
    """
    source_folder = "Batch_Data"
    dest_folder = "One_Patient_One_Admission"
    
    # Create destination folder if it doesn't exist
    Path(dest_folder).mkdir(exist_ok=True)
    print(f"Created/verified destination folder: {dest_folder}")
    
    # Load schema to understand which files have subject_id and hadm_id
    with open("Scripts/schema.json", "r") as f:
        schemas = json.load(f)
    
    # Categorize files based on their columns
    files_with_both_ids = []      # subject_id AND hadm_id
    files_with_subject_only = []  # subject_id only
    reference_files = []          # neither (dictionary/reference files)
    
    for schema in schemas:
        filename = schema["file_name"]
        columns = schema["columns"]
        
        if "subject_id" in columns and "hadm_id" in columns:
            files_with_both_ids.append(filename)
        elif "subject_id" in columns:
            files_with_subject_only.append(filename)
        else:
            reference_files.append(filename)
    
    print(f"\nFiles with both subject_id and hadm_id (dual filter): {files_with_both_ids}")
    print(f"Files with subject_id only (single filter): {files_with_subject_only}")
    print(f"Reference files (copy as-is): {reference_files}")
    
    total_records = 0
    
    # Process files with both subject_id and hadm_id (dual filtering)
    print(f"\n{'='*60}")
    print(f"DUAL FILTERING (subject_id={target_subject_id} AND hadm_id={target_hadm_id})")
    print(f"{'='*60}")
    
    for filename in files_with_both_ids:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            print(f"\nProcessing {filename}...")
            
            # Read the CSV file
            df = pd.read_csv(source_path)
            print(f"  Original records: {len(df)}")
            
            # Filter for both target subject_id and hadm_id
            filtered_df = df[(df['subject_id'] == target_subject_id) & 
                           (df['hadm_id'] == target_hadm_id)]
            print(f"  Filtered records: {len(filtered_df)}")
            
            # Save filtered data
            filtered_df.to_csv(dest_path, index=False)
            total_records += len(filtered_df)
            
            if len(filtered_df) > 0:
                print(f"  ✓ Saved to {dest_path}")
            else:
                print(f"  ⚠ No records found for subject_id {target_subject_id} AND hadm_id {target_hadm_id}")
                
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {str(e)}")
    
    # Process files with only subject_id (single filtering)
    print(f"\n{'='*60}")
    print(f"SINGLE FILTERING (subject_id={target_subject_id} only)")
    print(f"{'='*60}")
    
    for filename in files_with_subject_only:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            print(f"\nProcessing {filename}...")
            
            # Read the CSV file
            df = pd.read_csv(source_path)
            print(f"  Original records: {len(df)}")
            
            # Filter for target subject_id only
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
    print(f"\n{'='*60}")
    print(f"COPYING REFERENCE FILES")
    print(f"{'='*60}")
    
    for filename in reference_files:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            shutil.copy2(source_path, dest_path)
            print(f"  ✓ Copied {filename}")
        except Exception as e:
            print(f"  ✗ Error copying {filename}: {str(e)}")
    
    # Generate summary report
    print(f"\n" + "="*70)
    print(f"SUMMARY REPORT")
    print(f"="*70)
    print(f"Target Subject ID: {target_subject_id}")
    print(f"Target Hospital Admission ID: {target_hadm_id}")
    print(f"Total filtered records: {total_records}")
    print(f"Files processed:")
    print(f"  - {len(files_with_both_ids)} files with dual filtering (subject_id + hadm_id)")
    print(f"  - {len(files_with_subject_only)} files with single filtering (subject_id only)")
    print(f"  - {len(reference_files)} reference files copied as-is")
    print(f"Output directory: {dest_folder}/")
    
    # List all output files with sizes
    output_files = list(Path(dest_folder).glob("*.csv"))
    print(f"\nGenerated files ({len(output_files)} total):")
    
    # Separate by category for better organization
    dual_filter_outputs = [f for f in output_files if f.name in files_with_both_ids]
    single_filter_outputs = [f for f in output_files if f.name in files_with_subject_only]
    reference_outputs = [f for f in output_files if f.name in reference_files]
    
    if dual_filter_outputs:
        print(f"\n  📊 Dual-filtered files (subject_id + hadm_id):")
        for file_path in sorted(dual_filter_outputs):
            file_size = file_path.stat().st_size
            size_str = format_file_size(file_size)
            print(f"    - {file_path.name} ({size_str})")
    
    if single_filter_outputs:
        print(f"\n  👤 Single-filtered files (subject_id only):")
        for file_path in sorted(single_filter_outputs):
            file_size = file_path.stat().st_size
            size_str = format_file_size(file_size)
            print(f"    - {file_path.name} ({size_str})")
    
    if reference_outputs:
        print(f"\n  📚 Reference files (copied as-is):")
        for file_path in sorted(reference_outputs):
            file_size = file_path.stat().st_size
            size_str = format_file_size(file_size)
            print(f"    - {file_path.name} ({size_str})")

def format_file_size(size_bytes):
    """Format file size in human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024*1024:
        return f"{size_bytes/1024:.1f} KB"
    else:
        return f"{size_bytes/(1024*1024):.1f} MB"

if __name__ == "__main__":
    create_one_patient_one_admission_data(
        target_subject_id=10035631, 
        target_hadm_id=29462354
    )
