import pandas as pd
import os
import json
from pathlib import Path
import shutil
from datetime import datetime

def create_one_patient_one_admission_one_visit_data(
    target_subject_id=10035631, 
    target_hadm_id=29462354,
    start_time="2112-09-17 19:50:00",
    end_time="2112-10-04 08:13:45"
):
    """
    Filter data for a specific subject_id, hadm_id, and time range from Batch_Data folder
    and save filtered results to One_Patient_One_Admission_One_Visit folder.
    """
    source_folder = "Batch_Data"
    dest_folder = "One_Patient_One_Admission_One_Visit"
    
    # Parse the time range
    start_dt = pd.to_datetime(start_time)
    end_dt = pd.to_datetime(end_time)
    
    # Create destination folder if it doesn't exist
    Path(dest_folder).mkdir(exist_ok=True)
    print(f"Created/verified destination folder: {dest_folder}")
    
    # Load schema to understand which files have which columns
    with open("Scripts/schema.json", "r") as f:
        schemas = json.load(f)
    
    # Map files to their datetime columns
    file_datetime_mapping = {}
    files_with_both_ids = []
    files_with_subject_only = []
    reference_files = []
    
    for schema in schemas:
        filename = schema["file_name"]
        columns = schema["columns"]
        
        # Identify datetime columns
        datetime_columns = [col for col, dtype in columns.items() if dtype == "datetime"]
        file_datetime_mapping[filename] = datetime_columns
        
        # Categorize files
        if "subject_id" in columns and "hadm_id" in columns:
            files_with_both_ids.append(filename)
        elif "subject_id" in columns:
            files_with_subject_only.append(filename)
        else:
            reference_files.append(filename)
    
    print(f"\nTime Range Filter: {start_time} to {end_time}")
    print(f"Subject ID: {target_subject_id}")
    print(f"Hospital Admission ID: {target_hadm_id}")
    
    print(f"\nFiles with both subject_id and hadm_id: {files_with_both_ids}")
    print(f"Files with subject_id only: {files_with_subject_only}")
    print(f"Reference files: {reference_files}")
    
    print(f"\nDatetime columns by file:")
    for filename, dt_cols in file_datetime_mapping.items():
        if dt_cols:
            print(f"  {filename}: {dt_cols}")
    
    total_records = 0
    
    # Process files with both subject_id and hadm_id (dual filtering + temporal)
    print(f"\n{'='*80}")
    print(f"DUAL + TEMPORAL FILTERING")
    print(f"(subject_id={target_subject_id} AND hadm_id={target_hadm_id} AND time range)")
    print(f"{'='*80}")
    
    for filename in files_with_both_ids:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            print(f"\nProcessing {filename}...")
            
            # Read the CSV file
            df = pd.read_csv(source_path)
            print(f"  Original records: {len(df)}")
            
            # Filter for subject_id and hadm_id first
            filtered_df = df[(df['subject_id'] == target_subject_id) & 
                           (df['hadm_id'] == target_hadm_id)]
            print(f"  After ID filtering: {len(filtered_df)}")
            
            # Apply temporal filtering based on specific file requirements
            datetime_cols = file_datetime_mapping[filename]
            if datetime_cols and len(filtered_df) > 0:
                print(f"  Applying temporal filter on columns: {datetime_cols}")
                
                # Make a copy to avoid SettingWithCopyWarning
                filtered_df = filtered_df.copy()
                
                # Convert datetime columns to datetime type and detect date-only fields
                for col in datetime_cols:
                    if col in filtered_df.columns:
                        filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                
                def is_date_only_column(series):
                    """Check if a datetime column contains only date information (no time)"""
                    if series.isna().all():
                        return False
                    # Check if all non-null values have time component as 00:00:00
                    non_null_series = series.dropna()
                    if len(non_null_series) == 0:
                        return False
                    return all(dt.time() == pd.Timestamp('00:00:00').time() for dt in non_null_series)
                
                # Apply specific temporal filtering based on file type
                if filename == "prescriptions.csv":
                    # For prescriptions, check starttime within range (use inclusive boundaries)
                    if 'starttime' in filtered_df.columns:
                        condition = ((filtered_df['starttime'] >= start_dt) & (filtered_df['starttime'] <= end_dt))
                        temp_filtered = filtered_df[condition]
                        print(f"  After starttime temporal filtering: {len(temp_filtered)}")
                        
                        if len(temp_filtered) > 0:
                            filtered_df = temp_filtered
                        else:
                            print(f"  ⚠ No prescriptions with starttime in range, keeping empty dataset")
                            filtered_df = temp_filtered  # Keep empty dataset instead of all records
                            
                elif filename == "procedures_icd.csv":
                    # For procedures, chartdate must be on start_date, end_date, or between them
                    if 'chartdate' in filtered_df.columns:
                        if is_date_only_column(filtered_df['chartdate']):
                            # Date-only field: use inclusive boundaries (>= and <=)
                            start_date = start_dt.date()
                            end_date = end_dt.date()
                            condition = ((filtered_df['chartdate'].dt.date >= start_date) & 
                                       (filtered_df['chartdate'].dt.date <= end_date))
                        else:
                            # DateTime field: use inclusive boundaries (>= and <=)
                            condition = ((filtered_df['chartdate'] >= start_dt) & (filtered_df['chartdate'] <= end_dt))
                        temp_filtered = filtered_df[condition]
                        print(f"  After chartdate temporal filtering: {len(temp_filtered)}")
                        
                        if len(temp_filtered) > 0:
                            filtered_df = temp_filtered
                        else:
                            print(f"  ⚠ No procedures with chartdate in range, keeping empty dataset")
                            filtered_df = temp_filtered  # Keep empty dataset instead of all records
                            
                elif filename == "transfers.csv":
                    # For transfers, both intime and outtime should be within range (AND condition)
                    temporal_conditions = []
                    for col in ['intime', 'outtime']:
                        if col in filtered_df.columns:
                            # Use inclusive boundaries for transfers (>= and <=)
                            # Handle null values by treating them as always satisfying the condition
                            condition = (filtered_df[col].isna()) | ((filtered_df[col] >= start_dt) & (filtered_df[col] <= end_dt))
                            temporal_conditions.append(condition)
                    
                    if temporal_conditions:
                        # Use AND condition - both times must be in range (or null)
                        combined_condition = temporal_conditions[0]
                        for condition in temporal_conditions[1:]:
                            combined_condition = combined_condition & condition  # AND instead of OR
                        
                        temp_filtered = filtered_df[combined_condition]
                        print(f"  After temporal filtering: {len(temp_filtered)}")
                        
                        if len(temp_filtered) > 0:
                            filtered_df = temp_filtered
                        else:
                            print(f"  ⚠ No transfers with both times in range, keeping empty dataset")
                            filtered_df = temp_filtered  # Keep empty dataset instead of all records
                            
                else:
                    # For other files, use OR condition on all datetime columns with appropriate boundary logic
                    temporal_conditions = []
                    for col in datetime_cols:
                        if col in filtered_df.columns:
                            if is_date_only_column(filtered_df[col]):
                                # Date-only field: use inclusive boundaries (>= and <=)
                                start_date = start_dt.date()
                                end_date = end_dt.date()
                                condition = ((filtered_df[col].dt.date >= start_date) & 
                                           (filtered_df[col].dt.date <= end_date))
                            else:
                                # DateTime field: use inclusive boundaries (>= and <=)
                                condition = ((filtered_df[col] >= start_dt) & (filtered_df[col] <= end_dt))
                            temporal_conditions.append(condition)
                    
                    if temporal_conditions:
                        combined_condition = temporal_conditions[0]
                        for condition in temporal_conditions[1:]:
                            combined_condition = combined_condition | condition
                        
                        temp_filtered = filtered_df[combined_condition]
                        print(f"  After temporal filtering: {len(temp_filtered)}")
                        
                        if len(temp_filtered) > 0:
                            filtered_df = temp_filtered
                        else:
                            print(f"  ⚠ No records in time range, keeping empty dataset")
                            filtered_df = temp_filtered  # Keep empty dataset instead of all records
            
            # Save filtered data
            filtered_df.to_csv(dest_path, index=False)
            total_records += len(filtered_df)
            
            if len(filtered_df) > 0:
                print(f"  ✓ Saved {len(filtered_df)} records to {dest_path}")
            else:
                print(f"  ⚠ No records found")
                
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {str(e)}")
    
    # Process files with only subject_id (single filtering + temporal)
    print(f"\n{'='*80}")
    print(f"SINGLE + TEMPORAL FILTERING")
    print(f"(subject_id={target_subject_id} AND time range)")
    print(f"{'='*80}")
    
    for filename in files_with_subject_only:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            print(f"\nProcessing {filename}...")
            
            # Read the CSV file
            df = pd.read_csv(source_path)
            print(f"  Original records: {len(df)}")
            
            # Filter for subject_id first
            filtered_df = df[df['subject_id'] == target_subject_id]
            print(f"  After ID filtering: {len(filtered_df)}")
            
            # Apply temporal filtering if datetime columns exist
            datetime_cols = file_datetime_mapping[filename]
            if datetime_cols and len(filtered_df) > 0:
                print(f"  Applying temporal filter on columns: {datetime_cols}")
                
                # Make a copy to avoid SettingWithCopyWarning
                filtered_df = filtered_df.copy()
                
                # Convert datetime columns to datetime type
                for col in datetime_cols:
                    if col in filtered_df.columns:
                        filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                
                def is_date_only_column(series):
                    """Check if a datetime column contains only date information (no time)"""
                    if series.isna().all():
                        return False
                    # Check if all non-null values have time component as 00:00:00
                    non_null_series = series.dropna()
                    if len(non_null_series) == 0:
                        return False
                    return all(dt.time() == pd.Timestamp('00:00:00').time() for dt in non_null_series)
                
                # Create temporal filter conditions with appropriate boundary logic
                temporal_conditions = []
                for col in datetime_cols:
                    if col in filtered_df.columns:
                        if is_date_only_column(filtered_df[col]):
                            # Date-only field: use inclusive boundaries (>= and <=)
                            start_date = start_dt.date()
                            end_date = end_dt.date()
                            condition = ((filtered_df[col].dt.date >= start_date) & 
                                       (filtered_df[col].dt.date <= end_date))
                        else:
                            # DateTime field: use inclusive boundaries (>= and <=)
                            condition = ((filtered_df[col] >= start_dt) & (filtered_df[col] <= end_dt))
                        temporal_conditions.append(condition)
                
                # Apply temporal filter
                if temporal_conditions:
                    combined_condition = temporal_conditions[0]
                    for condition in temporal_conditions[1:]:
                        combined_condition = combined_condition | condition
                    
                    temp_filtered = filtered_df[combined_condition]
                    print(f"  After temporal filtering: {len(temp_filtered)}")
                    
                    if len(temp_filtered) > 0:
                        filtered_df = temp_filtered
                    else:
                        print(f"  ⚠ No records in time range, keeping empty dataset")
                        filtered_df = temp_filtered  # Keep empty dataset instead of all records
            
            # Save filtered data
            filtered_df.to_csv(dest_path, index=False)
            total_records += len(filtered_df)
            
            if len(filtered_df) > 0:
                print(f"  ✓ Saved {len(filtered_df)} records to {dest_path}")
            else:
                print(f"  ⚠ No records found")
                
        except Exception as e:
            print(f"  ✗ Error processing {filename}: {str(e)}")
    
    # Copy reference files (no filtering needed)
    print(f"\n{'='*80}")
    print(f"COPYING REFERENCE FILES")
    print(f"{'='*80}")
    
    for filename in reference_files:
        source_path = os.path.join(source_folder, filename)
        dest_path = os.path.join(dest_folder, filename)
        
        try:
            shutil.copy2(source_path, dest_path)
            print(f"  ✓ Copied {filename}")
        except Exception as e:
            print(f"  ✗ Error copying {filename}: {str(e)}")
    
    # Generate summary report
    print(f"\n" + "="*80)
    print(f"SUMMARY REPORT")
    print(f"="*80)
    print(f"Target Subject ID: {target_subject_id}")
    print(f"Target Hospital Admission ID: {target_hadm_id}")
    print(f"Time Range: {start_time} to {end_time}")
    print(f"Duration: {end_dt - start_dt}")
    print(f"Total filtered records: {total_records}")
    print(f"Files processed:")
    print(f"  - {len(files_with_both_ids)} files with dual + temporal filtering")
    print(f"  - {len(files_with_subject_only)} files with single + temporal filtering")
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
        print(f"\n  ⏰ Dual + temporal filtered files:")
        for file_path in sorted(dual_filter_outputs):
            file_size = file_path.stat().st_size
            size_str = format_file_size(file_size)
            datetime_cols = file_datetime_mapping[file_path.name]
            dt_info = f" (datetime cols: {datetime_cols})" if datetime_cols else ""
            print(f"    - {file_path.name} ({size_str}){dt_info}")
    
    if single_filter_outputs:
        print(f"\n  👤 Single + temporal filtered files:")
        for file_path in sorted(single_filter_outputs):
            file_size = file_path.stat().st_size
            size_str = format_file_size(file_size)
            datetime_cols = file_datetime_mapping[file_path.name]
            dt_info = f" (datetime cols: {datetime_cols})" if datetime_cols else ""
            print(f"    - {file_path.name} ({size_str}){dt_info}")
    
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
    create_one_patient_one_admission_one_visit_data(
        target_subject_id=10035631, 
        target_hadm_id=29462354,
        start_time="2112-09-17 19:50:00",
        end_time="2112-10-04 08:13:45"
    )
