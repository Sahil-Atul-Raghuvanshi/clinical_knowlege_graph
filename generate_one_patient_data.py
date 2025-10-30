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
    subject_ids = [10016742]  # Add more subject_ids as needed, e.g., [10016742, 10002495, 10003456]
    
    print(f"\n{'='*80}")
    print(f"FILTERING DATA FOR SUBJECT IDs: {subject_ids}")
    print(f"Source: Complete_Data/")
    print(f"Destination: Filtered_Data/")
    print(f"{'='*80}\n")
    
    # Create directory structure
    create_directory_structure()
    
    # Define the mapping of source directories to target directories
    directories = {
        "Complete_Data\\hosp": "Filtered_Data\\hosp",
        "Complete_Data\\icu": "Filtered_Data\\icu",
        "Complete_Data\\ed": "Filtered_Data\\ed",
        "Complete_Data\\note": "Filtered_Data\\note",
    }
    
    total_files_processed = 0
    total_files_successful = 0
    
    for source_dir, target_dir in directories.items():
        print(f"\nProcessing directory: {source_dir}")
        print(f"-" * 80)
        
        # Check if source directory exists
        if not os.path.exists(source_dir):
            print(f"Source directory {source_dir} does not exist, skipping...")
            continue
            
        # Get all CSV files in the source directory
        csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"No CSV files found in {source_dir}")
            continue
            
        print(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            input_path = os.path.join(source_dir, csv_file)
            output_path = os.path.join(target_dir, csv_file)
            
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
    print(f"Data saved to Filtered_Data/ (filtered by subject_ids or copied entirely if no subject_id column)")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
