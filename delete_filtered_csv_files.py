import os
import glob
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def delete_filtered_csv_files():
    """Delete all CSV files in the Filtered_Data subfolders (hosp, icu, ed, note)"""
    
    # Base path to Filtered_Data folder
    base_path = r"C:\Users\Coditas\Desktop\Projects\CKG\Phase1\Filtered_Data"
    
    # Subfolders to process
    subfolders = ["hosp", "icu", "ed", "note"]
    
    total_deleted = 0
    
    for subfolder in subfolders:
        subfolder_path = os.path.join(base_path, subfolder)
        
        # Check if subfolder exists
        if not os.path.exists(subfolder_path):
            logger.warning(f"Subfolder does not exist: {subfolder_path}")
            continue
        
        # Find all CSV files in the subfolder
        csv_pattern = os.path.join(subfolder_path, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            logger.info(f"No CSV files found in {subfolder}/")
            continue
        
        logger.info(f"\nProcessing {subfolder}/ - Found {len(csv_files)} CSV file(s)")
        
        # Delete each CSV file
        for csv_file in csv_files:
            try:
                os.remove(csv_file)
                filename = os.path.basename(csv_file)
                logger.info(f"  ✓ Deleted: {filename}")
                total_deleted += 1
            except Exception as e:
                logger.error(f"  ✗ Failed to delete {os.path.basename(csv_file)}: {e}")
    
    logger.info(f"\n{'='*80}")
    logger.info(f"SUMMARY: Total CSV files deleted: {total_deleted}")
    logger.info(f"{'='*80}")

if __name__ == "__main__":
    logger.info("Starting deletion of CSV files from Filtered_Data subfolders...")
    logger.info("="*80)
    
    # Confirm deletion
    response = input("\nThis will DELETE all CSV files in Filtered_Data/hosp, icu, ed, and note folders.\nAre you sure? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        delete_filtered_csv_files()
        logger.info("\n✓ Deletion completed!")
    else:
        logger.info("\n✗ Deletion cancelled by user.")

