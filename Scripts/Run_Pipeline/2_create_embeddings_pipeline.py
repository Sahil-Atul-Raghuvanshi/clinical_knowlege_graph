"""
Batch processing pipeline for large-scale patient embedding generation
Optimized for datasets with 100K+ patients (e.g., 364K patients)
"""
import logging
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import time
from tqdm import tqdm
import numpy as np

# Add Create_Embeddings directory to path for imports
# This file is at: Scripts/Run_Pipeline/2_create_embeddings_pipeline.py
# Need to access modules from: Scripts/Create_Embeddings/full_patient_embeddings/
embeddings_path = Path(__file__).parent.parent / 'Create_Embeddings' / 'full_patient_embeddings'
sys.path.insert(0, str(embeddings_path))

# Also add parent Create_Embeddings directory for neo4j_storage
create_embeddings_dir = Path(__file__).parent.parent / 'Create_Embeddings'
sys.path.insert(0, str(create_embeddings_dir))

# Add diagnosis_embeddings directory to path
diagnosis_embeddings_dir = create_embeddings_dir / 'diagnosis_embeddings'
sys.path.insert(0, str(diagnosis_embeddings_dir))

# Import ETL tracker for incremental loading
scripts_dir = Path(__file__).parent.parent  # Scripts directory
sys.path.insert(0, str(scripts_dir))
try:
    from utils.etl_tracker import ETLTracker
except ImportError:
    ETLTracker = None

# Import new embedding system modules
from embedding_pipeline import PatientEmbeddingPipeline
from utils.config import Config

# Import diagnosis embeddings module
try:
    from create_diagnosis_embeddings import create_diagnosis_embeddings
except ImportError as e:
    create_diagnosis_embeddings = None
    # Logger not yet initialized, will log warning later

# Setup logging
# Get project root and create logs directory
# This file is at: Scripts/Run_Pipeline/2_create_embeddings_pipeline.py
# Need to go up 2 levels to reach project root (Run_Pipeline -> Scripts -> Phase2)
project_root = Path(__file__).parent.parent.parent
logs_dir = project_root / 'logs'
logs_dir.mkdir(parents=True, exist_ok=True)

# Configure logging to save in logs directory
log_file = logs_dir / f'embedding_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Configure logging to file only (no console output)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Log file: {log_file}")

# Print to console only for critical messages (progress bar will show via tqdm)
print(f"Embedding pipeline started. Logs: {log_file}")
print("Processing patients and diagnoses... (see progress bar below)")

# Log ETL tracker availability
if ETLTracker is None:
    logger.warning("ETLTracker not found. Incremental loading will be disabled.")
else:
    logger.info("ETLTracker available. Incremental loading enabled.")

# Log diagnosis embeddings module availability
if create_diagnosis_embeddings is None:
    logger.warning("create_diagnosis_embeddings module not found. Diagnosis embeddings will be skipped.")
else:
    logger.info("Diagnosis embeddings module available. Will generate diagnosis embeddings after patient embeddings.")


class BatchProgress:
    """Track and persist batch processing progress"""
    
    def __init__(self, progress_file: str):
        self.progress_file = Path(progress_file)
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
    
    def _load(self) -> Dict:
        """Load progress from file"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'completed_batches': [],
            'failed_batches': [],
            'last_batch_index': -1,
            'total_processed': 0,
            'start_time': None,
            'last_update': None
        }
    
    def save(self):
        """Save progress to file"""
        self.data['last_update'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def mark_batch_completed(self, batch_index: int, patient_count: int):
        """Mark a batch as completed"""
        if batch_index not in self.data['completed_batches']:
            self.data['completed_batches'].append(batch_index)
        self.data['last_batch_index'] = batch_index
        self.data['total_processed'] += patient_count
        self.save()
    
    def mark_batch_failed(self, batch_index: int, error: str):
        """Mark a batch as failed"""
        self.data['failed_batches'].append({
            'batch_index': batch_index,
            'error': str(error),
            'timestamp': datetime.now().isoformat()
        })
        self.save()
    
    def is_batch_completed(self, batch_index: int) -> bool:
        """Check if batch is already completed"""
        return batch_index in self.data['completed_batches']
    
    def get_resume_index(self) -> int:
        """Get index to resume from"""
        return self.data['last_batch_index'] + 1
    
    def reset(self):
        """Reset progress"""
        self.data = {
            'completed_batches': [],
            'failed_batches': [],
            'last_batch_index': -1,
            'total_processed': 0,
            'start_time': datetime.now().isoformat(),
            'last_update': None
        }
        self.save()


class LargeScaleBatchPipeline:
    """Pipeline optimized for large-scale patient datasets (100K+) using hybrid storage"""
    
    def __init__(self, config: Config, tracker: Optional[ETLTracker] = None, tracker_file: Optional[str] = None):
        self.config = config
        self.pipeline = None
        self.progress = BatchProgress(config.progress_file)
        self.tracker = tracker
        self.tracker_file = tracker_file
        
        logger.info("Initializing Embedding Pipeline")
        logger.info(f"Batch size: {config.batch_processing.batch_size}")
        logger.info("Using Neo4j for patient and diagnosis embeddings")
        if tracker or tracker_file:
            logger.info("Incremental load mode: ENABLED (using ETL tracker)")
        else:
            logger.info("Incremental load mode: DISABLED (full load)")
    
    def setup(self):
        """Setup all components"""
        logger.info("Setting up patient embedding pipeline...")
        
        # Initialize the patient embedding pipeline with tracker support
        self.pipeline = PatientEmbeddingPipeline(
            self.config,
            tracker=self.tracker,
            tracker_file=self.tracker_file
        )
        self.pipeline.setup()
        
        logger.info("Pipeline setup complete [OK]")
    
    def get_unprocessed_patients(self) -> List[str]:
        """Get list of patients without embeddings"""
        logger.info("Querying for patients without embeddings...")
        
        if not self.pipeline:
            logger.error("Pipeline not initialized")
            return []
        
        query = """
        MATCH (p:Patient)
        WHERE p.textEmbedding IS NULL
        RETURN p.subject_id AS subject_id
        ORDER BY p.subject_id
        """
        
        result = self.pipeline.neo4j.execute_query(query)
        patient_ids = [str(r['subject_id']) for r in result]
        
        logger.info(f"Found {len(patient_ids)} patients without embeddings")
        return patient_ids
    
    def run_simple_pipeline(self, patient_ids: List[str]):
        """
        Run pipeline for specific patient IDs (for testing/small datasets)
        
        Args:
            patient_ids: List of specific patient IDs to process
        """
        logger.info("=" * 80)
        logger.info(f"RUNNING PIPELINE FOR {len(patient_ids)} SPECIFIC PATIENTS")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # Generate patient embeddings
            logger.info("\n[Step 1/2] Generating patient embeddings...")
            self.pipeline.generate_patient_embeddings(
                patient_ids=patient_ids,
                batch_size=self.config.batch_processing.batch_size,
                force=False
            )
            
            # Generate diagnosis embeddings
            if create_diagnosis_embeddings:
                logger.info("\n[Step 2/2] Generating diagnosis embeddings...")
                pipeline_log_file = str(log_file)
                create_diagnosis_embeddings(
                    tracker=self.tracker,
                    pipeline_log_file=pipeline_log_file,
                    patient_ids=patient_ids,
                    force=False
                )
            else:
                logger.warning("Diagnosis embeddings module not available. Skipping diagnosis embeddings generation.")
            
            elapsed = time.time() - start_time
            logger.info("\n" + "=" * 80)
            logger.info(f"[OK] PIPELINE COMPLETED")
            logger.info(f"  Processed: {len(patient_ids)} patients")
            logger.info(f"  Time: {elapsed:.2f}s")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Simple pipeline failed: {e}", exc_info=True)
            raise
    
    def run_full_batch_pipeline(self, reset_progress: bool = False, force_patients: bool = False):
        """
        Run complete batch pipeline for all unprocessed patients
        
        Args:
            reset_progress: If True, reset progress and start from beginning
            force_patients: If True, regenerate patient embeddings even if they exist
        """
        logger.info("\n" + "=" * 80)
        logger.info("STARTING PATIENT EMBEDDING PIPELINE")
        logger.info("=" * 80)
        
        total_start_time = time.time()
        
        if reset_progress:
            logger.info("Resetting progress...")
            self.progress.reset()
        
        try:
            # Generate patient embeddings
            logger.info("\n[Step 1/2] Generating patient embeddings...")
            patient_start_time = time.time()
            self.pipeline.generate_patient_embeddings(
                patient_ids=None,  # Process all patients
                batch_size=self.config.batch_processing.batch_size,
                force=force_patients
            )
            patient_elapsed = time.time() - patient_start_time
            logger.info(f"Patient embeddings completed in {patient_elapsed/3600:.2f} hours")
            
            # Generate diagnosis embeddings
            diagnosis_elapsed = 0
            if create_diagnosis_embeddings:
                logger.info("\n[Step 2/2] Generating diagnosis embeddings...")
                diagnosis_start_time = time.time()
                pipeline_log_file = str(log_file)
                create_diagnosis_embeddings(
                    tracker=self.tracker,
                    pipeline_log_file=pipeline_log_file,
                    patient_ids=None,  # Process all patients
                    force=force_patients
                )
                diagnosis_elapsed = time.time() - diagnosis_start_time
                logger.info(f"Diagnosis embeddings completed in {diagnosis_elapsed/3600:.2f} hours")
            else:
                logger.warning("Diagnosis embeddings module not available. Skipping diagnosis embeddings generation.")
            
            # Final summary
            total_elapsed = time.time() - total_start_time
            
            logger.info("\n" + "=" * 80)
            logger.info("EMBEDDING PIPELINE COMPLETED")
            logger.info("=" * 80)
            logger.info(f"Total time: {total_elapsed/3600:.2f} hours")
            logger.info(f"  - Patient embeddings: {patient_elapsed/3600:.2f} hours")
            if create_diagnosis_embeddings and diagnosis_elapsed > 0:
                logger.info(f"  - Diagnosis embeddings: {diagnosis_elapsed/3600:.2f} hours")
            logger.info("All embeddings stored in Neo4j")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def cleanup(self):
        """Cleanup resources"""
        if self.pipeline:
            self.pipeline.cleanup()
        logger.info("Pipeline cleanup complete")


def main():
    """Main entry point - always runs in batch mode"""
    # Configuration
    reset_progress = False
    force_patients = False
    config_path = None  # Set to path if using custom config file
    batch_size = None  # Set to override batch size from config
    
    # Log execution mode
    logger.info("=" * 80)
    logger.info("EXECUTION MODE: BATCH")
    logger.info("Running in BATCH mode - processing full dataset")
    logger.info("Pipeline includes: Patient embeddings + Diagnosis embeddings")
    logger.info("=" * 80)
    
    # Load config
    if config_path:
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        config = Config(**config_dict)
    else:
        config = Config()
    
    # Override batch size if specified
    if batch_size:
        config.batch_processing.batch_size = batch_size
        logger.info(f"Using batch size: {batch_size}")
    
    # Initialize pipeline
    # Initialize ETL tracker for incremental loading
    tracker_file = project_root / 'logs' / 'etl_tracker.csv'
    tracker = None
    if ETLTracker is not None:
        tracker = ETLTracker(str(tracker_file))
        logger.info(f"Initialized ETL tracker from: {tracker_file}")
    
    pipeline = LargeScaleBatchPipeline(config, tracker=tracker, tracker_file=str(tracker_file) if tracker else None)
    
    try:
        pipeline.setup()
        
        # Always run batch processing
        pipeline.run_full_batch_pipeline(
            reset_progress=reset_progress,
            force_patients=force_patients
        )
        
        print("\n✓ Pipeline completed successfully!")
        print(f"Check log file for details: {log_file}")
        
    except KeyboardInterrupt:
        logger.info("\nPipeline interrupted by user")
        logger.info("Progress has been saved. Run again to resume.")
        print("\n⚠ Pipeline interrupted by user. Progress saved.")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        print(f"\n✗ Pipeline failed. Check log file: {log_file}")
        sys.exit(1)
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()

