# full_load.py - Execute all knowledge graph loading scripts in order
# Supports both FULL LOAD (first run) and INCREMENTAL LOAD (subsequent runs)
import logging
import sys
import os
import time
from datetime import datetime
import importlib.util

# Get the project root directory (parent of Scripts directory)
script_dir = os.path.dirname(os.path.abspath(__file__))
scripts_root = os.path.dirname(script_dir)
project_root = os.path.dirname(scripts_root)
kg_scripts_dir = os.path.join(scripts_root, 'Generate_Clinical_Knowledge_Graphs')
process_clinical_notes_dir = os.path.join(kg_scripts_dir, 'process_clinical_notes')
create_nodes_dir = os.path.join(kg_scripts_dir, 'create_nodes_and_relationships')

# Import ETL tracker from utils
sys.path.insert(0, scripts_root)  # Add Scripts directory to path
from utils.etl_tracker import ETLTracker

# Create logs directory if it doesn't exist
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
    print(f"Created logs directory: {logs_dir}")

# Configure logging with UTF-8 encoding
log_filename = os.path.join(logs_dir, f'full_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
file_handler = logging.FileHandler(log_filename, encoding='utf-8', mode='a')  # Append mode
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.stream.reconfigure(encoding='utf-8') if hasattr(stream_handler.stream, 'reconfigure') else None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger(__name__)

# Set environment variable for script 14 (which runs at module import time)
# Other scripts will receive log file path as parameter
os.environ['FULL_LOAD_LOG_FILE'] = log_filename

def import_module_from_file(file_path, module_name):
    """Dynamically import a module from a file path"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def get_patients_to_process(tracker):
    """
    Get list of patients that should be processed.
    Returns patients that have been successfully processed by script 1 (patient nodes).
    """
    try:
        # Get all patients successfully processed by script 1
        patients = tracker.get_processed_patients('1_add_patient_nodes')
        if patients:
            return patients
        
        # Fallback: try to get from data files if tracker is empty
        try:
            import pandas as pd
            patients_csv = os.path.join(project_root, 'Filtered_Data', 'core', 'patients.csv')
            if os.path.exists(patients_csv):
                df = pd.read_csv(patients_csv)
                if 'subject_id' in df.columns:
                    return set(df['subject_id'].astype(int).unique())
        except Exception:
            pass
        
        return set()
    except Exception as e:
        logger.warning(f"Could not determine patients to process: {e}")
        return set()

def mark_script_failure(tracker, script_name, patients_to_process):
    """
    Mark all relevant patients as failed for a script.
    Only marks patients that haven't been successfully processed by this script.
    """
    if not patients_to_process:
        logger.warning(f"No patients found to mark as failed for {script_name}")
        return
    
    failed_count = 0
    for subject_id in patients_to_process:
        # Only mark as failed if patient hasn't been successfully processed by this script
        if not tracker.is_patient_processed(subject_id, script_name):
            try:
                tracker.mark_patient_processed(subject_id, script_name, status='failed')
                failed_count += 1
            except Exception as e:
                logger.error(f"Error marking patient {subject_id} as failed: {e}")
    
    if failed_count > 0:
        logger.info(f"Marked {failed_count} patients as failed for script '{script_name}' in tracker")

def run_script(script_name, function_callable, step_number, total_steps, tracker=None):
    """Execute a single script and handle errors"""
    try:
        logger.info("=" * 80)
        logger.info(f"STEP {step_number}/{total_steps}: Running {script_name}")
        logger.info("=" * 80)
        start_time = time.time()
        
        function_callable()
        
        elapsed_time = time.time() - start_time
        logger.info(f"✓ STEP {step_number}/{total_steps} COMPLETED: {script_name} (took {elapsed_time:.2f} seconds)")
        logger.info("")
        return True
    except Exception as e:
        logger.error(f"✗ STEP {step_number}/{total_steps} FAILED: {script_name}")
        logger.error(f"Error: {str(e)}")
        logger.exception("Full traceback:")
        
        # Mark failure in tracker for data loading scripts (skip clinical note processing scripts)
        if tracker and script_name.startswith(('1_', '2_', '3_', '4_', '5_', '6_', '7_', '8_', '9_', '10_', '11_', '12_', '13_', '14_', '15_', '16_', '17_', '18_')):
            try:
                # Extract script name without .py extension for tracker
                script_name_for_tracker = script_name.replace('.py', '')
                patients_to_process = get_patients_to_process(tracker)
                mark_script_failure(tracker, script_name_for_tracker, patients_to_process)
            except Exception as tracker_error:
                logger.error(f"Error marking failures in tracker: {tracker_error}")
        
        return False

def main():
    """Execute all knowledge graph loading scripts in sequence"""
    # Initialize ETL tracker
    tracker_file = os.path.join(project_root, 'logs', 'etl_tracker.csv')
    tracker = ETLTracker(tracker_file)
    
    # Determine if this is a full load or incremental load
    processed_patients = tracker.get_all_processed_patients()
    is_full_load = len(processed_patients) == 0
    
    logger.info("=" * 80)
    if is_full_load:
        logger.info("STARTING FULL KNOWLEDGE GRAPH LOAD (First Run)")
    else:
        logger.info(f"STARTING INCREMENTAL KNOWLEDGE GRAPH LOAD")
        logger.info(f"Found {len(processed_patients)} already processed patients in tracker")
    logger.info("=" * 80)
    logger.info(f"Log file: {log_filename}")
    logger.info(f"Tracker file: {tracker_file}")
    logger.info("")
    
    overall_start_time = time.time()
    
    # Import all required modules dynamically
    try:
        logger.info("Loading script modules...")
        # Clinical note processing scripts (must run first) - in process_clinical_notes folder
        script48 = import_module_from_file(os.path.join(process_clinical_notes_dir, "convert_text_clinical_note_to_json_using_logic.py"), "convert_text_clinical_note_to_json_using_logic")
        script49 = import_module_from_file(os.path.join(process_clinical_notes_dir, "49_clinical_notes_flatenning.py"), "clinical_notes_flatenning")
        
        # Data loading scripts - in create_nodes_and_relationships folder
        script1 = import_module_from_file(os.path.join(create_nodes_dir, "1_add_patient_nodes.py"), "add_patient_nodes")
        script2 = import_module_from_file(os.path.join(create_nodes_dir, "2_patient_flow_through_the_hospital.py"), "patient_flow_through_the_hospital")
        script3 = import_module_from_file(os.path.join(create_nodes_dir, "3_add_icu_stays_label.py"), "add_icu_stays_label")
        script4 = import_module_from_file(os.path.join(create_nodes_dir, "4_add_prescription_nodes.py"), "add_prescription_nodes")
        script5 = import_module_from_file(os.path.join(create_nodes_dir, "5_add_procedure_nodes.py"), "add_procedure_nodes")
        script6 = import_module_from_file(os.path.join(create_nodes_dir, "6_add_diagnosis_nodes.py"), "add_diagnosis_nodes")
        script7 = import_module_from_file(os.path.join(create_nodes_dir, "7_add_labevent_nodes.py"), "add_labevent_nodes")
        script8 = import_module_from_file(os.path.join(create_nodes_dir, "8_add_drg_codes.py"), "add_drg_codes")
        script9 = import_module_from_file(os.path.join(create_nodes_dir, "9_add_micro_biology_events.py"), "add_micro_biology_events")
        script10 = import_module_from_file(os.path.join(create_nodes_dir, "10_add_provider_nodes.py"), "add_provider_nodes")
        script11 = import_module_from_file(os.path.join(create_nodes_dir, "11_add_assessment_nodes.py"), "add_assessment_nodes")
        script12 = import_module_from_file(os.path.join(create_nodes_dir, "12_add_past_history.py"), "add_past_history")
        script13 = import_module_from_file(os.path.join(create_nodes_dir, "13_update_chief_complaints.py"), "update_chief_complaints")
        script15 = import_module_from_file(os.path.join(create_nodes_dir, "15_add_discharge_clinical_note.py"), "add_discharge_clinical_note")
        script16 = import_module_from_file(os.path.join(create_nodes_dir, "16_add_allergies_identified_node.py"), "add_allergies_identified_node")
        script17 = import_module_from_file(os.path.join(create_nodes_dir, "17_add_hpi_summary_node.py"), "add_hpi_summary_node")
        script18 = import_module_from_file(os.path.join(create_nodes_dir, "18_add_hospitalization_data.py"), "add_hospitalization_data")
        # Note: script14 (cleanup) is imported later when needed, as it runs at module import time
        logger.info("All script modules loaded successfully!")
    except Exception as e:
        logger.error(f"Failed to load script modules: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)
    
    # Define the execution pipeline with tracker and log file support
    # Scripts will receive tracker and pipeline_log_file as parameters
    pipeline = [
        # Clinical note processing (must run first)
        ("convert_text_clinical_note_to_json_using_logic.py", lambda: script48.main()),
        ("49_clinical_notes_flatenning.py", lambda: script49.main()),
        
        # Data loading scripts (with tracker and log file support where applicable)
        ("1_add_patient_nodes.py", lambda: script1.add_patient_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("2_patient_flow_through_the_hospital.py", lambda: script2.create_patient_flow(tracker=tracker, pipeline_log_file=log_filename)),
        ("3_add_icu_stays_label.py", lambda: script3.add_icu_stays_label(tracker=tracker, pipeline_log_file=log_filename)),
        ("4_add_prescription_nodes.py", lambda: script4.create_prescription_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("5_add_procedure_nodes.py", lambda: script5.create_procedure_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("6_add_diagnosis_nodes.py", lambda: script6.create_diagnosis_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("7_add_labevent_nodes.py", lambda: script7.create_labevent_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("8_add_drg_codes.py", lambda: script8.add_drg_codes(tracker=tracker, pipeline_log_file=log_filename)),
        ("9_add_micro_biology_events.py", lambda: script9.create_microbiology_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("10_add_provider_nodes.py", lambda: script10.add_provider_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("11_add_assessment_nodes.py", lambda: script11.create_initial_assessment_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("12_add_past_history.py", lambda: script12.add_past_history_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("13_update_chief_complaints.py", lambda: script13.update_chief_complaints(tracker=tracker, pipeline_log_file=log_filename)),
        ("15_add_discharge_clinical_note.py", lambda: script15.add_discharge_clinical_note_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("16_add_allergies_identified_node.py", lambda: script16.add_allergy_identified_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("17_add_hpi_summary_node.py", lambda: script17.add_hpi_summary_nodes(tracker=tracker, pipeline_log_file=log_filename)),
        ("18_add_hospitalization_data.py", lambda: script18.add_hospitalization_data(tracker=tracker, pipeline_log_file=log_filename)),
        ("14_delete_unwanted_connections_1.py", lambda: import_module_from_file(os.path.join(create_nodes_dir, "14_delete_unwanted_connections_1.py"), "delete_unwanted_connections")),  # Runs at module import time, uses FULL_LOAD_LOG_FILE env var
    ]
    
    total_steps = len(pipeline)  # Cleanup script is now in pipeline
    successful_steps = 0
    failed_steps = 0
    
    # Execute each script in sequence
    for step_number, (script_name, function) in enumerate(pipeline, start=1):
        success = run_script(script_name, function, step_number, total_steps, tracker=tracker)
        if success:
            successful_steps += 1
        else:
            failed_steps += 1
            logger.error(f"Script {script_name} failed. Stopping execution.")
            break
    
    # Print final summary
    total_elapsed_time = time.time() - overall_start_time
    logger.info("")
    logger.info("=" * 80)
    logger.info("FULL LOAD SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total steps: {total_steps}")
    logger.info(f"Successful: {successful_steps}")
    logger.info(f"Failed: {failed_steps}")
    logger.info(f"Total time: {total_elapsed_time:.2f} seconds ({total_elapsed_time/60:.2f} minutes)")
    
    if failed_steps == 0:
        logger.info("✓ ALL STEPS COMPLETED SUCCESSFULLY!")
        if is_full_load:
            logger.info("Knowledge graph has been fully loaded.")
        else:
            logger.info("Knowledge graph has been incrementally updated.")
        
        # Print tracker summary
        summary = tracker.get_processing_summary()
        if summary:
            logger.info("")
            logger.info("ETL Tracker Summary:")
            for script_name, count in summary.items():
                logger.info(f"  {script_name}: {count} patients processed")
    else:
        logger.error("✗ SOME STEPS FAILED!")
        logger.error("Please check the log for details and fix errors before retrying.")
        sys.exit(1)
    
    logger.info("=" * 80)

if __name__ == "__main__":
    main()

