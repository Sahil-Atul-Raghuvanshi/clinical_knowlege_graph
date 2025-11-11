# full_load.py - Execute all knowledge graph loading scripts in order
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

# Create logs directory if it doesn't exist
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
    print(f"Created logs directory: {logs_dir}")

# Configure logging with UTF-8 encoding
log_filename = os.path.join(logs_dir, f'full_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.stream.reconfigure(encoding='utf-8') if hasattr(stream_handler.stream, 'reconfigure') else None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger(__name__)

def import_module_from_file(file_path, module_name):
    """Dynamically import a module from a file path"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def run_script(script_name, function_callable, step_number, total_steps):
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
        return False

def main():
    """Execute all knowledge graph loading scripts in sequence"""
    logger.info("=" * 80)
    logger.info("STARTING FULL KNOWLEDGE GRAPH LOAD")
    logger.info("=" * 80)
    logger.info(f"Log file: {log_filename}")
    logger.info("")
    
    overall_start_time = time.time()
    
    # Import all required modules dynamically
    try:
        logger.info("Loading script modules...")
        # Clinical note processing scripts (must run first)
        script48 = import_module_from_file(os.path.join(kg_scripts_dir, "48_convert_text_clinical_node_to_json.py"), "convert_text_clinical_node_to_json")
        script49 = import_module_from_file(os.path.join(kg_scripts_dir, "49_clinical_notes_flatenning.py"), "clinical_notes_flatenning")
        
        # Data loading scripts
        script1 = import_module_from_file(os.path.join(kg_scripts_dir, "1_add_patient_nodes.py"), "add_patient_nodes")
        script2 = import_module_from_file(os.path.join(kg_scripts_dir, "2_patient_flow_through_the_hospital.py"), "patient_flow_through_the_hospital")
        script3 = import_module_from_file(os.path.join(kg_scripts_dir, "3_add_icu_stays_label.py"), "add_icu_stays_label")
        script4 = import_module_from_file(os.path.join(kg_scripts_dir, "4_add_prescription_nodes.py"), "add_prescription_nodes")
        script5 = import_module_from_file(os.path.join(kg_scripts_dir, "5_add_procedure_nodes.py"), "add_procedure_nodes")
        script6 = import_module_from_file(os.path.join(kg_scripts_dir, "6_add_diagnosis_nodes.py"), "add_diagnosis_nodes")
        script7 = import_module_from_file(os.path.join(kg_scripts_dir, "7_add_labevent_nodes.py"), "add_labevent_nodes")
        script8 = import_module_from_file(os.path.join(kg_scripts_dir, "8_add_drg_codes.py"), "add_drg_codes")
        script9 = import_module_from_file(os.path.join(kg_scripts_dir, "9_add_micro_biology_events.py"), "add_micro_biology_events")
        script10 = import_module_from_file(os.path.join(kg_scripts_dir, "10_add_provider_nodes.py"), "add_provider_nodes")
        script11 = import_module_from_file(os.path.join(kg_scripts_dir, "11_add_assessment_nodes.py"), "add_assessment_nodes")
        script12 = import_module_from_file(os.path.join(kg_scripts_dir, "12_add_past_history.py"), "add_past_history")
        script13 = import_module_from_file(os.path.join(kg_scripts_dir, "13_update_chief_complaints.py"), "update_chief_complaints")
        script15 = import_module_from_file(os.path.join(kg_scripts_dir, "15_add_discharge_clinical_note.py"), "add_discharge_clinical_note")
        script16 = import_module_from_file(os.path.join(kg_scripts_dir, "16_add_allergies_identified_node.py"), "add_allergies_identified_node")
        script17 = import_module_from_file(os.path.join(kg_scripts_dir, "17_add_hpi_summary_node.py"), "add_hpi_summary_node")
        script18 = import_module_from_file(os.path.join(kg_scripts_dir, "18_add_hospitalization_data.py"), "add_hospitalization_data")
        logger.info("All script modules loaded successfully!")
    except Exception as e:
        logger.error(f"Failed to load script modules: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)
    
    # Define the execution pipeline
    pipeline = [
        # Clinical note processing (must run first)
        ("48_convert_text_clinical_node_to_json.py", lambda: script48.main()),
        ("49_clinical_notes_flatenning.py", lambda: script49.main()),
        
        # Data loading scripts
        ("1_add_patient_nodes.py", lambda: script1.add_patient_nodes()),
        ("2_patient_flow_through_the_hospital.py", lambda: script2.create_patient_flow()),
        ("3_add_icu_stays_label.py", lambda: script3.add_icu_stays_label()),
        ("4_add_prescription_nodes.py", lambda: script4.create_prescription_nodes()),
        ("5_add_procedure_nodes.py", lambda: script5.create_procedure_nodes()),
        ("6_add_diagnosis_nodes.py", lambda: script6.create_diagnosis_nodes()),
        ("7_add_labevent_nodes.py", lambda: script7.create_labevent_nodes()),
        ("8_add_drg_codes.py", lambda: script8.add_drg_codes()),
        ("9_add_micro_biology_events.py", lambda: script9.create_microbiology_nodes()),
        ("10_add_provider_nodes.py", lambda: script10.add_provider_nodes()),
        ("11_add_assessment_nodes.py", lambda: script11.create_initial_assessment_nodes()),
        ("12_add_past_history.py", lambda: script12.add_past_history_nodes()),
        ("13_update_chief_complaints.py", lambda: script13.update_chief_complaints()),
        ("15_add_discharge_clinical_note.py", lambda: script15.add_discharge_clinical_note_nodes()),
        ("16_add_allergies_identified_node.py", lambda: script16.add_allergy_identified_nodes()),
        ("17_add_hpi_summary_node.py", lambda: script17.add_hpi_summary_nodes()),
        ("18_add_hospitalization_data.py", lambda: script18.add_hospitalization_data()),
    ]
    
    total_steps = len(pipeline) + 1  # +1 for cleanup script
    successful_steps = 0
    failed_steps = 0
    
    # Execute each script in sequence
    for step_number, (script_name, function) in enumerate(pipeline, start=1):
        success = run_script(script_name, function, step_number, total_steps)
        if success:
            successful_steps += 1
        else:
            failed_steps += 1
            logger.error(f"Script {script_name} failed. Stopping execution.")
            break
    
    # Run cleanup script if all previous scripts succeeded
    if failed_steps == 0:
        logger.info("=" * 80)
        logger.info(f"STEP {total_steps}/{total_steps}: Running 14_delete_unwanted_connections_1.py")
        logger.info("=" * 80)
        start_time = time.time()
        
        try:
            from neo4j import GraphDatabase
            
            URI = "neo4j://127.0.0.1:7687"
            AUTH = ("neo4j", "admin123")
            DATABASE = "clinicalknowledgegraph"
            
            driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
            
            with driver.session() as session:
                logger.info("Removing cross-connections between Prescription/Procedure/LabEvents hierarchies...")
                
                # Delete connections FROM PrescriptionsBatch/Prescription TO Procedures
                query1 = """
                MATCH (p)-[r:INCLUDED_PROCEDURES]->(proc)
                WHERE (p:PrescriptionsBatch OR p:Prescription)
                  AND (proc:Procedures OR proc:ProceduresBatch)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result1 = session.run(query1)
                count1 = result1.single()["deleted_count"]
                logger.info(f"Deleted {count1} INCLUDED_PROCEDURES from Prescription hierarchy to Procedures")
                
                # Delete connections FROM PrescriptionsBatch/Prescription TO LabEvents
                query2 = """
                MATCH (p)-[r:INCLUDED_LAB_EVENTS]->(lab)
                WHERE (p:PrescriptionsBatch OR p:Prescription)
                  AND (lab:LabEvents OR lab:LabEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result2 = session.run(query2)
                count2 = result2.single()["deleted_count"]
                logger.info(f"Deleted {count2} INCLUDED_LAB_EVENTS from Prescription hierarchy to LabEvents")
                
                # Delete connections FROM Procedures TO PrescriptionsBatch/Prescription
                query3 = """
                MATCH (proc)-[r:ISSUED_PRESCRIPTIONS]->(p)
                WHERE (proc:Procedures OR proc:ProceduresBatch)
                  AND (p:PrescriptionsBatch OR p:Prescription)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result3 = session.run(query3)
                count3 = result3.single()["deleted_count"]
                logger.info(f"Deleted {count3} ISSUED_PRESCRIPTIONS from Procedures to Prescription hierarchy")
                
                # Delete connections FROM LabEvents TO PrescriptionsBatch/Prescription
                query4 = """
                MATCH (lab)-[r:ISSUED_PRESCRIPTIONS]->(p)
                WHERE (lab:LabEvents OR lab:LabEvent)
                  AND (p:PrescriptionsBatch OR p:Prescription)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result4 = session.run(query4)
                count4 = result4.single()["deleted_count"]
                logger.info(f"Deleted {count4} ISSUED_PRESCRIPTIONS from LabEvents to Prescription hierarchy")
                
                # Delete connections FROM Procedures TO LabEvents
                query5 = """
                MATCH (proc)-[r:INCLUDED_LAB_EVENTS]->(lab)
                WHERE (proc:Procedures OR proc:ProceduresBatch)
                  AND (lab:LabEvents OR lab:LabEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result5 = session.run(query5)
                count5 = result5.single()["deleted_count"]
                logger.info(f"Deleted {count5} INCLUDED_LAB_EVENTS from Procedures to LabEvents")
                
                # Delete connections FROM LabEvents TO Procedures
                query6 = """
                MATCH (lab)-[r:INCLUDED_PROCEDURES]->(proc)
                WHERE (lab:LabEvents OR lab:LabEvent)
                  AND (proc:Procedures OR proc:ProceduresBatch)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result6 = session.run(query6)
                count6 = result6.single()["deleted_count"]
                logger.info(f"Deleted {count6} INCLUDED_PROCEDURES from LabEvents to Procedures")
                
                # Delete ANY connections FROM Procedures TO LabEvents (bidirectional)
                query7 = """
                MATCH (p:Procedures)-[r]-(lab)
                WHERE (lab:LabEvents OR lab:LabEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result7 = session.run(query7)
                count7 = result7.single()["deleted_count"]
                logger.info(f"Deleted {count7} connections between Procedures and LabEvents")
                
                # Delete ANY connections FROM Procedures TO Prescriptions (bidirectional)
                query8 = """
                MATCH (proc:Procedures)-[r]-(presc)
                WHERE (presc:Prescription OR presc:PrescriptionsBatch)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result8 = session.run(query8)
                count8 = result8.single()["deleted_count"]
                logger.info(f"Deleted {count8} connections between Procedures and Prescriptions")
                
                # Delete ANY connections between LabEvent and Prescription (bidirectional)
                query9 = """
                MATCH (lab:LabEvent)-[r]-(presc:Prescription)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result9 = session.run(query9)
                count9 = result9.single()["deleted_count"]
                logger.info(f"Deleted {count9} connections between LabEvent and Prescription nodes")
                
                # Delete connections FROM Prescriptions TO MicrobiologyEvents
                query10 = """
                MATCH (presc)-[r:INCLUDED_MICROBIOLOGY_EVENTS]->(micro)
                WHERE (presc:Prescription OR presc:PrescriptionsBatch)
                  AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result10 = session.run(query10)
                count10 = result10.single()["deleted_count"]
                logger.info(f"Deleted {count10} INCLUDED_MICROBIOLOGY_EVENTS from Prescriptions to MicrobiologyEvents")
                
                # Delete connections FROM Procedures TO MicrobiologyEvents
                query11 = """
                MATCH (proc)-[r:INCLUDED_MICROBIOLOGY_EVENTS]->(micro)
                WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
                  AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result11 = session.run(query11)
                count11 = result11.single()["deleted_count"]
                logger.info(f"Deleted {count11} INCLUDED_MICROBIOLOGY_EVENTS from Procedures to MicrobiologyEvents")
                
                # Delete ANY connections between Prescriptions and MicrobiologyEvents (bidirectional)
                query12 = """
                MATCH (presc)-[r]-(micro)
                WHERE (presc:Prescription OR presc:PrescriptionsBatch)
                  AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result12 = session.run(query12)
                count12 = result12.single()["deleted_count"]
                logger.info(f"Deleted {count12} connections between Prescriptions and MicrobiologyEvents")
                
                # Delete ANY connections between Procedures and MicrobiologyEvents (bidirectional)
                query13 = """
                MATCH (proc)-[r]-(micro)
                WHERE (proc:Procedures OR proc:ProceduresBatch OR proc:Procedure)
                  AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result13 = session.run(query13)
                count13 = result13.single()["deleted_count"]
                logger.info(f"Deleted {count13} connections between Procedures and MicrobiologyEvents")
                
                # Delete old MicrobiologyEvents batch nodes and INCLUDED_MICROBIOLOGY_EVENTS relationships
                # But keep CONTAINED_MICROBIOLOGY_EVENT from LabEvents to MicrobiologyEvent
                query14a = """
                MATCH ()-[r:INCLUDED_MICROBIOLOGY_EVENTS]->()
                DELETE r
                RETURN count(r) as deleted_count
                """
                result14a = session.run(query14a)
                count14a = result14a.single()["deleted_count"]
                logger.info(f"Deleted {count14a} old INCLUDED_MICROBIOLOGY_EVENTS relationships")
                
                query14b = """
                MATCH (me:MicrobiologyEvents)
                DETACH DELETE me
                RETURN count(me) as deleted_count
                """
                result14b = session.run(query14b)
                count14b = result14b.single()["deleted_count"]
                logger.info(f"Deleted {count14b} old MicrobiologyEvents batch nodes")
                
                # Delete unwanted connections between LabEvents and MicrobiologyEvents
                # but preserve CONTAINED_MICROBIOLOGY_EVENT from LabEvents to MicrobiologyEvent
                query14c = """
                MATCH (lab)-[r]-(micro)
                WHERE (lab:LabEvents OR lab:LabEvent)
                  AND (micro:MicrobiologyEvents OR micro:MicrobiologyEvent)
                  AND NOT (lab:LabEvents AND type(r) = 'CONTAINED_MICROBIOLOGY_EVENT' AND micro:MicrobiologyEvent)
                DELETE r
                RETURN count(r) as deleted_count
                """
                result14c = session.run(query14c)
                count14c = result14c.single()["deleted_count"]
                logger.info(f"Deleted {count14c} unwanted connections between LabEvents and MicrobiologyEvents")
                
                count14 = count14a + count14b + count14c
                
                # Delete RECORDED_CHART_EVENTS from non-ICUStay nodes to ChartEventBatch
                query15 = """
                MATCH (n)-[r:RECORDED_CHART_EVENTS]->(ceb:ChartEventBatch)
                WHERE NOT n:ICUStay
                DELETE r
                RETURN count(r) as deleted_count
                """
                result15 = session.run(query15)
                count15 = result15.single()["deleted_count"]
                logger.info(f"Deleted {count15} RECORDED_CHART_EVENTS from non-ICUStay nodes")
                
                # Delete ANY other relationships to/from ChartEventBatch except RECORDED_CHART_EVENTS from ICUStay and CONTAINED_CHART_EVENT to ChartEvent
                query16 = """
                MATCH (n)-[r]-(ceb:ChartEventBatch)
                WHERE NOT (
                    (n:ICUStay AND type(r) = 'RECORDED_CHART_EVENTS') OR
                    (n:ChartEvent AND type(r) = 'CONTAINED_CHART_EVENT')
                )
                DELETE r
                RETURN count(r) as deleted_count
                """
                result16 = session.run(query16)
                count16 = result16.single()["deleted_count"]
                logger.info(f"Deleted {count16} unwanted relationships to/from ChartEventBatch")
                
                total = count1 + count2 + count3 + count4 + count5 + count6 + count7 + count8 + count9 + count10 + count11 + count12 + count13 + count14 + count15 + count16
                logger.info(f"\nTotal cross-connections deleted: {total}")
            
            driver.close()
            
            elapsed_time = time.time() - start_time
            logger.info(f"✓ STEP {total_steps}/{total_steps} COMPLETED: 14_delete_unwanted_connections_1.py (took {elapsed_time:.2f} seconds)")
            successful_steps += 1
            
        except Exception as e:
            logger.error(f"✗ STEP {total_steps}/{total_steps} FAILED: 14_delete_unwanted_connections_1.py")
            logger.error(f"Error: {str(e)}")
            logger.exception("Full traceback:")
            failed_steps += 1
    
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
        logger.info("Knowledge graph has been fully loaded.")
    else:
        logger.error("✗ SOME STEPS FAILED!")
        logger.error("Please check the log for details and fix errors before retrying.")
        sys.exit(1)
    
    logger.info("=" * 80)

if __name__ == "__main__":
    main()

