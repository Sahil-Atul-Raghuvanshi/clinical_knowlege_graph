# 53_check_timestamps_and_order.py
import logging
from neo4j import GraphDatabase
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Neo4j configuration
URI = "neo4j://127.0.0.1:7687"
AUTH = ("neo4j", "admin123")
DATABASE = "10016742"

def get_folder_name():
    """Read folder name from foldername.txt"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        foldername_path = os.path.join(script_dir, 'foldername.txt')
        with open(foldername_path, 'r') as f:
            folder_name = f.read().strip()
        logger.info(f"Using folder name: {folder_name}")
        return folder_name
    except Exception as e:
        logger.warning(f"Could not read folder name: {e}")
        return "default"

def extract_timestamp(node):
    """Extract the primary timestamp from a node based on its label"""
    props = node._properties
    label = list(node.labels)[0] if node.labels else "Unknown"
    
    # Define timestamp field priority by node type
    timestamp_fields = {
        'HospitalAdmission': ['admittime', 'dischtime'],
        'EmergencyDepartment': ['intime', 'outtime'],
        'UnitAdmission': ['intime', 'outtime'],
        'ICUStay': ['intime', 'outtime'],
        'Transfer': ['intime', 'outtime'],
        'Discharge': ['outtime', 'intime'],
        'Prescription': ['starttime'],
        'Procedures': ['time'],
        'LabEvent': ['charttime'],
        'MicrobiologyEvent': ['charttime'],
        'ChartEvent': ['charttime'],
        'PreviousPrescriptionMeds': ['charttime'],
        'AdministeredMeds': ['charttime'],
        'InitialAssessment': ['charttime'] if 'charttime' in props else []
    }
    
    # Get the fields to check for this label
    fields = timestamp_fields.get(label, [])
    
    # Try each field in order
    for field in fields:
        if field in props and props[field]:
            try:
                # Parse the timestamp
                timestamp_str = props[field]
                if isinstance(timestamp_str, str):
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except:
                continue
    
    return None

def format_node_output(node, timestamp):
    """Format a node for output"""
    props = node._properties
    node_id = node.element_id
    label = list(node.labels)[0] if node.labels else "Unknown"
    
    output = []
    output.append(f"\n{label}")
    output.append("_" * 100)
    output.append(f"<id>: {node_id}")
    
    # Add all properties
    for key, value in sorted(props.items()):
        if isinstance(value, list):
            output.append(f"{key}: {value}")
        else:
            output.append(f'{key}: "{value}"')
    
    if timestamp:
        output.append(f"TIMESTAMP: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        output.append("TIMESTAMP: None (No timestamp)")
    
    output.append("_" * 100)
    
    return "\n".join(output)

def get_child_nodes(session, parent_node):
    """Get child nodes that don't have timestamps"""
    node_id = parent_node.element_id
    
    # Query to get all related nodes
    query = """
    MATCH (parent)-[r]->(child)
    WHERE elementId(parent) = $node_id
    RETURN child, type(r) as relationship_type
    """
    
    results = session.run(query, node_id=node_id)
    children = []
    
    for record in results:
        child = record['child']
        rel_type = record['relationship_type']
        
        # Check if child has no timestamp
        if extract_timestamp(child) is None:
            children.append((child, rel_type))
    
    return children

def generate_timeline():
    """Generate a chronologically ordered timeline of all nodes, organized by patient"""
    driver = GraphDatabase.driver(URI, auth=AUTH, database=DATABASE)
    folder_name = get_folder_name()
    
    output_filename = f"timeline_ordered_nodes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        with driver.session() as session:
            logger.info("Fetching all patients...")
            
            # First, get all patients
            patient_query = """
            MATCH (p:Patient)
            RETURN p.subject_id as subject_id, p
            ORDER BY p.subject_id
            """
            
            patient_results = session.run(patient_query)
            patients = [(record['subject_id'], record['p']) for record in patient_results]
            
            logger.info(f"Found {len(patients)} patients")
            
            # For each patient, get all their related nodes
            patient_timelines = {}
            
            for subject_id, patient_node in patients:
                logger.info(f"Processing patient {subject_id}...")
                
                # Query to get all nodes related to this patient
                nodes_query = """
                MATCH (p:Patient {subject_id: $subject_id})
                OPTIONAL MATCH (p)-[*]->(n)
                WHERE n.name IS NOT NULL
                RETURN DISTINCT n
                """
                
                results = session.run(nodes_query, subject_id=subject_id)
                
                # Collect nodes with timestamps for this patient
                nodes_with_timestamps = []
                nodes_without_timestamps = []
                
                for record in results:
                    node = record['n']
                    if node is None:
                        continue
                        
                    timestamp = extract_timestamp(node)
                    
                    if timestamp:
                        nodes_with_timestamps.append((node, timestamp))
                    else:
                        # Check if this node has a parent with timestamp
                        label = list(node.labels)[0] if node.labels else "Unknown"
                        # These are typically child nodes that should appear with their parents
                        if label in ['Diagnosis', 'DRG', 'PatientPastHistory', 'HPISummary', 
                                    'DischargeClinicalNote', 'AdmissionVitals', 'AdmissionLabs', 
                                    'AdmissionMedications', 'DischargeVitals', 'DischargeLabs',
                                    'DischargeMedications', 'MedicationStarted', 'MedicationStopped',
                                    'MedicationToAvoid', 'PrescriptionsBatch', 'ProceduresBatch',
                                    'LabEvents', 'MicrobiologyEvents', 'ChartEventBatch']:
                            nodes_without_timestamps.append(node)
                
                # Sort by timestamp
                nodes_with_timestamps.sort(key=lambda x: x[1])
                
                patient_timelines[subject_id] = {
                    'patient_node': patient_node,
                    'nodes_with_timestamps': nodes_with_timestamps,
                    'nodes_without_timestamps': nodes_without_timestamps
                }
                
                logger.info(f"Patient {subject_id}: {len(nodes_with_timestamps)} nodes with timestamps")
            
            # Write to file
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write("=" * 100 + "\n")
                f.write("CHRONOLOGICAL TIMELINE OF CLINICAL KNOWLEDGE GRAPH NODES (PATIENT-WISE)\n")
                f.write("=" * 100 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Database: {DATABASE}\n")
                f.write(f"Total patients: {len(patients)}\n")
                f.write("=" * 100 + "\n\n")
                
                # Write each patient's timeline
                for subject_id, timeline_data in patient_timelines.items():
                    patient_node = timeline_data['patient_node']
                    nodes_with_timestamps = timeline_data['nodes_with_timestamps']
                    
                    # Write patient header
                    f.write("\n" + "=" * 100 + "\n")
                    f.write(f"PATIENT: {subject_id}\n")
                    f.write("=" * 100 + "\n")
                    
                    # Write patient information
                    patient_props = patient_node._properties
                    f.write("Patient Information:\n")
                    for key, value in sorted(patient_props.items()):
                        if isinstance(value, list):
                            f.write(f"  {key}: {value}\n")
                        else:
                            f.write(f'  {key}: "{value}"\n')
                    f.write("-" * 100 + "\n")
                    f.write(f"Total events: {len(nodes_with_timestamps)}\n")
                    f.write("=" * 100 + "\n\n")
                    
                    # Write each node with timestamp for this patient
                    for node, timestamp in nodes_with_timestamps:
                        # Write the main node
                        f.write(format_node_output(node, timestamp))
                        f.write("\n")
                        
                        # Get and write child nodes without timestamps
                        children = get_child_nodes(session, node)
                        if children:
                            f.write(f"\n{'  '* 2}--- Related Nodes (No Timestamp) ---\n")
                            for child, rel_type in children:
                                child_label = list(child.labels)[0] if child.labels else "Unknown"
                                f.write(f"\n{'  ' * 2}[{rel_type}] -> {child_label}\n")
                                
                                # Format child node properties indented
                                props = child._properties
                                for key, value in sorted(props.items()):
                                    if isinstance(value, list):
                                        f.write(f"{'  ' * 3}{key}: {value}\n")
                                    else:
                                        f.write(f"{'  ' * 3}{key}: \"{value}\"\n")
                                f.write(f"{'  ' * 2}" + "-" * 80 + "\n")
                    
                    f.write("\n" + "=" * 100 + "\n")
                    f.write(f"END OF PATIENT {subject_id} TIMELINE\n")
                    f.write("=" * 100 + "\n\n")
                
                f.write("\n" + "=" * 100 + "\n")
                f.write("END OF ALL PATIENT TIMELINES\n")
                f.write("=" * 100 + "\n")
            
            logger.info(f"Timeline written to: {output_filename}")
            
    except Exception as e:
        logger.error(f"Error generating timeline: {e}")
        raise
    finally:
        driver.close()

if __name__ == "__main__":
    logger.info("Starting timeline generation...")
    generate_timeline()
    logger.info("Timeline generation complete!")

