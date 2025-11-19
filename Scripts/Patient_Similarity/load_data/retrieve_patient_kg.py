"""
Patient Knowledge Graph Retrieval Module
Handles extraction of patient subgraph from Neo4j database
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from utils.neo4j_connection import Neo4jConnection

logger = logging.getLogger(__name__)


def extract_timestamp(node_props: dict, labels: list) -> Optional[datetime]:
    """
    Extract the primary timestamp from a node based on its label
    
    Args:
        node_props: Node properties dictionary
        labels: List of node labels
        
    Returns:
        Datetime object if timestamp found, None otherwise
    """
    if not labels or not node_props:
        return None
    
    label = labels[0] if labels else "Unknown"
    
    # Map labels to timestamp fields (matching create_patient_journey_pdf.py)
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
        'InitialAssessment': ['charttime'] if 'charttime' in node_props else []
    }
    
    fields = timestamp_fields.get(label, [])
    
    for field in fields:
        if field in node_props and node_props[field]:
            try:
                timestamp_str = node_props[field]
                if isinstance(timestamp_str, str):
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            except:
                continue
    
    return None


def retrieve_patient_kg(connection: Neo4jConnection, subject_id: str) -> Dict[str, Any]:
    """
    Retrieve complete graph structure with nodes, relationships, and attributes using APOC.
    Returns temporally ordered graph data for comprehensive summarization.
    
    Args:
        connection: Neo4j connection object
        subject_id: Patient ID to extract graph for
        
    Returns:
        Dictionary containing patient graph structure with keys:
        - patient_id: str
        - patient: dict (labels and properties)
        - nodes: dict (all nodes in the subgraph)
        - relationships: list (all relationships in the subgraph)
        - temporal_events: list (chronologically ordered events with timestamps)
    """
    logger.info(f"Extracting graph structure for patient {subject_id} using APOC...")
    
    # Use APOC to get complete subgraph starting from Patient node
    query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    CALL apoc.path.subgraphAll(p, {
        relationshipFilter: ">",
        minLevel: 0,
        maxLevel: -1,
        labelFilter: "-DiagnosisItem|-MedicationItem|-LabResultItem|-MicrobiologyResultItem"
    })
    YIELD nodes, relationships
    RETURN nodes, relationships
    """
    
    try:
        results = connection.execute_query(query, {"subject_id": str(subject_id)})
    except Exception as e:
        # Fallback to original query if APOC is not available
        print(f"APOC not available or query failed: {e}. Falling back to original query.")
        logger.warning(f"APOC not available or query failed: {e}. Falling back to original query.")
        query_fallback = """
        MATCH (p:Patient)
        WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
        WITH p
        OPTIONAL MATCH path = (p)-[r*1..10]->(n)
        WHERE NOT n:DiagnosisItem 
          AND NOT n:MedicationItem 
          AND NOT n:LabResultItem 
          AND NOT n:MicrobiologyResultItem
        WITH p, relationships(path) as rels, nodes(path) as nodeList
        WHERE rels IS NOT NULL AND size(rels) > 0
        UNWIND range(0, size(rels)-1) as idx
        WITH p, rels[idx] as rel, nodeList[idx] as startNode, nodeList[idx+1] as endNode
        WHERE NOT startNode:DiagnosisItem 
          AND NOT startNode:MedicationItem 
          AND NOT startNode:LabResultItem 
          AND NOT startNode:MicrobiologyResultItem
          AND NOT endNode:DiagnosisItem 
          AND NOT endNode:MedicationItem 
          AND NOT endNode:LabResultItem 
          AND NOT endNode:MicrobiologyResultItem
        RETURN DISTINCT
            labels(startNode) as start_labels,
            properties(startNode) as start_props,
            type(rel) as relationship_type,
            labels(endNode) as end_labels,
            properties(endNode) as end_props
        """
        results = connection.execute_query(query_fallback, {"subject_id": str(subject_id)})
        
        # Process fallback results (original logic)
        graph_data = {
            "patient_id": str(subject_id),
            "nodes": {},
            "relationships": [],
            "temporal_events": []
        }
        
        # Get patient node separately
        patient_query = """
        MATCH (p:Patient)
        WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
        RETURN labels(p) as labels, properties(p) as props
        """
        
        patient_results = connection.execute_query(patient_query, {"subject_id": str(subject_id)})
        
        if patient_results:
            patient_record = patient_results[0]
            graph_data["patient"] = {
                "labels": list(patient_record['labels']) if patient_record.get('labels') else [],
                "properties": dict(patient_record['props']) if patient_record.get('props') else {}
            }
        
        # Process relationships and nodes
        for record in results:
            start_labels = list(record['start_labels']) if record.get('start_labels') else []
            start_props = dict(record['start_props']) if record.get('start_props') else {}
            rel_type = record.get('relationship_type')
            end_labels = list(record['end_labels']) if record.get('end_labels') else []
            end_props = dict(record['end_props']) if record.get('end_props') else {}
            
            # Store nodes
            if start_labels and start_props:
                node_key = f"{start_labels[0]}_{start_props.get('event_id', start_props.get('name', 'unknown'))}"
                if node_key not in graph_data["nodes"]:
                    graph_data["nodes"][node_key] = {
                        "labels": start_labels,
                        "properties": start_props
                    }
                    
                    # Add to temporal events if it has a timestamp
                    timestamp = extract_timestamp(start_props, start_labels)
                    if timestamp:
                        graph_data["temporal_events"].append({
                            "node_key": node_key,
                            "labels": start_labels,
                            "properties": start_props,
                            "timestamp": timestamp.isoformat(),
                            "timestamp_obj": timestamp
                        })
            
            if end_labels and end_props:
                node_key = f"{end_labels[0]}_{end_props.get('event_id', end_props.get('name', 'unknown'))}"
                if node_key not in graph_data["nodes"]:
                    graph_data["nodes"][node_key] = {
                        "labels": end_labels,
                        "properties": end_props
                    }
                    
                    # Add to temporal events if it has a timestamp
                    timestamp = extract_timestamp(end_props, end_labels)
                    if timestamp:
                        graph_data["temporal_events"].append({
                            "node_key": node_key,
                            "labels": end_labels,
                            "properties": end_props,
                            "timestamp": timestamp.isoformat(),
                            "timestamp_obj": timestamp
                        })
            
            # Store relationship
            if rel_type and start_labels and end_labels:
                graph_data["relationships"].append({
                    "from": {
                        "label": start_labels[0] if start_labels else "Unknown",
                        "name": start_props.get('name', start_props.get('event_id', 'unknown'))
                    },
                    "relationship": rel_type,
                    "to": {
                        "label": end_labels[0] if end_labels else "Unknown",
                        "name": end_props.get('name', end_props.get('event_id', 'unknown')),
                        "properties": end_props
                    }
                })
        
        # Sort temporal events by timestamp
        graph_data["temporal_events"].sort(key=lambda x: x["timestamp_obj"])
        # Remove timestamp_obj before returning (not JSON serializable)
        for event in graph_data["temporal_events"]:
            event.pop("timestamp_obj", None)
        
        logger.info(f"Extracted {len(graph_data['nodes'])} nodes, {len(graph_data['relationships'])} relationships, and {len(graph_data['temporal_events'])} temporal events")
        
        if len(graph_data['relationships']) == 0:
            logger.warning(f"Patient {subject_id} has no relationships in the graph")
        
        return graph_data
    
    # Process APOC results
    if not results:
        logger.warning(f"No results returned for patient {subject_id}")
        return {
            "patient_id": str(subject_id),
            "nodes": {},
            "relationships": [],
            "temporal_events": []
        }
    
    # Build structured graph data
    graph_data = {
        "patient_id": str(subject_id),
        "nodes": {},
        "relationships": [],
        "temporal_events": []
    }
    
    # Extract nodes and relationships from APOC result
    all_nodes = {}
    all_relationships = []
    
    for record in results:
        nodes = record.get('nodes', [])
        relationships = record.get('relationships', [])
        
        # Process nodes
        for node in nodes:
            if not hasattr(node, 'labels') or not hasattr(node, '_properties'):
                continue
                
            labels = list(node.labels) if node.labels else []
            props = dict(node._properties) if hasattr(node, '_properties') else {}
            
            # Skip item nodes
            if any(label in ['DiagnosisItem', 'MedicationItem', 'LabResultItem', 'MicrobiologyResultItem'] for label in labels):
                continue
            
            # Create unique node key
            node_id = props.get('event_id') or props.get('hadm_id') or props.get('stay_id') or props.get('name', 'unknown')
            node_key = f"{labels[0] if labels else 'Unknown'}_{node_id}"
            
            if node_key not in all_nodes:
                all_nodes[node_key] = {
                    "labels": labels,
                    "properties": props
                }
                
                # Add to temporal events if it has a timestamp
                timestamp = extract_timestamp(props, labels)
                if timestamp:
                    graph_data["temporal_events"].append({
                        "node_key": node_key,
                        "labels": labels,
                        "properties": props,
                        "timestamp": timestamp.isoformat(),
                        "timestamp_obj": timestamp
                    })
        
        # Process relationships
        for rel in relationships:
            if not hasattr(rel, 'start_node') or not hasattr(rel, 'end_node') or not hasattr(rel, 'type'):
                continue
            
            start_node = rel.start_node
            end_node = rel.end_node
            rel_type = rel.type
            
            start_labels = list(start_node.labels) if start_node.labels else []
            start_props = dict(start_node._properties) if hasattr(start_node, '_properties') else {}
            end_labels = list(end_node.labels) if end_node.labels else []
            end_props = dict(end_node._properties) if hasattr(end_node, '_properties') else {}
            
            # Skip if either node is an item node
            if any(label in ['DiagnosisItem', 'MedicationItem', 'LabResultItem', 'MicrobiologyResultItem'] for label in start_labels + end_labels):
                continue
            
            all_relationships.append({
                "from": {
                    "label": start_labels[0] if start_labels else "Unknown",
                    "name": start_props.get('name', start_props.get('event_id', 'unknown'))
                },
                "relationship": rel_type,
                "to": {
                    "label": end_labels[0] if end_labels else "Unknown",
                    "name": end_props.get('name', end_props.get('event_id', 'unknown')),
                    "properties": end_props
                }
            })
    
    # Get patient node separately
    patient_query = """
    MATCH (p:Patient)
    WHERE p.subject_id = $subject_id OR toString(p.subject_id) = $subject_id
    RETURN labels(p) as labels, properties(p) as props
    """
    
    patient_results = connection.execute_query(patient_query, {"subject_id": str(subject_id)})
    
    if patient_results:
        patient_record = patient_results[0]
        graph_data["patient"] = {
            "labels": list(patient_record['labels']) if patient_record.get('labels') else [],
            "properties": dict(patient_record['props']) if patient_record.get('props') else {}
        }
    
    graph_data["nodes"] = all_nodes
    graph_data["relationships"] = all_relationships
    
    # Sort temporal events by timestamp
    graph_data["temporal_events"].sort(key=lambda x: x["timestamp_obj"])
    # Remove timestamp_obj before returning (not JSON serializable)
    for event in graph_data["temporal_events"]:
        event.pop("timestamp_obj", None)
    
    logger.info(f"Extracted {len(graph_data['nodes'])} nodes, {len(graph_data['relationships'])} relationships, and {len(graph_data['temporal_events'])} temporal events")
    
    if len(graph_data['relationships']) == 0:
        logger.warning(f"Patient {subject_id} has no relationships in the graph")
    
    return graph_data

