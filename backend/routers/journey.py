"""
Patient journey endpoint.
Retrieves complete chronological journey including child nodes for each event.
"""
import logging
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List, Optional

from services.neo4j_service import get_connection

router = APIRouter()
logger = logging.getLogger(__name__)


def _get_child_nodes(conn, element_id: str) -> Dict[str, List]:
    """Fetch child nodes for a given parent element_id."""
    if not element_id:
        return {}
    query = """
    MATCH (parent)-[r]->(child)
    WHERE elementId(parent) = $eid
      AND NOT child:DiagnosisItem AND NOT child:MedicationItem
      AND NOT child:LabResultItem AND NOT child:MicrobiologyResultItem
    RETURN labels(child) AS labels, properties(child) AS props, type(r) AS rel_type
    """
    try:
        from load_data.retrieve_patient_kg import extract_timestamp
        results = conn.execute_query(query, {"eid": element_id})
        children: Dict[str, List] = {}
        for record in results:
            labels = list(record.get("labels", []))
            props = dict(record.get("props", {}))
            rel_type = record.get("rel_type", "")
            if not labels:
                continue
            label = labels[0]
            # Only include children without their own timestamps (static info nodes)
            if extract_timestamp(props, labels) is None:
                if label not in children:
                    children[label] = []
                children[label].append({"props": props, "rel_type": rel_type})
        return children
    except Exception as e:
        logger.warning(f"Error fetching child nodes for {element_id}: {e}")
        return {}


@router.get("/{patient_id}")
def get_patient_journey(patient_id: str) -> Dict[str, Any]:
    if not patient_id.strip().isdigit():
        raise HTTPException(status_code=400, detail="Patient ID must be numeric")

    conn = get_connection()

    check = conn.execute_query(
        "MATCH (p:Patient) WHERE p.subject_id = $sid OR toString(p.subject_id) = $sid RETURN p.subject_id AS id",
        {"sid": str(patient_id)},
    )
    if not check:
        raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")

    from load_data.retrieve_patient_kg import retrieve_patient_kg, transform_kg_to_journey_format

    graph_data = retrieve_patient_kg(conn, patient_id.strip())
    journey_data = transform_kg_to_journey_format(graph_data)

    if not journey_data.get("events"):
        raise HTTPException(status_code=404, detail=f"No journey events found for patient {patient_id}")

    # Enrich each event with its child nodes and serialize timestamps
    enriched_events = []
    for event in journey_data.get("events", []):
        element_id = event.get("element_id", "")
        children = _get_child_nodes(conn, element_id)

        timestamp = event.get("timestamp")
        timestamp_str = timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp)

        enriched_events.append({
            "labels": event.get("labels", []),
            "properties": event.get("properties", {}),
            "element_id": element_id,
            "timestamp": timestamp_str,
            "children": children,
        })

    patient = journey_data.get("patient", {})

    return {
        "patient_id": patient_id,
        "patient": patient,
        "events": enriched_events,
    }
