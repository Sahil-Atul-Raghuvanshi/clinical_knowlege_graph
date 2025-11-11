"""
Configuration settings for Hybrid Embedding System
Supports both Neo4j (node-level) and Milvus (item-level) embeddings
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Neo4jConfig:
    """Neo4j database configuration"""
    uri: str = "neo4j://127.0.0.1:7687"
    username: str = "neo4j"
    password: str = "admin123"
    database: str = "clinicalknowledgegraph"
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            uri=os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687"),
            username=os.getenv("NEO4J_USERNAME", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "admin123"),
            database=os.getenv("NEO4J_DATABASE", "clinicalknowledgegraph")
        )


@dataclass
class MilvusConfig:
    """Milvus vector database configuration"""
    host: str = "localhost"
    port: int = 19530
    alias: str = "default"
    dimension: int = 384  # all-MiniLM-L6-v2 dimension
    index_type: str = "HNSW"  # HNSW, IVF_FLAT, etc.
    metric_type: str = "COSINE"  # COSINE, L2, IP
    
    # Collection names for different item types
    prescription_collection: str = "prescription_items"
    microbiology_collection: str = "microbiology_items"
    lab_result_collection: str = "lab_result_items"
    diagnosis_collection: str = "diagnosis_items"
    
    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv("MILVUS_HOST", "localhost"),
            port=int(os.getenv("MILVUS_PORT", "19530")),
            alias=os.getenv("MILVUS_ALIAS", "default")
        )


@dataclass
class EmbeddingConfig:
    """Embedding generation configuration"""
    # Structural embedding settings
    structural_dimension: int = 128
    fastrp_dimension: int = 128
    fastrp_iteration_weights: List[float] = field(default_factory=lambda: [0.0, 1.0])
    fastrp_normalization_strength: float = 0.0
    
    # Text embedding settings
    text_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    text_dimension: int = 384
    
    # Combined embedding settings
    combine_method: str = "concatenate"
    structural_weight: float = 0.5
    textual_weight: float = 0.5
    
    @property
    def combined_dimension(self):
        """Calculate combined embedding dimension"""
        if self.combine_method == "concatenate":
            return self.structural_dimension + self.text_dimension
        return self.structural_dimension


@dataclass
class GraphConfig:
    """Graph projection configuration"""
    graph_name: str = "patient_journey_graph"
    max_depth: int = 4
    
    node_labels: List[str] = field(default_factory=lambda: [
        'Patient', 'HospitalAdmission', 'EmergencyDepartment', 
        'UnitAdmission', 'Discharge', 'ICUStay',
        'Diagnosis', 'Prescription', 'PrescriptionsBatch',
        'PreviousPrescriptionMeds', 'AdministeredMeds',
        'Procedures', 'ProceduresBatch', 
        'LabEvents', 'LabEvent', 'MicrobiologyEvent',
        'DischargeClinicalNote', 'HPISummary', 'PatientPastHistory',
        'InitialAssessment', 'AllergyIdentified',
        'AdmissionMedications', 'DischargeMedications',
        'MedicationStarted', 'MedicationStopped', 'MedicationToAvoid',
        'AdmissionLabs', 'DischargeLabs',
        'AdmissionVitals', 'DischargeVitals',
        'DRG', 'Provider', 'ChartEventBatch', 'ChartEvent'
    ])
    
    relationship_types: List[str] = field(default_factory=lambda: [
        # Patient flow relationships (updated to reflect new patient flow logic)
        'VISITED_ED',  # Patient -> EmergencyDepartment
        'ADMITTED_TO_UNIT',  # Patient -> UnitAdmission (for direct unit admissions)
        'LED_TO_UNIT_ADMISSION',  # Event -> UnitAdmission (for transfers/admits)
        'LED_TO_DISCHARGE',  # Event -> Discharge
        'LED_TO_ED',  # Event -> EmergencyDepartment (between events)
        'LED_TO_ED_VISIT',  # Discharge -> EmergencyDepartment (for subsequent ED visits)
        'LED_TO_FIRST_UNIT_ADMISSION',  # HospitalAdmission -> UnitAdmission (first unit)
        'LED_TO_ADMISSION',  # EmergencyDepartment -> HospitalAdmission (general)
        'LED_TO_ADMISSION_DURING_STAY',  # EmergencyDepartment -> HospitalAdmission (during ED stay)
        'LED_TO_ADMISSION_AFTER_DISCHARGE',  # EmergencyDepartment -> HospitalAdmission (after ED discharge)
        # Clinical data relationships
        'RECORDED_DIAGNOSES', 'ISSUED_PRESCRIPTIONS', 'CONTAINED_PRESCRIPTION',
        'RECORDED_PREVIOUS_MEDICATIONS', 'ADMINISTERED_MEDICATIONS',
        'CONTAINED_PROCEDURE', 'INCLUDED_PROCEDURES',
        'INCLUDED_LAB_EVENTS', 'CONTAINED_LAB_EVENT',
        'RECORDED_LAB_RESULTS', 'INCLUDED_LAB_RESULTS',
        'CONTAINED_MICROBIOLOGY_EVENT', 'RECORDED_CHART_EVENTS',
        'DOCUMENTED_IN_NOTE', 'INCLUDED_HPI_SUMMARY',
        'INCLUDED_PAST_HISTORY', 'INCLUDED_TRIAGE_ASSESSMENT',
        'HAS_ALLERGY', 'INCLUDED_MEDICATIONS', 'RECORDED_MEDICATIONS',
        'STARTED_MEDICATIONS', 'STOPPED_MEDICATIONS',
        'LISTED_MEDICATIONS_TO_AVOID', 'RECORDED_VITALS',
        'WAS_ASSIGNED_DRG_CODE', 'MANAGED_ADMISSION'
    ])


@dataclass
class BatchProcessingConfig:
    """Batch processing configuration"""
    batch_size: int = 5000
    max_retries: int = 3
    resume_from_checkpoint: bool = True
    item_batch_size: int = 5000  # For Milvus insertions


@dataclass
class Config:
    """Main configuration container"""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig.from_env)
    milvus: MilvusConfig = field(default_factory=MilvusConfig.from_env)
    embedding: EmbeddingConfig = field(default_factory=EmbeddingConfig)
    graph: GraphConfig = field(default_factory=GraphConfig)
    batch_processing: BatchProcessingConfig = field(default_factory=BatchProcessingConfig)
    
    log_level: str = "INFO"
    output_dir: str = "./output"
    checkpoint_dir: str = "./checkpoints"
    progress_file: str = "./checkpoints/batch_progress.json"
    
    def __post_init__(self):
        """Create output directories"""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)

