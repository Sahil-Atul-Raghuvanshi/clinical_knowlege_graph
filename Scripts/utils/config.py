"""
Configuration settings for Patient Embedding System
Uses Neo4j for patient node embeddings
"""
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

# Find project root (parent of Scripts directory)
# This file is in Scripts/utils, so go up 2 levels
_config_file_path = Path(__file__).resolve()
_scripts_dir = _config_file_path.parent.parent
_project_root = _scripts_dir.parent

# Load .env file from project root
_env_path = _project_root / '.env'
if _env_path.exists():
    load_dotenv(_env_path)
else:
    # Fallback: try loading from current directory
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
        # Patient flow relationships
        'VISITED_ED',  # Patient -> EmergencyDepartment
        'ADMITTED_TO_UNIT',  # Patient -> UnitAdmission (for direct unit admissions)
        'LED_TO_UNIT_ADMISSION',  # UnitAdmission -> UnitAdmission (unit transfers)
        'LED_TO_DISCHARGE',  # UnitAdmission -> Discharge, Discharge -> Discharge
        'LED_TO_ED',  # UnitAdmission -> EmergencyDepartment
        'LED_TO_ED_VISIT',  # Discharge -> EmergencyDepartment (for subsequent ED visits)
        'LED_TO_FIRST_UNIT_ADMISSION',  # HospitalAdmission -> UnitAdmission (first unit)
        'LED_TO_ADMISSION',  # EmergencyDepartment -> HospitalAdmission (general)
        'LED_TO_ADMISSION_DURING_STAY',  # EmergencyDepartment -> HospitalAdmission (during ED stay)
        'LED_TO_ADMISSION_AFTER_DISCHARGE',  # EmergencyDepartment -> HospitalAdmission (after ED discharge)
        # Clinical data relationships - EmergencyDepartment
        'RECORDED_PREVIOUS_MEDICATIONS',  # EmergencyDepartment -> PreviousPrescriptionMeds
        'ADMINISTERED_MEDICATIONS',  # EmergencyDepartment -> AdministeredMeds
        'INCLUDED_TRIAGE_ASSESSMENT',  # EmergencyDepartment -> InitialAssessment
        'RECORDED_DIAGNOSES',  # EmergencyDepartment -> Diagnosis, Discharge -> Diagnosis
        'INCLUDED_PROCEDURES',  # EmergencyDepartment/UnitAdmission/ICUStay/HospitalAdmission -> ProceduresBatch
        'INCLUDED_LAB_EVENTS',  # EmergencyDepartment/UnitAdmission/ICUStay/HospitalAdmission -> LabEvents
        'ISSUED_PRESCRIPTIONS',  # EmergencyDepartment/UnitAdmission/ICUStay/HospitalAdmission -> PrescriptionsBatch
        # Clinical data relationships - Procedures and Prescriptions
        'CONTAINED_PROCEDURE',  # ProceduresBatch -> Procedures
        'CONTAINED_PRESCRIPTION',  # PrescriptionsBatch -> Prescription
        # Clinical data relationships - Labs and Microbiology
        'CONTAINED_LAB_EVENT',  # LabEvents -> LabEvent
        'CONTAINED_MICROBIOLOGY_EVENT',  # LabEvents -> MicrobiologyEvent
        'RECORDED_LAB_RESULTS',  # DischargeClinicalNote -> DischargeLabs
        'INCLUDED_LAB_RESULTS',  # HospitalAdmission -> AdmissionLabs
        # Clinical data relationships - Chart Events
        'RECORDED_CHART_EVENTS',  # ICUStay -> ChartEventBatch
        'CONTAINED_CHART_EVENT',  # ChartEventBatch -> ChartEvent
        # Clinical data relationships - HospitalAdmission
        'WAS_ASSIGNED_DRG_CODE',  # HospitalAdmission -> DRG
        'INCLUDED_PAST_HISTORY',  # HospitalAdmission -> PatientPastHistory
        'INCLUDED_HPI_SUMMARY',  # HospitalAdmission -> HPISummary
        'RECORDED_VITALS',  # HospitalAdmission -> AdmissionVitals, DischargeClinicalNote -> DischargeVitals
        'INCLUDED_MEDICATIONS',  # HospitalAdmission -> AdmissionMedications
        'MANAGED_ADMISSION',  # Provider -> HospitalAdmission
        # Clinical data relationships - Discharge
        'DOCUMENTED_IN_NOTE',  # Discharge -> DischargeClinicalNote
        'HAS_ALLERGY',  # Discharge -> AllergyIdentified
        'STARTED_MEDICATIONS',  # Discharge -> MedicationStarted
        'STOPPED_MEDICATIONS',  # Discharge -> MedicationStopped
        'LISTED_MEDICATIONS_TO_AVOID',  # Discharge -> MedicationToAvoid
        # Clinical data relationships - DischargeClinicalNote
        'RECORDED_MEDICATIONS'  # DischargeClinicalNote -> DischargeMedications
    ])


@dataclass
class BatchProcessingConfig:
    """Batch processing configuration"""
    batch_size: int = 5000
    max_retries: int = 3
    resume_from_checkpoint: bool = True


@dataclass
class Config:
    """Main configuration container"""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig.from_env)
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

