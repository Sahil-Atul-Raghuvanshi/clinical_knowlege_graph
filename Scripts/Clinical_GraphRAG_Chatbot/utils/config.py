"""
Configuration settings for Clinical GraphRAG Chatbot
"""
import os
import json
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
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
    
    @classmethod
    def from_json(cls, config_path: Optional[Path] = None):
        """Load configuration from JSON file"""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "neo4j_config.json"
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                return cls(**config_data)
        return cls.from_env()


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
    
    @classmethod
    def from_json(cls, config_path: Optional[Path] = None):
        """Load configuration from JSON file"""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "milvus_config.json"
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                # Handle nested collections structure
                if 'collections' in config_data:
                    collections = config_data.pop('collections')
                    config_data['prescription_collection'] = collections.get('prescription_items', config_data.get('prescription_collection', 'prescription_items'))
                    config_data['microbiology_collection'] = collections.get('microbiology_items', config_data.get('microbiology_collection', 'microbiology_items'))
                    config_data['lab_result_collection'] = collections.get('lab_result_items', config_data.get('lab_result_collection', 'lab_result_items'))
                    config_data['diagnosis_collection'] = collections.get('diagnosis_items', config_data.get('diagnosis_collection', 'diagnosis_items'))
                return cls(**config_data)
        return cls.from_env()

