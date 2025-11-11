"""
ETL Tracker for tracking processed patients and their data.
Uses a CSV file to track which patients have been processed by which scripts.
"""
import pandas as pd
import os
import logging
from typing import Set, List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class ETLTracker:
    """Tracks which patients have been processed by which scripts"""
    
    def __init__(self, tracker_file: str):
        """
        Initialize ETL tracker
        
        Args:
            tracker_file: Path to CSV file that tracks processed patients
        """
        self.tracker_file = tracker_file
        self._ensure_tracker_exists()
        self._load_tracker()
    
    def _ensure_tracker_exists(self):
        """Create tracker file if it doesn't exist"""
        if not os.path.exists(self.tracker_file):
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.tracker_file), exist_ok=True)
            
            # Create empty tracker with headers
            df = pd.DataFrame(columns=[
                'subject_id',
                'script_name',
                'processed_at',
                'status'
            ])
            df.to_csv(self.tracker_file, index=False)
            logger.info(f"Created new ETL tracker file: {self.tracker_file}")
    
    def _load_tracker(self):
        """Load tracker data from CSV"""
        try:
            if os.path.exists(self.tracker_file) and os.path.getsize(self.tracker_file) > 0:
                self.tracker_df = pd.read_csv(self.tracker_file)
                # Ensure subject_id is int
                self.tracker_df['subject_id'] = self.tracker_df['subject_id'].astype(int)
            else:
                self.tracker_df = pd.DataFrame(columns=[
                    'subject_id',
                    'script_name',
                    'processed_at',
                    'status'
                ])
        except Exception as e:
            logger.warning(f"Error loading tracker file: {e}. Starting with empty tracker.")
            self.tracker_df = pd.DataFrame(columns=[
                'subject_id',
                'script_name',
                'processed_at',
                'status'
            ])
    
    def _save_tracker(self):
        """Save tracker data to CSV"""
        try:
            self.tracker_df.to_csv(self.tracker_file, index=False)
        except Exception as e:
            logger.error(f"Error saving tracker file: {e}")
            raise
    
    def is_patient_processed(self, subject_id: int, script_name: str) -> bool:
        """
        Check if a patient has been processed by a specific script
        
        Args:
            subject_id: Patient subject_id
            script_name: Name of the script (e.g., '1_add_patient_nodes')
            
        Returns:
            True if patient has been processed, False otherwise
        """
        if self.tracker_df.empty:
            return False
        
        mask = (
            (self.tracker_df['subject_id'] == subject_id) &
            (self.tracker_df['script_name'] == script_name) &
            (self.tracker_df['status'] == 'success')
        )
        return mask.any()
    
    def get_processed_patients(self, script_name: str) -> Set[int]:
        """
        Get set of subject_ids that have been processed by a specific script
        
        Args:
            script_name: Name of the script
            
        Returns:
            Set of subject_id integers
        """
        if self.tracker_df.empty:
            return set()
        
        mask = (
            (self.tracker_df['script_name'] == script_name) &
            (self.tracker_df['status'] == 'success')
        )
        processed = self.tracker_df[mask]['subject_id'].unique()
        return set(processed)
    
    def mark_patient_processed(self, subject_id: int, script_name: str, status: str = 'success'):
        """
        Mark a patient as processed by a script
        
        Args:
            subject_id: Patient subject_id
            script_name: Name of the script
            status: Status of processing ('success' or 'failed')
        """
        # Remove any existing entry for this patient+script combination
        mask = (
            (self.tracker_df['subject_id'] == subject_id) &
            (self.tracker_df['script_name'] == script_name)
        )
        self.tracker_df = self.tracker_df[~mask]
        
        # Add new entry
        new_row = pd.DataFrame([{
            'subject_id': int(subject_id),
            'script_name': script_name,
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': status
        }])
        self.tracker_df = pd.concat([self.tracker_df, new_row], ignore_index=True)
        self._save_tracker()
    
    def mark_patients_processed_batch(self, subject_ids: List[int], script_name: str, status: str = 'success'):
        """
        Mark multiple patients as processed by a script (batch operation)
        
        Args:
            subject_ids: List of patient subject_ids
            script_name: Name of the script
            status: Status of processing ('success' or 'failed')
        """
        if not subject_ids:
            return
        
        # Remove existing entries for these patients+script combination
        mask = (
            (self.tracker_df['subject_id'].isin(subject_ids)) &
            (self.tracker_df['script_name'] == script_name)
        )
        self.tracker_df = self.tracker_df[~mask]
        
        # Add new entries
        new_rows = pd.DataFrame([{
            'subject_id': int(sid),
            'script_name': script_name,
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': status
        } for sid in subject_ids])
        self.tracker_df = pd.concat([self.tracker_df, new_rows], ignore_index=True)
        self._save_tracker()
    
    def get_all_processed_patients(self) -> Set[int]:
        """
        Get set of all subject_ids that have been processed by any script
        
        Returns:
            Set of subject_id integers
        """
        if self.tracker_df.empty:
            return set()
        
        mask = self.tracker_df['status'] == 'success'
        processed = self.tracker_df[mask]['subject_id'].unique()
        return set(processed)
    
    def get_processing_summary(self) -> Dict[str, int]:
        """
        Get summary of processing status
        
        Returns:
            Dictionary with script names as keys and count of processed patients as values
        """
        if self.tracker_df.empty:
            return {}
        
        mask = self.tracker_df['status'] == 'success'
        summary = self.tracker_df[mask].groupby('script_name')['subject_id'].nunique().to_dict()
        return summary

