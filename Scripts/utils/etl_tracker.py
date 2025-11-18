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
        Check if a patient has been processed by a specific script.
        Checks the most recent record (by processed_at timestamp) for the patient+script combination.
        Only returns True if the most recent record has status='success'.
        
        Args:
            subject_id: Patient subject_id
            script_name: Name of the script (e.g., '1_add_patient_nodes')
            
        Returns:
            True if patient has been successfully processed (most recent attempt was success), False otherwise
        """
        if self.tracker_df.empty:
            return False
        
        # Filter records for this patient and script
        mask = (
            (self.tracker_df['subject_id'] == subject_id) &
            (self.tracker_df['script_name'] == script_name)
        )
        patient_records = self.tracker_df[mask]
        
        if patient_records.empty:
            return False
        
        # Convert processed_at to datetime for proper sorting
        patient_records = patient_records.copy()
        patient_records['processed_at'] = pd.to_datetime(patient_records['processed_at'])
        
        # Sort by processed_at descending and get the most recent record
        patient_records = patient_records.sort_values('processed_at', ascending=False)
        most_recent_record = patient_records.iloc[0]
        
        # Only return True if the most recent record has status='success'
        return most_recent_record['status'] == 'success'
    
    def get_processed_patients(self, script_name: str) -> Set[int]:
        """
        Get set of subject_ids that have been processed by a specific script.
        Only includes patients where the most recent record (by processed_at) has status='success'.
        
        Args:
            script_name: Name of the script
            
        Returns:
            Set of subject_id integers
        """
        if self.tracker_df.empty:
            return set()
        
        # Filter records for this script
        script_records = self.tracker_df[self.tracker_df['script_name'] == script_name].copy()
        
        if script_records.empty:
            return set()
        
        # Convert processed_at to datetime for proper sorting
        script_records['processed_at'] = pd.to_datetime(script_records['processed_at'])
        
        # For each patient, get the most recent record
        processed_patients = set()
        for subject_id in script_records['subject_id'].unique():
            patient_records = script_records[script_records['subject_id'] == subject_id]
            patient_records = patient_records.sort_values('processed_at', ascending=False)
            most_recent_record = patient_records.iloc[0]
            
            # Only include if most recent record has status='success'
            if most_recent_record['status'] == 'success':
                processed_patients.add(int(subject_id))
        
        return processed_patients
    
    def mark_patient_processed(self, subject_id: int, script_name: str, status: str = 'success'):
        """
        Mark a patient as processed by a script.
        Adds a new entry without removing old ones, allowing history of attempts.
        The is_patient_processed() method will check the most recent entry.
        
        Args:
            subject_id: Patient subject_id
            script_name: Name of the script
            status: Status of processing ('success' or 'failed')
        """
        # Add new entry (keep old entries for history - is_patient_processed checks most recent)
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
        Mark multiple patients as processed by a script (batch operation).
        Adds new entries without removing old ones, allowing history of attempts.
        The is_patient_processed() method will check the most recent entry.
        
        Args:
            subject_ids: List of patient subject_ids
            script_name: Name of the script
            status: Status of processing ('success' or 'failed')
        """
        if not subject_ids:
            return
        
        # Add new entries (keep old entries for history - is_patient_processed checks most recent)
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
        Get set of all subject_ids that have been processed by any script.
        Only includes patients where the most recent record (by processed_at) has status='success'.
        
        Returns:
            Set of subject_id integers
        """
        if self.tracker_df.empty:
            return set()
        
        # Convert processed_at to datetime for proper sorting
        tracker_copy = self.tracker_df.copy()
        tracker_copy['processed_at'] = pd.to_datetime(tracker_copy['processed_at'])
        
        # For each patient+script combination, get the most recent record
        processed_patients = set()
        for (subject_id, script_name), group in tracker_copy.groupby(['subject_id', 'script_name']):
            group_sorted = group.sort_values('processed_at', ascending=False)
            most_recent_record = group_sorted.iloc[0]
            
            # Only include if most recent record has status='success'
            if most_recent_record['status'] == 'success':
                processed_patients.add(int(subject_id))
        
        return processed_patients
    
    def get_processing_summary(self) -> Dict[str, int]:
        """
        Get summary of processing status.
        Only counts patients where the most recent record (by processed_at) has status='success'.
        
        Returns:
            Dictionary with script names as keys and count of processed patients as values
        """
        if self.tracker_df.empty:
            return {}
        
        # Convert processed_at to datetime for proper sorting
        tracker_copy = self.tracker_df.copy()
        tracker_copy['processed_at'] = pd.to_datetime(tracker_copy['processed_at'])
        
        # For each script, count patients with most recent status='success'
        summary = {}
        for script_name in tracker_copy['script_name'].unique():
            script_records = tracker_copy[tracker_copy['script_name'] == script_name]
            processed_patients = set()
            
            for subject_id in script_records['subject_id'].unique():
                patient_records = script_records[script_records['subject_id'] == subject_id]
                patient_records = patient_records.sort_values('processed_at', ascending=False)
                most_recent_record = patient_records.iloc[0]
                
                if most_recent_record['status'] == 'success':
                    processed_patients.add(int(subject_id))
            
            summary[script_name] = len(processed_patients)
        
        return summary

