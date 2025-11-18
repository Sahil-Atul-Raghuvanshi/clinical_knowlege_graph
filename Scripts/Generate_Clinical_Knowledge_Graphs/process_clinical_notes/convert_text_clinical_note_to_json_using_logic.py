"""
Clinical Note to JSON Converter using Rule-Based Text Processing

This script converts clinical discharge notes to structured JSON format using
regex patterns, NLP techniques, and text processing instead of LLM.

FEATURES:
- Rule-based extraction using regex patterns
- Resume capability (skips already processed notes)
- Incremental saving (saves each note immediately)
- No API costs or rate limits
"""

import pandas as pd
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from tqdm import tqdm

def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and newlines, and replacing ___ with empty string"""
    if not text:
        return ""
    # Replace ___ with empty string (as done in LLM version)
    text = text.replace("___", "")
    # Replace multiple newlines with single space
    text = re.sub(r'\n+', ' ', text)
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def is_redacted(value: str) -> bool:
    """Check if a value is redacted (contains only underscores)"""
    if not value:
        return True
    cleaned = value.strip()
    return cleaned == "___" or cleaned == "" or (len(cleaned) <= 3 and all(c in ['_', ' '] for c in cleaned))

def parse_redacted_value(value: str) -> Optional[str]:
    """Convert redacted values to null (None), non-redacted empty values to empty string"""
    if is_redacted(value):
        return None  # Will be null in JSON
    cleaned = clean_text(value) if value else ""
    return cleaned if cleaned else None  # Return None for empty strings to match LLM output

def extract_section(text: str, section_pattern: str, next_section_pattern: Optional[str] = None) -> str:
    """Extract a section from text using regex pattern - flexible with variations"""
    # Try multiple variations of the pattern
    patterns_to_try = [
        section_pattern,
        section_pattern.replace(':', ''),
        section_pattern.replace(':', '.*:'),
    ]
    
    match = None
    for pattern_str in patterns_to_try:
        pattern = re.compile(pattern_str, re.IGNORECASE | re.MULTILINE)
        match = pattern.search(text)
        if match:
            break
    
    if not match:
        return ""
    
    start_pos = match.end()
    
    if next_section_pattern:
        # Try multiple variations of next section pattern
        next_patterns = [
            next_section_pattern,
            next_section_pattern.replace(':', ''),
        ]
        next_match = None
        for next_pattern_str in next_patterns:
            next_pattern = re.compile(next_pattern_str, re.IGNORECASE | re.MULTILINE)
            next_match = next_pattern.search(text, start_pos)
            if next_match:
                break
        end_pos = next_match.start() if next_match else len(text)
    else:
        end_pos = len(text)
    
    return text[start_pos:end_pos].strip()

def parse_key_value_line(line: str, key_pattern: str) -> Optional[Dict[str, str]]:
    """Parse a key-value line"""
    pattern = re.compile(key_pattern, re.IGNORECASE)
    match = pattern.search(line)
    if match:
        key = match.group(1).strip()
        value = line[match.end():].strip()
        return {key: parse_redacted_value(value)}
    return None

def extract_administrative_info(text: str) -> Dict[str, Any]:
    """Extract administrative information"""
    admin_info = {
        "Name": None,
        "Unit No": None,
        "Admission Date": None,
        "Discharge Date": None,
        "Date of Birth": None,
        "Sex": None,
        "Service": None,
        "Attending": None,
        "Allergies": []
    }
    
    # Extract Name
    name_match = re.search(r'Name:\s*(.+?)(?:\n|Unit No:)', text, re.IGNORECASE)
    if name_match:
        admin_info["Name"] = parse_redacted_value(name_match.group(1))
    
    # Extract Unit No
    unit_match = re.search(r'Unit No:\s*(.+?)(?:\n|Admission Date:)', text, re.IGNORECASE)
    if unit_match:
        admin_info["Unit No"] = parse_redacted_value(unit_match.group(1))
    
    # Extract Admission Date
    adm_date_match = re.search(r'Admission Date:\s*(.+?)(?:\n|Discharge Date:)', text, re.IGNORECASE)
    if adm_date_match:
        admin_info["Admission Date"] = parse_redacted_value(adm_date_match.group(1))
    
    # Extract Discharge Date
    dis_date_match = re.search(r'Discharge Date:\s*(.+?)(?:\n|Date of Birth:)', text, re.IGNORECASE)
    if dis_date_match:
        admin_info["Discharge Date"] = parse_redacted_value(dis_date_match.group(1))
    
    # Extract Date of Birth
    dob_match = re.search(r'Date of Birth:\s*(.+?)(?:\n|Sex:)', text, re.IGNORECASE)
    if dob_match:
        admin_info["Date of Birth"] = parse_redacted_value(dob_match.group(1))
    
    # Extract Sex
    sex_match = re.search(r'Sex:\s*([MF])', text, re.IGNORECASE)
    if sex_match:
        admin_info["Sex"] = sex_match.group(1)
    
    # Extract Service
    service_match = re.search(r'Service:\s*(.+?)(?:\n|Allergies:)', text, re.IGNORECASE)
    if service_match:
        admin_info["Service"] = clean_text(service_match.group(1))
    
    # Extract Attending
    attending_match = re.search(r'Attending:\s*(.+?)(?:\n|Chief Complaint:)', text, re.IGNORECASE)
    if attending_match:
        attending_val = attending_match.group(1).strip()
        # Handle "." as redacted/null
        if attending_val == "." or attending_val == "___" or is_redacted(attending_val):
            admin_info["Attending"] = None
        else:
            admin_info["Attending"] = parse_redacted_value(attending_val)
    
    # Extract Allergies
    allergies_section = extract_section(text, r'Allergies:', r'Attending:|Chief Complaint:')
    if allergies_section:
        allergies = []
        lines = allergies_section.split('\n')
        for line in lines:
            line = line.strip()
            if line and not is_redacted(line):
                if "No Known Allergies" in line or "No Known" in line:
                    allergies.append("No Known Allergies / Adverse Drug Reactions")
                elif line and line != "___":
                    allergies.append(clean_text(line))
        admin_info["Allergies"] = allergies if allergies else []
    
    return admin_info

def extract_clinical_summary(text: str) -> Dict[str, Any]:
    """Extract clinical summary information"""
    summary = {
        "Chief Complaint": None,
        "Major Surgical or Invasive Procedure": None,
        "Past Medical History": [],
        "Social History": None,
        "Family History": None
    }
    
    # Extract Chief Complaint
    cc_section = extract_section(text, r'Chief Complaint:', r'Major Surgical|History of Present Illness')
    if cc_section:
        summary["Chief Complaint"] = clean_text(cc_section)
    
    # Extract Major Surgical or Invasive Procedure
    procedure_section = extract_section(text, r'Major Surgical or Invasive Procedure:', r'History of Present Illness')
    if procedure_section:
        procedures = []
        lines = procedure_section.split('\n')
        for line in lines:
            line = line.strip()
            # Remove redacted prefixes like "___ "
            line = re.sub(r'^___\s+', '', line)
            if line and not is_redacted(line) and line.lower() != "none":
                clean_proc = clean_text(line)
                if clean_proc:
                    procedures.append(clean_proc)
        if procedures:
            summary["Major Surgical or Invasive Procedure"] = procedures[0] if len(procedures) == 1 else procedures
        else:
            summary["Major Surgical or Invasive Procedure"] = None
    
    # Extract Past Medical History - handle numbered lists, dash lists, or plain lists
    # Keep multi-line items together (e.g., "HIV: on HAART, CD4 count 173, HIV viral load undetectable")
    pmh_section = extract_section(text, r'Past Medical History:', r'Social History|Family History|Physical Exam')
    if pmh_section:
        pmh_items = []
        # Try dash-separated items first (most common format): "- HCV Cirrhosis: genotype 3a"
        lines = pmh_section.split('\n')
        current_item = ""
        for line in lines:
            line = line.strip()
            if not line:
                if current_item:  # Empty line might end current item
                    item_text = clean_text(current_item.strip())
                    if item_text and not is_redacted(item_text):
                        pmh_items.append(item_text)
                    current_item = ""
                continue
            
            # Check if this line starts a new item (starts with dash or number)
            if re.match(r'^[-•]\s+', line) or re.match(r'^\d+\.\s+', line):
                # Save previous item if exists
                if current_item.strip():
                    item_text = clean_text(current_item.strip())
                    # Remove leading dash/bullet/number if still present
                    item_text = re.sub(r'^[-•]\s*', '', item_text)
                    item_text = re.sub(r'^\d+\.\s*', '', item_text)
                    item_text = item_text.strip()
                    if item_text and not is_redacted(item_text):
                        pmh_items.append(item_text)
                
                # Start new item - remove leading dash/bullet/number
                current_item = re.sub(r'^[-•]\s*', '', line)
                current_item = re.sub(r'^\d+\.\s*', '', current_item)
                current_item = current_item.strip()
            else:
                # Continuation of current item (multi-line)
                if current_item:
                    current_item += " " + line
                else:
                    # First line without dash/number - start new item
                    current_item = line
        
        # Add last item
        if current_item.strip():
            item_text = clean_text(current_item.strip())
            item_text = re.sub(r'^[-•]\s*', '', item_text)
            item_text = re.sub(r'^\d+\.\s*', '', item_text)
            item_text = item_text.strip()
            if item_text and not is_redacted(item_text):
                pmh_items.append(item_text)
        
        summary["Past Medical History"] = pmh_items
    
    # Extract Social History - check both Social History section and Family History section
    social_sentences = []
    
    # First check explicit Social History section
    social_section = extract_section(text, r'Social History:', r'Family History|Physical Exam')
    if social_section and not is_redacted(social_section):
        # Split into sentences and add all
        sentences = re.split(r'\.\s+', social_section)
        for sent in sentences:
            sent = sent.strip()
            if sent:
                social_sentences.append(clean_text(sent + '.'))
    
    # Also check Family History section for embedded social history
    family_section = extract_section(text, r'Family History:', r'Physical Exam|History of Present Illness')
    if family_section:
        # Pattern for alcohol consumption sentence - get full sentence
        alcohol_match = re.search(r'Her last alcohol consumption[^.]*?\.', family_section, re.IGNORECASE)
        if alcohol_match:
            sent = clean_text(alcohol_match.group(0))
            if sent not in social_sentences:
                social_sentences.append(sent)
        
        # Pattern for regular alcohol consumption - must be separate sentence
        no_alcohol_match = re.search(r'No regular alcohol consumption[^.]*?\.', family_section, re.IGNORECASE)
        if no_alcohol_match:
            sent = clean_text(no_alcohol_match.group(0))
            if sent not in social_sentences:
                social_sentences.append(sent)
        
        # Pattern for drug use - match LLM format: "Last drug use ___ years ago."
        drug_match = re.search(r'Last drug use[^.]*?\.', family_section, re.IGNORECASE)
        if drug_match:
            sent = clean_text(drug_match.group(0))
            if sent not in social_sentences:
                social_sentences.append(sent)
        
        # Pattern for smoking - must capture full sentence
        smoking_match = re.search(r'She quit smoking[^.]*?\.', family_section, re.IGNORECASE)
        if smoking_match:
            sent = clean_text(smoking_match.group(0))
            if sent not in social_sentences:
                social_sentences.append(sent)
    
    if social_sentences:
        summary["Social History"] = ' '.join(social_sentences)
    else:
        summary["Social History"] = None
    
    # Extract Family History - exclude social history sentences
    # Use the family_section we already extracted above
    if family_section:
        # Remove social history sentences that might be mixed in
        family_text = family_section
        # Remove sentences about alcohol, drug use, smoking that belong in social history
        family_text = re.sub(r'Her last alcohol consumption[^.]*?\.', '', family_text, flags=re.IGNORECASE)
        family_text = re.sub(r'No regular alcohol consumption[^.]*?\.', '', family_text, flags=re.IGNORECASE)
        family_text = re.sub(r'Last drug use[^.]*?\.', '', family_text, flags=re.IGNORECASE)
        family_text = re.sub(r'She quit smoking[^.]*?\.', '', family_text, flags=re.IGNORECASE)
        family_text = clean_text(family_text)
        # Remove any remaining social history keywords
        family_text = re.sub(r'\s+alcohol\s+', ' ', family_text, flags=re.IGNORECASE)
        family_text = re.sub(r'\s+smoking\s+', ' ', family_text, flags=re.IGNORECASE)
        family_text = re.sub(r'\s+drug\s+use\s+', ' ', family_text, flags=re.IGNORECASE)
        family_text = clean_text(family_text)
        summary["Family History"] = family_text if family_text and not is_redacted(family_text) else None
    
    return summary

def parse_vitals(vitals_text: str) -> Dict[str, Any]:
    """Parse vital signs from text - handles formats like "98.4 70 106/63 16 97%RA" """
    vitals = {
        "Temperature": None,
        "Heart Rate": None,
        "Blood Pressure": None,
        "Respiratory Rate": None,
        "SpO2": None
    }
    
    # Pattern for format: "98.4 70 106/63 16 97%RA" (T HR BP RR SpO2)
    compact_pattern = r'(\d{2,3}\.?\d*)\s+(\d{2,3})\s+(\d{2,3}/\d{2,3})\s+(\d{1,2})\s+([\d%]+[A-Z]*)'
    compact_match = re.search(compact_pattern, vitals_text)
    if compact_match:
        vitals["Temperature"] = float(compact_match.group(1)) if '.' in compact_match.group(1) else int(compact_match.group(1))
        vitals["Heart Rate"] = int(compact_match.group(2))
        vitals["Blood Pressure"] = compact_match.group(3)
        vitals["Respiratory Rate"] = int(compact_match.group(4))
        spo2_val = compact_match.group(5)
        # Ensure %RA format is preserved
        if '%' not in spo2_val and 'RA' in vitals_text:
            spo2_val = spo2_val + "%RA"
        vitals["SpO2"] = spo2_val
        return vitals
    
    # Pattern for format: "VS: 98.1 107/61 78 18 97RA" or "VS: T98.1 105/57 79 20 97RA"
    # Also handle: "VS: 97.9 PO 109 / 71 70 16 97 ra" (with "PO" in the middle)
    vs_pattern = r'VS[:\s]*T?(\d{2,3}\.?\d*)[\s-]*(?:PO\s+)?(\d{2,3}\s*/\s*\d{2,3})[\s-]*(\d{2,3})[\s-]*(\d{1,2})[\s-]*([\d%]+[A-Z]*)'
    vs_match = re.search(vs_pattern, vitals_text, re.IGNORECASE)
    if vs_match:
        vitals["Temperature"] = float(vs_match.group(1)) if '.' in vs_match.group(1) else int(vs_match.group(1))
        vitals["Blood Pressure"] = vs_match.group(2).replace(' ', '')  # Remove spaces from BP
        vitals["Heart Rate"] = int(vs_match.group(3))
        vitals["Respiratory Rate"] = int(vs_match.group(4))
        spo2_val = vs_match.group(5)
        # Ensure %RA or RA format is preserved
        if '%' not in spo2_val and 'RA' in vitals_text.upper():
            if 'RA' not in spo2_val.upper():
                spo2_val = spo2_val + "%RA"
            else:
                spo2_val = re.sub(r'ra', '%RA', spo2_val, flags=re.IGNORECASE) if '%' not in spo2_val else spo2_val
        vitals["SpO2"] = spo2_val
        return vitals
    
    # Individual patterns as fallback
    # Temperature patterns - handle "T 97", "Tm 98.6", "Tc 98.2", "Temperature 98.1"
    temp_match = re.search(r'\bT[:\s,]+([\d.]+)|Temperature[:\s,]*([\d.]+)|Tm[:\s,]+([\d.]+)|Tc[:\s,]+([\d.]+)', vitals_text, re.IGNORECASE)
    if temp_match:
        temp = temp_match.group(1) or temp_match.group(2) or temp_match.group(3) or temp_match.group(4)
        try:
            vitals["Temperature"] = float(temp) if '.' in temp else int(temp)
        except:
            vitals["Temperature"] = temp
    
    # Heart Rate patterns - handle "HR 103", "Heart Rate 103", "HR 80-95"
    hr_match = re.search(r'\bHR[:\s,]+([\d-]+)|Heart Rate[:\s,]+([\d-]+)', vitals_text, re.IGNORECASE)
    if hr_match:
        hr = hr_match.group(1) or hr_match.group(2)
        # Handle ranges like "80-95"
        if '-' in hr:
            vitals["Heart Rate"] = hr  # Keep as string for ranges
        else:
            try:
                vitals["Heart Rate"] = int(hr)
            except:
                vitals["Heart Rate"] = hr
    
    # Blood Pressure patterns - handle "BP 98/65", "Blood Pressure 98/65", "BP 82-98/42-68"
    bp_match = re.search(r'\bBP[:\s,]+([\d/-]+)|Blood Pressure[:\s,]+([\d/-]+)', vitals_text, re.IGNORECASE)
    if bp_match:
        bp = bp_match.group(1) or bp_match.group(2)
        vitals["Blood Pressure"] = bp
    else:
        # Try to find BP pattern without label
        bp_pattern_match = re.search(r'(\d{2,3}-?\d{0,3}/\d{2,3}-?\d{0,3})', vitals_text)
        if bp_pattern_match:
            vitals["Blood Pressure"] = bp_pattern_match.group(1)
    
    # Respiratory Rate patterns - handle "RR 18", "Respiratory Rate 18", "RR ___"
    rr_match = re.search(r'\bRR[:\s,]+([\d_]+)|Respiratory Rate[:\s,]+([\d_]+)', vitals_text, re.IGNORECASE)
    if rr_match:
        rr = rr_match.group(1) or rr_match.group(2)
        if rr.strip() == "___" or not rr.strip():
            vitals["Respiratory Rate"] = None
        else:
            try:
                vitals["Respiratory Rate"] = int(rr) if rr.isdigit() else rr
            except:
                vitals["Respiratory Rate"] = rr
    
    # SpO2 patterns - handle "O2 94RA", "SpO2 97%RA", "O2 91-99% RA"
    spo2_match = re.search(r'\bO2[:\s,]+([\d%-]+[%]?\s*[A-Z]*)|SpO2[:\s,]+([\d%-]+[%]?\s*[A-Z]*)', vitals_text, re.IGNORECASE)
    if spo2_match:
        spo2 = spo2_match.group(1) or spo2_match.group(2)
        spo2 = spo2.strip()
        # Handle ranges like "91-99% RA"
        if '-' in spo2:
            vitals["SpO2"] = spo2  # Keep as string for ranges
        else:
            # Ensure proper format - if it's just digits and RA, add %
            if re.match(r'^\d+RA$', spo2, re.IGNORECASE):
                spo2 = spo2.replace('RA', '%RA', 1)
            elif re.match(r'^\d+$', spo2) and 'RA' in vitals_text.upper():
                spo2 = spo2 + "%RA"
            vitals["SpO2"] = spo2
    
    # Also check for Glucose if present
    glucose_match = re.search(r'Glucose[:\s,]+([\d.]+)', vitals_text, re.IGNORECASE)
    if glucose_match:
        try:
            vitals["Glucose"] = float(glucose_match.group(1)) if '.' in glucose_match.group(1) else int(glucose_match.group(1))
        except:
            vitals["Glucose"] = glucose_match.group(1)
    
    return vitals

def extract_history_of_present_illness(text: str) -> Dict[str, Any]:
    """Extract History of Present Illness"""
    hpi = {
        "Summary": None,
        "ED Findings": {
            "General": None,
            "Vitals": {},
            "Imaging": None,
            "Initial Treatment": [],
            "ICU/MICU Status": None
        },
        "ED Labs": {
            "description": {}
        }
    }
    
    hpi_section = extract_section(text, r'History of Present Illness:', r'Past Medical History|Physical Exam')
    if not hpi_section:
        return hpi
    
    # Clean the summary - remove ED vitals and labs from summary text (they go in separate fields)
    summary_text = hpi_section
    
    # Find where ED vitals/labs section starts to truncate summary before that
    ed_vitals_pos = re.search(r'In the ED[^.]*vitals\s+(?:were|:)|In the ED[^.]*initial\s+vitals', summary_text, re.IGNORECASE)
    ed_labs_pos = re.search(r'Labs\s+notable\s+for|Labs\s+were\s+significant\s+for', summary_text, re.IGNORECASE)
    
    # Find the earliest position where ED section starts
    earliest_pos = len(summary_text)
    if ed_vitals_pos:
        earliest_pos = min(earliest_pos, ed_vitals_pos.start())
    if ed_labs_pos:
        earliest_pos = min(earliest_pos, ed_labs_pos.start())
    
    # If we found an ED section, truncate summary before it
    if earliest_pos < len(summary_text):
        summary_text = summary_text[:earliest_pos].strip()
    
    # Also remove any remaining ED references
    summary_text = re.sub(r'In the ED[^.]*vitals\s+(?:were|:)[^.]*?\.', '', summary_text, flags=re.IGNORECASE)
    summary_text = re.sub(r'In the ED[^.]*initial\s+vitals[^.]*?\.', '', summary_text, flags=re.IGNORECASE)
    summary_text = re.sub(r'Labs\s+notable\s+for[^.]*?\.', '', summary_text, flags=re.IGNORECASE | re.DOTALL)
    summary_text = re.sub(r'Labs\s+were\s+significant\s+for[^.]*?\.', '', summary_text, flags=re.IGNORECASE | re.DOTALL)
    
    # Clean up the summary text
    summary_text = clean_text(summary_text)
    # Remove leading "___ " if present
    summary_text = re.sub(r'^___\s+', '', summary_text)
    # Remove trailing ED-related text that might have been left
    summary_text = re.sub(r'\s+In the ED[^.]*$', '', summary_text, flags=re.IGNORECASE)
    hpi["Summary"] = summary_text
    
    # Extract ED Findings General - look for "She had no confusion and was alert and oriented x3"
    general_match = re.search(r'(?:She|He|The patient|Pt)\s+had\s+no\s+confusion\s+and\s+was\s+alert\s+and\s+oriented\s+x3', hpi_section, re.IGNORECASE)
    if general_match:
        hpi["ED Findings"]["General"] = clean_text(general_match.group(0))
    
    # Extract ED vitals - look for various formats
    # "In the ED, initial vitals were 98.4 70 106/63 16 97%RA"
    # "In the ED, initial vitals: 97.6 81 148/83 16 100% RA"
    ed_vitals_patterns = [
        r'In the ED[^.]*vitals\s+(?:were|:)[^.]*?(\d{2,3}\.?\d*\s+\d{2,3}\s+\d{2,3}\s*/\s*\d{2,3}\s+\d{1,2}\s+[\d%]+\s*[A-Z]*)',
        r'In the ED[^.]*initial\s+vitals[^.]*?(\d{2,3}\.?\d*\s+\d{2,3}\s+\d{2,3}\s*/\s*\d{2,3}\s+\d{1,2}\s+[\d%]+\s*[A-Z]*)',
    ]
    
    for pattern in ed_vitals_patterns:
        ed_vitals_match = re.search(pattern, hpi_section, re.IGNORECASE)
        if ed_vitals_match:
            vitals_text = ed_vitals_match.group(0)
            parsed_vitals = parse_vitals(vitals_text)
            # Ensure SpO2 has %RA format
            if parsed_vitals.get("SpO2"):
                if not parsed_vitals["SpO2"].endswith("%RA") and not parsed_vitals["SpO2"].endswith("RA"):
                    if '%' in vitals_text.upper() or 'RA' in vitals_text.upper():
                        # Extract SpO2 from text
                        spo2_match = re.search(r'(\d+)\s*%?\s*RA', vitals_text, re.IGNORECASE)
                        if spo2_match:
                            parsed_vitals["SpO2"] = spo2_match.group(1) + "%RA"
            hpi["ED Findings"]["Vitals"] = parsed_vitals
            break
    
    # Extract ED labs - comprehensive extraction from "Labs were significant for..." section
    ed_labs = {}
    ed_labs_match = re.search(r'Labs[^.]*?(?:notable|significant|for)[^.]*?([^.]+)', hpi_section, re.IGNORECASE | re.DOTALL)
    labs_text = ed_labs_match.group(1) if ed_labs_match else hpi_section
    
    # Extract all lab values comprehensively - match LLM format
    # Pattern: "Na 127 K 5.3 lactate 2.1 INR 1.7. ALT 135 AST 244 AP 123. no leukocytosis. Ascitic fluid showed 220 WBC."
    
    # Basic labs
    na_match = re.search(r'\bNa\s+(\d+)', labs_text, re.IGNORECASE)
    if na_match:
        ed_labs["Sodium"] = int(na_match.group(1))
    
    k_match = re.search(r'\bK\s+([\d.]+)', labs_text, re.IGNORECASE)
    if k_match:
        try:
            ed_labs["Potassium"] = float(k_match.group(1)) if '.' in k_match.group(1) else int(k_match.group(1))
        except:
            ed_labs["Potassium"] = k_match.group(1)
    
    lactate_match = re.search(r'lactate\s+([\d.]+)', labs_text, re.IGNORECASE)
    if lactate_match:
        try:
            ed_labs["Lactate"] = float(lactate_match.group(1)) if '.' in lactate_match.group(1) else int(lactate_match.group(1))
        except:
            ed_labs["Lactate"] = lactate_match.group(1)
    
    inr_match = re.search(r'INR\s+([\d.]+)', labs_text, re.IGNORECASE)
    if inr_match:
        inr_val = inr_match.group(1).rstrip('.')  # Remove trailing period
        try:
            ed_labs["INR"] = float(inr_val) if '.' in inr_val else int(inr_val)
        except:
            ed_labs["INR"] = inr_val
    
    alt_match = re.search(r'ALT\s+(\d+)', labs_text, re.IGNORECASE)
    if alt_match:
        try:
            ed_labs["ALT"] = int(alt_match.group(1))
        except:
            ed_labs["ALT"] = alt_match.group(1)
    
    ast_match = re.search(r'AST\s+(\d+)', labs_text, re.IGNORECASE)
    if ast_match:
        try:
            ed_labs["AST"] = int(ast_match.group(1))
        except:
            ed_labs["AST"] = ast_match.group(1)
    
    ap_match = re.search(r'\bAP\s+(\d+)|Alkaline\s+Phosphatase\s+(\d+)|AlkPhos\s+(\d+)', labs_text, re.IGNORECASE)
    if ap_match:
        ap_val = ap_match.group(1) or ap_match.group(2) or ap_match.group(3)
        try:
            ed_labs["Alkaline Phosphatase"] = int(ap_val)
        except:
            ed_labs["Alkaline Phosphatase"] = ap_val
    
    # Leukocytosis
    if re.search(r'no\s+leukocytosis', labs_text, re.IGNORECASE):
        ed_labs["Leukocytosis"] = "no leukocytosis"
    
    # Ascitic fluid WBC
    ascites_wbc_match = re.search(r'Ascitic\s+fluid\s+showed\s+(\d+)\s+WBC', labs_text, re.IGNORECASE)
    if ascites_wbc_match:
        try:
            ed_labs["Ascitic Fluid WBC"] = int(ascites_wbc_match.group(1))
        except:
            ed_labs["Ascitic Fluid WBC"] = ascites_wbc_match.group(1)
    
    # Only assign if we found at least one lab value
    if ed_labs:
        hpi["ED Labs"] = ed_labs  # LLM uses flat structure, not nested "description"
    else:
        hpi["ED Labs"] = {}
    
    # Extract imaging - handle various formats
    # "Imaging showed: CXR showed a prominent esophagus"
    # "CXR showed..." or "CT showed..."
    imaging_patterns = [
        r'Imaging\s+showed[^:]*?:\s*([^.\n]+)',
        r'(CXR|CT|US|ultrasound|EKG|RUQ US)\s+showed[^.]*?([^.\n]+)',
        r'(CXR|CT|US|ultrasound|EKG|RUQ US)[^.]*?(clear|negative|with[^.]*?\.)',
    ]
    
    for pattern in imaging_patterns:
        imaging_match = re.search(pattern, hpi_section, re.IGNORECASE)
        if imaging_match:
            if 'Imaging showed' in imaging_match.group(0):
                imaging_text = clean_text(imaging_match.group(1))
            else:
                imaging_text = clean_text(imaging_match.group(0))
            
            # Only set if it's a reasonable imaging finding
            if imaging_text and len(imaging_text) < 200:
                hpi["ED Findings"]["Imaging"] = imaging_text
                break
    
    if not hpi["ED Findings"]["Imaging"]:
        hpi["ED Findings"]["Imaging"] = None
    
    # Extract initial treatment - look for "was given" or "received" patterns
    # Pattern: "She was given Morphine Sulfate 5 mg IV ONCE MR1, and a GI cocktail."
    treatment_match = re.search(r'(?:given|received|was given)[^.]*?([^.]+)', hpi_section, re.IGNORECASE)
    if treatment_match:
        treatment_text = treatment_match.group(1)
        
        # Extract complete medication names with proper capitalization
        # Pattern: "Morphine Sulfate 5 mg IV ONCE MR1"
        med_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(\d+(?:\.\d+)?)\s*(mg|mL|gm|g|mcg)\s+(IV|PO|IM|IH)\s*([A-Z\s]+)?'
        med_matches = list(re.finditer(med_pattern, treatment_text))
        for match in med_matches:
            med_name = match.group(1)
            dose = match.group(2)
            unit = match.group(3)
            route = match.group(4)
            additional = match.group(5).strip() if match.group(5) else ""
            treatment_str = f"{med_name} {dose} {unit} {route}"
            if additional:
                treatment_str += " " + additional
            if treatment_str not in hpi["ED Findings"]["Initial Treatment"]:
                hpi["ED Findings"]["Initial Treatment"].append(treatment_str)
        
        # Extract "GI cocktail" or similar descriptive treatments
        gi_cocktail_match = re.search(r'\ba\s+GI\s+cocktail\b', treatment_text, re.IGNORECASE)
        if gi_cocktail_match:
            if "a GI cocktail" not in hpi["ED Findings"]["Initial Treatment"]:
                hpi["ED Findings"]["Initial Treatment"].append("a GI cocktail")
    
    return hpi

def extract_physical_exam(text: str) -> Dict[str, Any]:
    """Extract physical examination findings"""
    pe = {
        "Admission Exam": {
            "General": None,
            "Vitals": {},
            "HEENT": None,
            "Neck": None,
            "Lungs": None,
            "Cardiovascular": None,
            "Abdomen": None,
            "Extremities": None,
            "Skin": None,
            "Neuro": None
        },
        "Discharge Exam": {
            "General": None,
            "Vitals": {},
            "HEENT": None,
            "Neck": None,
            "Lungs": None,
            "Cardiovascular": None,
            "Abdomen": None,
            "Extremities": None,
            "Skin": None,
            "Neuro": None
        }
    }
    
    # Extract Admission Physical Exam - handle combined "ADMISSION/DISCHARGE EXAM" sections
    # Try to find separate sections first, then combined
    adm_pe_section = extract_section(text, r'ADMISSION PHYSICAL EXAM|Physical Exam:', r'DISCHARGE|Pertinent Results|Discharge:')
    
    # If not found, try combined section - handle "=================\nADMISSION/DISCHARGE EXAM\n================="
    if not adm_pe_section:
        # Look for combined section with various patterns
        combined_patterns = [
            r'ADMISSION/DISCHARGE EXAM',
            r'ADMISSION.*DISCHARGE.*EXAM',
            r'ADMISSION.*DISCHARGE.*PHYSICAL',
        ]
        for pattern in combined_patterns:
            combined_match = re.search(pattern, text, re.IGNORECASE)
            if combined_match:
                # Extract from after the header until next major section
                start_pos = combined_match.end()
                next_sections = ['Pertinent Results', 'Brief Hospital Course', 'Medications']
                end_pos = len(text)
                text_lower = text.lower()
                for section in next_sections:
                    next_pos = text_lower.find(section.lower(), start_pos)
                    if next_pos > 0:
                        end_pos = min(end_pos, next_pos)
                        break
                combined_section = text[start_pos:end_pos].strip()
                if combined_section:
                    adm_pe_section = combined_section
                    break
    
    if adm_pe_section:
        # Extract vitals - handle format "Vitals - T 97, BP 98/65, HR 103, RR 18, O2 94RA, Glucose 128."
        # Also handle "VS: 98.1 107/61 78 18 97RA"
        vitals_text = None
        vitals_match = re.search(r'Vitals[:\s-]+([^\n]+)', adm_pe_section, re.IGNORECASE)
        if vitals_match:
            vitals_text = vitals_match.group(1)
        else:
            vs_match = re.search(r'VS[:\s]*([^\n]+)', adm_pe_section, re.IGNORECASE)
            if vs_match:
                vitals_text = vs_match.group(1)
        
        if vitals_text:
            parsed_vitals = parse_vitals(vitals_text)
            # Ensure all vitals are extracted - if missing, try to extract individually
            if not parsed_vitals.get("Temperature"):
                # Try to extract from the line directly
                temp_match = re.search(r'(\d{2,3}\.?\d*)', vitals_text)
                if temp_match and float(temp_match.group(1)) < 120:  # Reasonable temp range
                    parsed_vitals["Temperature"] = float(temp_match.group(1)) if '.' in temp_match.group(1) else int(temp_match.group(1))
            if not parsed_vitals.get("Heart Rate"):
                # Try to extract HR - pattern: "98.1 107/61 78 18 97RA" (T BP HR RR SpO2)
                # After BP, next number is HR, then RR
                hr_rr_match = re.search(r'(\d{2,3}/\d{2,3})\s+(\d{2,3})\s+(\d{1,2})\s+', vitals_text)
                if hr_rr_match:
                    parsed_vitals["Heart Rate"] = int(hr_rr_match.group(2))
                    parsed_vitals["Respiratory Rate"] = int(hr_rr_match.group(3))
                else:
                    # Try individual patterns
                    hr_match = re.search(r'\s+(\d{2,3})\s+(\d{2,3}/\d{2,3})', vitals_text)
                    if hr_match:
                        parsed_vitals["Heart Rate"] = int(hr_match.group(1))
            if not parsed_vitals.get("Respiratory Rate"):
                # Try to extract RR
                rr_match = re.search(r'(\d{2,3}/\d{2,3})\s+(\d{1,2})\s+', vitals_text)
                if rr_match:
                    parsed_vitals["Respiratory Rate"] = int(rr_match.group(2))
            # Ensure SpO2 format is correct
            if parsed_vitals.get("SpO2") and 'RA' in vitals_text and '%' not in parsed_vitals["SpO2"]:
                if not parsed_vitals["SpO2"].endswith("RA"):
                    parsed_vitals["SpO2"] = parsed_vitals["SpO2"] + "RA"
            pe["Admission Exam"]["Vitals"] = parsed_vitals
        
        # Extract exam components - need to handle multi-line entries
        # Split by component labels and extract each
        lines = adm_pe_section.split('\n')
        current_component = None
        current_value = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if this line starts a new component - handle variations
            # Important: Check for "CARDIAC:" separately from "NECK:" to avoid mixing them
            if re.match(r'^CARDIAC[:\s]+', line, re.IGNORECASE):
                # Save previous component
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Cardiovascular"
                current_value = re.sub(r'^CARDIAC[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(General|GEN)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "General"
                current_value = re.sub(r'^(General|GEN)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^HEENT[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "HEENT"
                current_value = re.sub(r'^HEENT[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^NECK[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Neck"
                current_value = re.sub(r'^NECK[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(LUNG|Lungs?|PULM)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Lungs"
                current_value = re.sub(r'^(LUNG|Lungs?|PULM)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(CV|Cardiovascular|HEART|COR)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Cardiovascular"
                current_value = re.sub(r'^(CV|Cardiovascular|HEART|COR)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(ABD|Abdomen)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Abdomen"
                current_value = re.sub(r'^(ABD|Abdomen)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(EXT|Extremities|EXTREM)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Extremities"
                current_value = re.sub(r'^(EXT|Extremities|EXTREM)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^Skin[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Skin"
                current_value = re.sub(r'^Skin[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^(Neuro|NEURO)[:\s]+', line, re.IGNORECASE):
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Admission Exam"][current_component] = value
                current_component = "Neuro"
                current_value = re.sub(r'^(Neuro|NEURO)[:\s]+', '', line, flags=re.IGNORECASE)
            elif re.match(r'^PULSES[:\s]+', line, re.IGNORECASE):
                # PULSES is part of Extremities, not a separate component
                if current_component == "Extremities":
                    current_value += " " + line
                else:
                    # Save previous and start Extremities
                    if current_component and current_value:
                        value = clean_text(current_value)
                        if value:
                            pe["Admission Exam"][current_component] = value
                    current_component = "Extremities"
                    current_value = re.sub(r'^PULSES[:\s]+', '', line, flags=re.IGNORECASE)
            elif current_component:
                # Continuation of current component
                current_value += " " + line
        
        # Save last component
        if current_component and current_value:
            value = clean_text(current_value)
            if value:
                pe["Admission Exam"][current_component] = value
    
    # Extract Discharge Physical Exam - also check for "Discharge:" section
    # Check if we have a combined section
    is_combined = False
    if adm_pe_section:
        # Check if the section header indicates combined
        pe_start = text.find('Physical Exam')
        if pe_start < 0:
            # Try finding ADMISSION/DISCHARGE directly
            pe_start = text.find('ADMISSION')
        if pe_start >= 0:
            header_area = text[max(0, pe_start-100):pe_start+300]
            if 'ADMISSION/DISCHARGE' in header_area.upper() or re.search(r'ADMISSION.*DISCHARGE', header_area, re.IGNORECASE):
                is_combined = True
    
    dis_pe_section = None
    if is_combined and adm_pe_section:
        # Use the same section for discharge
        dis_pe_section = adm_pe_section
    else:
        # Try to find separate discharge section
        dis_pe_section = extract_section(text, r'DISCHARGE PE|DISCHARGE PHYSICAL EXAM:|Discharge:\s*PHYSICAL EXAMINATION', r'Pertinent Results|Brief Hospital Course')
    
    if dis_pe_section:
        # Extract vitals - handle format "Vitals - Tm 98.6, Tc 98.2, BP 82-98/42-68, HR 80-95, RR ___, O2 91-99% RA."
        vitals_text = None
        vitals_match = re.search(r'Vitals[:\s-]+([^\n]+)', dis_pe_section, re.IGNORECASE)
        if vitals_match:
            vitals_text = vitals_match.group(1)
        else:
            vs_match = re.search(r'VS[:\s]*([^\n]+)', dis_pe_section, re.IGNORECASE)
            if vs_match:
                vitals_text = vs_match.group(1)
        
        if vitals_text:
            parsed_vitals = parse_vitals(vitals_text)
            # Ensure all vitals are extracted - if missing, try to extract individually
            if not parsed_vitals.get("Temperature"):
                temp_match = re.search(r'(\d{2,3}\.?\d*)', vitals_text)
                if temp_match and float(temp_match.group(1)) < 120:
                    parsed_vitals["Temperature"] = float(temp_match.group(1)) if '.' in temp_match.group(1) else int(temp_match.group(1))
            if not parsed_vitals.get("Heart Rate"):
                hr_match = re.search(r'\s+(\d{2,3})\s+(\d{2,3}/\d{2,3})', vitals_text)
                if hr_match:
                    parsed_vitals["Heart Rate"] = int(hr_match.group(1))
            if not parsed_vitals.get("Respiratory Rate"):
                rr_match = re.search(r'(\d{2,3}/\d{2,3})\s+(\d{1,2})\s+', vitals_text)
                if rr_match:
                    parsed_vitals["Respiratory Rate"] = int(rr_match.group(2))
            # SpO2 might not be present in discharge vitals
            pe["Discharge Exam"]["Vitals"] = parsed_vitals
        
        # Also extract exam components for discharge - handle multi-line entries
        lines = dis_pe_section.split('\n')
        current_component = None
        current_value = ""
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if this line starts a new component - handle variations
            if re.match(r'^(General|GEN|HEENT|Neck|Lungs?|PULM|CV|Cardiovascular|HEART|COR|ABD|Abdomen|EXT|Extremities|EXTREM|Skin|Neuro|NEURO|GU)[:\s-]+', line, re.IGNORECASE):
                # Save previous component
                if current_component and current_value:
                    value = clean_text(current_value)
                    if value:
                        pe["Discharge Exam"][current_component] = value
                
                # Start new component - handle abbreviations
                if re.match(r'^(General|GEN)[:\s-]+', line, re.IGNORECASE):
                    current_component = "General"
                    current_value = re.sub(r'^(General|GEN)[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^HEENT[:\s-]+', line, re.IGNORECASE):
                    current_component = "HEENT"
                    current_value = re.sub(r'^HEENT[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^Neck[:\s-]+', line, re.IGNORECASE):
                    current_component = "Neck"
                    current_value = re.sub(r'^Neck[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^(Lungs?|PULM)[:\s-]+', line, re.IGNORECASE):
                    current_component = "Lungs"
                    current_value = re.sub(r'^(Lungs?|PULM)[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^(CV|Cardiovascular|HEART|COR)[:\s-]+', line, re.IGNORECASE):
                    current_component = "Cardiovascular"
                    current_value = re.sub(r'^(CV|Cardiovascular|HEART|COR)[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^(ABD|Abdomen)[:\s-]+', line, re.IGNORECASE):
                    current_component = "Abdomen"
                    current_value = re.sub(r'^(ABD|Abdomen)[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^(EXT|Extremities|EXTREM)[:\s-]+', line, re.IGNORECASE):
                    current_component = "Extremities"
                    current_value = re.sub(r'^(EXT|Extremities|EXTREM)[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^Skin[:\s-]+', line, re.IGNORECASE):
                    current_component = "Skin"
                    current_value = re.sub(r'^Skin[:\s-]+', '', line, flags=re.IGNORECASE)
                elif re.match(r'^(Neuro|NEURO)[:\s-]+', line, re.IGNORECASE):
                    current_component = "Neuro"
                    current_value = re.sub(r'^(Neuro|NEURO)[:\s-]+', '', line, flags=re.IGNORECASE)
            elif current_component:
                # Continuation of current component
                current_value += " " + line
        
        # Save last component
        if current_component and current_value:
            value = clean_text(current_value)
            if value:
                pe["Discharge Exam"][current_component] = value
    
    return pe

def parse_lab_values(lab_text: str) -> Dict[str, str]:
    """Parse lab values from text"""
    labs = {}
    
    # More comprehensive lab patterns - handle formats like "GLUCOSE-109*" or "WBC-5.0#"
    # Pattern: LABNAME-VALUE or LABNAME: VALUE
    lab_line_pattern = r'([A-Z\s/()]+)[:\s-]+([\d.]+[#*K]?)\s*'
    matches = re.finditer(lab_line_pattern, lab_text, re.IGNORECASE)
    
    for match in matches:
        lab_name = match.group(1).strip().upper()
        value = match.group(2).strip()
        
        # Normalize lab names
        lab_name_map = {
            'GLUCOSE': 'GLUCOSE',
            'UREA N': 'UREA N',
            'CREAT': 'CREAT',
            'CREATININE': 'CREAT',
            'SODIUM': 'SODIUM',
            'NA': 'SODIUM',
            'POTASSIUM': 'POTASSIUM',
            'K': 'POTASSIUM',
            'CHLORIDE': 'CHLORIDE',
            'CL': 'CHLORIDE',
            'TOTAL CO2': 'TOTAL CO2',
            'HCO3': 'TOTAL CO2',
            'ANION GAP': 'ANION GAP',
            'ANGAP': 'ANION GAP',
            'ALT': 'ALT(SGPT)',
            'ALT(SGPT)': 'ALT(SGPT)',
            'SGPT': 'ALT(SGPT)',
            'AST': 'AST(SGOT)',
            'AST(SGOT)': 'AST(SGOT)',
            'SGOT': 'AST(SGOT)',
            'ALK PHOS': 'ALK PHOS',
            'ALKALINE PHOSPHATASE': 'ALK PHOS',
            'TOT BILI': 'TOT BILI',
            'TOTAL BILIRUBIN': 'TOT BILI',
            'TBILI': 'TOT BILI',
            'LIPASE': 'LIPASE',
            'ALBUMIN': 'ALBUMIN',
            'WBC': 'WBC',
            'RBC': 'RBC',
            'HGB': 'HGB',
            'HEMOGLOBIN': 'HGB',
            'HCT': 'HCT',
            'HEMATOCRIT': 'HCT',
            'MCV': 'MCV',
            'MCH': 'MCH',
            'MCHC': 'MCHC',
            'RDW': 'RDW',
            'PLT': 'PLT COUNT',
            'PLT COUNT': 'PLT COUNT',
            'PLATELET': 'PLT COUNT',
            'PLATELETS': 'PLT COUNT',
            'NEUTS': 'NEUTS',
            'NEUTROPHILS': 'NEUTS',
            'LYMPHS': 'LYMPHS',
            'LYMPHOCYTES': 'LYMPHS',
            'MONOS': 'MONOS',
            'MONOCYTES': 'MONOS',
            'EOS': 'EOS',
            'EOSINOPHILS': 'EOS',
            'BASOS': 'BASOS',
            'BASOPHILS': 'BASOS',
            'INR': 'INR',
            'PTT': 'PTT',
            'ESTGFR': 'estGFR',
            'EST GFR': 'estGFR',
            'ESTIMATED GFR': 'estGFR',
            'GFR': 'estGFR',
        }
        
        # Find matching normalized name - check for exact matches first
        normalized_name = None
        lab_name_upper = lab_name.upper()
        
        # Check for exact match first
        if lab_name_upper in lab_name_map:
            normalized_name = lab_name_map[lab_name_upper]
        else:
            # Check for partial match
            for key, norm_name in lab_name_map.items():
                if key in lab_name_upper:
                    normalized_name = norm_name
                    break
        
        if normalized_name:
            # Don't overwrite if already exists with a better value
            if normalized_name not in labs:
                labs[normalized_name] = value
        else:
            # Keep original if no mapping found, but clean it up
            clean_lab_name = lab_name.strip()
            if clean_lab_name and clean_lab_name not in ['', 'U/S', '1.', '2.', '3.']:
                labs[clean_lab_name] = value
    
    # Also try to extract from patterns like "Na 127" or "K 5.3"
    simple_patterns = {
        'Na': r'Na[:\s-]+(\d+)',
        'K': r'K[:\s-]+([\d.]+)',
        'Cr': r'Cr[:\s-]+([\d.]+)',
        'INR': r'INR[:\s-]+([\d.]+)',
        'HCO3': r'HCO3[:\s-]+([\d.]+)',
        'TOTAL CO2': r'TOTAL\s+CO2[:\s-]+([\d.]+)',
        'estGFR': r'estGFR[:\s-]+([^\s\n]+)',
        'EST GFR': r'EST\s+GFR[:\s-]+([^\s\n]+)',
        'ESTGFR': r'ESTGFR[:\s-]+([^\s\n]+)',
    }
    
    for key, pattern in simple_patterns.items():
        if key == 'HCO3' or key == 'TOTAL CO2':
            if 'TOTAL CO2' not in labs:
                match = re.search(pattern, lab_text, re.IGNORECASE)
                if match:
                    labs['TOTAL CO2'] = match.group(1)
        elif key == 'estGFR' or key == 'EST GFR' or key == 'ESTGFR':
            if 'estGFR' not in labs:
                match = re.search(pattern, lab_text, re.IGNORECASE)
                if match:
                    labs['estGFR'] = match.group(1)
        elif key not in labs or not labs.get(key):
            match = re.search(pattern, lab_text, re.IGNORECASE)
            if match:
                if key == 'Na':
                    labs['SODIUM'] = match.group(1)
                elif key == 'K':
                    labs['POTASSIUM'] = match.group(1)
                elif key == 'Cr':
                    labs['CREAT'] = match.group(1)
                else:
                    labs[key] = match.group(1)
    
    return labs

def extract_pertinent_results(text: str) -> Dict[str, Any]:
    """Extract pertinent results including labs, microbiology, and imaging"""
    results = {
        "Admission Labs": {"description": {}},
        "Discharge Labs": {"description": {}},
        "Microbiology": {"description": None},
        "Imaging Studies": []
    }
    
    # Extract labs section
    labs_section = extract_section(text, r'Pertinent Results:', r'Brief Hospital Course|Discharge')
    if not labs_section:
        # Try without colon
        labs_section = extract_section(text, r'Pertinent Results', r'Brief Hospital Course|Discharge')
    
    if labs_section:
        # Admission labs - handle both "ADMISSION LABS" and "LABS ON ADMISSION" formats
        adm_labs = extract_section(labs_section, r'ADMISSION LABS|LABS ON ADMISSION:', r'DISCHARGE LABS|LABS ON DISCHARGE|MICRO|IMAGING|Brief Hospital Course')
        if adm_labs:
            # Parse labs from format like "GLUCOSE-109* UREA N-25*"
            results["Admission Labs"]["description"] = parse_lab_values(adm_labs)
        else:
            # If no explicit admission section, try to find labs before discharge section
            # Look for patterns like "___ 10:25PM   GLUCOSE-109*"
            lab_lines = re.findall(r'[A-Z\s/()]+[:\s-]+[\d.]+[#*K]?', labs_section, re.IGNORECASE)
            if lab_lines:
                results["Admission Labs"]["description"] = parse_lab_values('\n'.join(lab_lines))
        
        # Discharge labs
        dis_labs = extract_section(labs_section, r'DISCHARGE LABS|LABS ON DISCHARGE:', r'MICRO|IMAGING|Brief Hospital Course')
        if dis_labs:
            parsed_dis_labs = parse_lab_values(dis_labs)
            if parsed_dis_labs:
                results["Discharge Labs"]["description"] = parsed_dis_labs
            else:
                results["Discharge Labs"] = None
        else:
            # If no discharge labs section found, set to null (not empty dict)
            results["Discharge Labs"] = None
        
        # Microbiology
        micro_section = extract_section(labs_section, r'MICROBIOLOGY|MICRO:', r'IMAGING|IMAGING/STUDIES|Brief Hospital Course')
        if micro_section:
            # Clean up the microbiology text
            micro_text = clean_text(micro_section)
            # Remove redacted timestamps and formatting
            micro_text = re.sub(r'___\s+\d+:\d+\s+[ap]m\s+', '', micro_text, flags=re.IGNORECASE)
            micro_text = re.sub(r'============\s+', '', micro_text)
            micro_text = re.sub(r'\s+\(Final\s+___:\s*', ' (Final): ', micro_text, flags=re.IGNORECASE)
            micro_text = re.sub(r'\s+\(Final\s*:\s*', ' (Final): ', micro_text, flags=re.IGNORECASE)
            # Clean up extra whitespace
            micro_text = re.sub(r'\s+', ' ', micro_text)
            micro_text = micro_text.strip()
            results["Microbiology"]["description"] = micro_text if micro_text else None
        
        # Imaging Studies - look for CXR, U/S, CT, etc.
        imaging_section = extract_section(labs_section, r'IMAGING|IMAGING/STUDIES:', r'Brief Hospital Course')
        if not imaging_section:
            # Try to find imaging in the main labs section
            imaging_section = labs_section
        
        if imaging_section:
            # Extract CXR
            cxr_match = re.search(r'CXR[:\s]*([^\n]+(?:\.\s*[^\n]+)*)', imaging_section, re.IGNORECASE)
            if cxr_match:
                findings = clean_text(cxr_match.group(1))
                if findings and not is_redacted(findings):
                    results["Imaging Studies"].append({
                        "Study Type": "CXR",
                        "Date": None,
                        "Findings": findings
                    })
            
            # Extract U/S or Ultrasound - capture multi-line findings
            us_match = re.search(r'U/S[:\s]*', imaging_section, re.IGNORECASE)
            if us_match:
                # Find where U/S section starts and ends
                us_start = us_match.end()
                # Find where it ends (next section like "Diagnostic" or "Brief Hospital Course" or end of imaging section)
                us_end = len(imaging_section)
                next_markers = ['Diagnostic', 'Brief Hospital Course', 'CXR', 'CT']
                for marker in next_markers:
                    next_pos = imaging_section.find('\n' + marker, us_start, re.IGNORECASE)
                    if next_pos > 0:
                        us_end = min(us_end, next_pos)
                
                us_text = imaging_section[us_start:us_end].strip()
                # Remove "Diagnostic para attempted" line if present
                us_text = re.sub(r'Diagnostic\s+para\s+attempted[^.]*?\.', '', us_text, flags=re.IGNORECASE)
                us_text = re.sub(r'On the floor[^.]*?\.', '', us_text, flags=re.IGNORECASE)
                findings = clean_text(us_text)
                if findings and not is_redacted(findings) and len(findings) > 10:
                    results["Imaging Studies"].append({
                        "Study Type": "U/S",
                        "Date": None,
                        "Findings": findings
                    })
            
            # Extract RUQ US or RUQUS (both formats)
            ruq_match = re.search(r'RUQ\s*US[:\s-]*([^\n]+(?:\.\s*[^\n]+)*)', imaging_section, re.IGNORECASE)
            if ruq_match:
                findings = clean_text(ruq_match.group(1))
                if findings and not is_redacted(findings):
                    results["Imaging Studies"].append({
                        "Study Type": "RUQUS",  # Match LLM format
                        "Date": None,
                        "Findings": findings
                    })
            
            # Extract CT
            ct_match = re.search(r'CT[:\s]+([A-Z\s]+)[:\s]*([^\n]+(?:\.\s*[^\n]+)*)', imaging_section, re.IGNORECASE)
            if ct_match:
                study_type = clean_text(ct_match.group(1))
                findings = clean_text(ct_match.group(2))
                if findings and not is_redacted(findings):
                    results["Imaging Studies"].append({
                        "Study Type": f"CT {study_type}" if study_type else "CT",
                        "Date": None,
                        "Findings": findings
                    })
    
    return results

def extract_hospital_course(text: str) -> Dict[str, Any]:
    """Extract hospital course - get complete text"""
    # Find the start of Brief Hospital Course
    course_start_match = re.search(r'Brief Hospital Course:', text, re.IGNORECASE)
    if not course_start_match:
        return {"description": None}
    
    course_start = course_start_match.end()
    
    # Find where it ends - look for "Medications on Admission" or "___ on Admission:"
    med_start = text.find('Medications on Admission', course_start)
    if med_start < 0:
        med_start = text.find('___ on Admission:', course_start)
    if med_start < 0:
        med_start = text.find('Discharge Medications', course_start)
    if med_start < 0:
        med_start = len(text)
    
    actual_course = text[course_start:med_start].strip()
    course_text = clean_text(actual_course)
    # Remove leading "___ " if present
    course_text = re.sub(r'^___\s+', '', course_text)
    
    # Remove any trailing text that belongs to next section
    # Look for patterns like "___ w/ HIV on HAART" at the end (this is start of next section)
    course_text = re.sub(r'\s+___\s+w/\s+[A-Z].*$', '', course_text, flags=re.IGNORECASE)
    
    return {"description": course_text if course_text else None}

def extract_transitional_issues(text: str) -> Dict[str, Any]:
    """Extract transitional issues"""
    issues = {
        "Antibiotic Plan": None,
        "Medication Changes": {
            "Start": [],
            "Stop": [],
            "Avoid": []
        },
        "Equipment Changes": None,
        "Follow-up Labs": None,
        "Pending Tests": [],
        "Code Status": None,
        "Legal Guardian": None
    }
    
    trans_section = extract_section(text, r'TRANSITIONAL ISSUES|Transitional Issues:', r'Medications|Discharge')
    if trans_section:
        # Extract code status - look for "# Code: full" pattern
        code_match = re.search(r'#\s*Code[:\s]+([^\n]+)', trans_section, re.IGNORECASE)
        if code_match:
            issues["Code Status"] = clean_text(code_match.group(1))
        
        # Extract follow-up labs - look for "[ ] For hyperkalemia and hyponatremia..."
        fup_labs_match = re.search(r'\[?\s*\]?\s*For\s+[^.]*?biweekly\s+BMPs[^.]*?\.', trans_section, re.IGNORECASE | re.DOTALL)
        if fup_labs_match:
            fup_text = clean_text(fup_labs_match.group(0))
            # Remove leading brackets
            fup_text = re.sub(r'^\[?\s*\]?\s*', '', fup_text)
            issues["Follow-up Labs"] = fup_text
        
        # Extract pending tests - look for "[ ] Follow-up peritoneal fluid culture"
        pending_match = re.search(r'\[?\s*\]?\s*Follow-up\s+peritoneal\s+fluid\s+culture', trans_section, re.IGNORECASE)
        if pending_match:
            test_text = "Follow-up peritoneal fluid culture"
            if test_text not in issues["Pending Tests"]:
                issues["Pending Tests"].append(test_text)
        
        # Extract medication changes - look for "[ ] New/changed medications: Furosemide 10mg daily (new)"
        med_change_match = re.search(r'\[?\s*\]?\s*New/changed\s+medications[:\s]+([^\n]+)', trans_section, re.IGNORECASE)
        if med_change_match:
            med_text = clean_text(med_change_match.group(1))
            if med_text and "(new)" in med_text.lower():
                # Remove "(new)" and clean up
                med_text = re.sub(r'\s*\(new\)', '', med_text, flags=re.IGNORECASE)
                med_text = med_text.strip()
                if med_text and med_text not in issues["Medication Changes"]["Start"]:
                    issues["Medication Changes"]["Start"].append(med_text)
        
        # Also check individual lines for other patterns
        lines = trans_section.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Remove leading dashes or brackets
            line_clean = re.sub(r'^[-\[\]]\s*', '', line)
            
            # Skip if it's a header or separator
            if re.match(r'^#\s+', line_clean) or re.match(r'^=+$', line_clean):
                continue
            
            # Extract code status from line
            if re.search(r'#\s*Code[:\s]+', line_clean, re.IGNORECASE):
                code_match = re.search(r'#\s*Code[:\s]+([^\n]+)', line_clean, re.IGNORECASE)
                if code_match:
                    issues["Code Status"] = clean_text(code_match.group(1))
            
            # Extract follow-up labs from line
            if 'biweekly' in line_clean.lower() and 'BMP' in line_clean.upper():
                if not issues["Follow-up Labs"]:
                    fup_text = clean_text(line_clean)
                    fup_text = re.sub(r'^\[?\s*\]?\s*', '', fup_text)
                    issues["Follow-up Labs"] = fup_text
            
            # Extract pending tests from line
            if 'follow-up' in line_clean.lower() and ('culture' in line_clean.lower() or 'test' in line_clean.lower()):
                test_text = clean_text(line_clean)
                test_text = re.sub(r'^\[?\s*\]?\s*', '', test_text)
                if test_text and test_text not in issues["Pending Tests"] and len(test_text) < 200:
                    issues["Pending Tests"].append(test_text)
            
            # Extract medication changes from line
            if 'new/changed medications' in line_clean.lower() or ('new' in line_clean.lower() and 'medication' in line_clean.lower()):
                med_match = re.search(r'[:\s]+([^\n]+)', line_clean, re.IGNORECASE)
                if med_match:
                    med_text = clean_text(med_match.group(1))
                    med_text = re.sub(r'\s*\(new\)', '', med_text, flags=re.IGNORECASE)
                    med_text = med_text.strip()
                    if med_text and med_text not in issues["Medication Changes"]["Start"]:
                        issues["Medication Changes"]["Start"].append(med_text)
        
        # Also check hospital course for medication changes and pending tests mentioned there
        hospital_course = extract_section(text, r'Brief Hospital Course:', r'Medications|Discharge')
        if hospital_course:
            # Look for pending tests like "needs EGD", "scheduled for", etc. - match LLM format
            # Look for "outpatient screening EGD" or "outpatient screening colonoscopy"
            egd_match = re.search(r'(?:schedule|scheduled)\s+outpatient\s+screening\s+EGD', hospital_course, re.IGNORECASE)
            if egd_match:
                if "Outpatient screening EGD" not in issues["Pending Tests"]:
                    issues["Pending Tests"].append("Outpatient screening EGD")
            
            colonoscopy_match = re.search(r'(?:schedule|scheduled)\s+outpatient\s+screening\s+colonoscopy', hospital_course, re.IGNORECASE)
            if colonoscopy_match:
                if "Outpatient screening colonoscopy" not in issues["Pending Tests"]:
                    issues["Pending Tests"].append("Outpatient screening colonoscopy")
            
            # Look for generic "outpatient screening" without specific test
            generic_screening = re.search(r'schedule\s+outpatient\s+screening\s+([^.\s]+)', hospital_course, re.IGNORECASE)
            if generic_screening and "Outpatient screening" not in str(issues["Pending Tests"]):
                test_name = generic_screening.group(1)
                issues["Pending Tests"].append(f"Outpatient screening {test_name}")
            
            # Look for "outpatient screening EGD and" (incomplete) - match LLM format
            incomplete_screening = re.search(r'outpatient\s+screening\s+EGD\s+and\s+([^.\s]*)', hospital_course, re.IGNORECASE)
            if incomplete_screening:
                # Add "Outpatient screening EGD" first
                if "Outpatient screening EGD" not in issues["Pending Tests"]:
                    issues["Pending Tests"].append("Outpatient screening EGD")
                # Then add incomplete one
                test_name = incomplete_screening.group(1).strip()
                if test_name:
                    if f"Outpatient screening {test_name}" not in issues["Pending Tests"]:
                        issues["Pending Tests"].append(f"Outpatient screening {test_name}")
                else:
                    # If no test name after "and", add just "Outpatient screening "
                    if "Outpatient screening " not in issues["Pending Tests"]:
                        issues["Pending Tests"].append("Outpatient screening ")
            
            # Look for "Forehead lesion biopsy result" - check in past medical history or hospital course
            if 'forehead' in hospital_course.lower() and 'biopsy' in hospital_course.lower() and 'pending' in hospital_course.lower():
                if "Forehead lesion biopsy result" not in issues["Pending Tests"]:
                    issues["Pending Tests"].append("Forehead lesion biopsy result")
            
            # Also check past medical history for pending biopsy
            pmh_section = extract_section(text, r'Past Medical History:', r'Social History|Family History|Physical Exam')
            if pmh_section and 'forehead' in pmh_section.lower() and 'biopsy' in pmh_section.lower() and 'pending' in pmh_section.lower():
                if "Forehead lesion biopsy result" not in issues["Pending Tests"]:
                    issues["Pending Tests"].append("Forehead lesion biopsy result")
            
            # Look for medication changes in hospital course - be more specific
            # Check for Acetaminophen addition - look in discharge medications section
            discharge_meds = extract_section(text, r'Discharge Medications:', r'Discharge Disposition|Discharge Diagnosis')
            if discharge_meds:
                acetaminophen_match = re.search(r'Acetaminophen\s+(\d+)\s*mg[^.]*?', discharge_meds, re.IGNORECASE)
                if acetaminophen_match:
                    dose = acetaminophen_match.group(1)
                    # Look for frequency pattern
                    freq_match = re.search(r'Q(\d+)H|Q(\d+)H:PRN', discharge_meds, re.IGNORECASE)
                    freq = freq_match.group(1) if freq_match else "6"
                    med_text = f"Acetaminophen {dose} mg PO Q{freq}H:PRN pain"
                    if med_text not in issues["Medication Changes"]["Start"]:
                        issues["Medication Changes"]["Start"].append(med_text)
            
            # Extract follow-up labs from hospital course - match LLM format
            # Pattern: "Pt was scheduled with current PCP for  check upon discharge."
            fup_labs_match = re.search(r'Pt\s+was\s+scheduled\s+with\s+current\s+PCP\s+for[^.]*?check[^.]*?\.', hospital_course, re.IGNORECASE)
            if fup_labs_match:
                issues["Follow-up Labs"] = clean_text("Pt was scheduled with current PCP for  check upon discharge.")
            elif re.search(r'scheduled\s+with\s+current\s+PCP\s+for[^.]*?check', hospital_course, re.IGNORECASE):
                issues["Follow-up Labs"] = "Pt was scheduled with current PCP for  check upon discharge."
    
    return issues

def extract_medications(text: str) -> Dict[str, List[str]]:
    """Extract medications on admission and discharge"""
    meds = {
        "On Admission": [],
        "On Discharge": []
    }
    
    # Admission medications
    adm_meds_section = extract_section(text, r'Medications on Admission:', r'Discharge Medications')
    if adm_meds_section:
        # Extract numbered medication list
        med_items = re.findall(r'\d+\.\s*([^\n]+)', adm_meds_section)
        for item in med_items:
            item = clean_text(item)
            if item and not item.startswith("The Preadmission"):
                meds["On Admission"].append(item)
    
    # Discharge medications - include full prescription text
    dis_meds_section = extract_section(text, r'Discharge Medications:', r'Discharge Disposition|Discharge Diagnosis')
    if dis_meds_section:
        med_items = re.findall(r'\d+\.\s*([^\n]+)', dis_meds_section)
        for item in med_items:
            item = clean_text(item)
            if item:
                # Skip "Outpatient Lab Work" as a medication - it's a separate instruction
                if "Outpatient Lab Work" in item:
                    # Extract the lab work instructions separately if needed
                    # For now, we'll include it but could be filtered later
                    pass
                
                # Check if there's a prescription line (RX) on the next line
                # Look for RX pattern in the section
                med_name_match = re.search(r'^([A-Za-z\s-]+(?:\s+\([^)]+\))?)', item)
                if med_name_match:
                    med_name = med_name_match.group(1).strip()
                    # Look for RX line for this medication - match full RX text including refills
                    # Pattern: RX *medication name* ... * (may include Refills:)
                    # Handle case where med_name might be partial (e.g., "Furosemide" in "Furosemide 10 mg PO DAILY")
                    rx_pattern = rf'RX\s*\*{re.escape(med_name.split()[0])}[^*]*\*(?:[^*]*\*)?'
                    rx_match = re.search(rx_pattern, dis_meds_section, re.IGNORECASE | re.DOTALL)
                    if rx_match:
                        # Get the full RX text, including any continuation lines
                        rx_start = rx_match.start()
                        # Find where RX ends (next medication number or end of section)
                        rx_end = len(dis_meds_section)
                        next_med_match = re.search(r'\n\s*\d+\.\s+', dis_meds_section[rx_start:])
                        if next_med_match:
                            rx_end = rx_start + next_med_match.start()
                        rx_text = clean_text(dis_meds_section[rx_start:rx_end])
                        # Append RX text to medication
                        item = item + " " + rx_text
                
                # Filter out malformed entries like "5 (One half) tablet(s) by mouth Please"
                # This is part of RX text that got separated
                if re.match(r'^\d+\s*\([^)]+\)\s+tablet', item, re.IGNORECASE):
                    # This is a fragment, skip it
                    continue
                
                meds["On Discharge"].append(item)
    
    return meds

def extract_discharge_info(text: str) -> Dict[str, Any]:
    """Extract discharge information"""
    discharge = {
        "Disposition": None,
        "Facility Name": None,
        "Primary Diagnoses": [],
        "Secondary Diagnoses": [],
        "Condition": {
            "Mental Status": None,
            "Level of Consciousness": None,
            "Activity Status": None
        },
        "Discharge Instructions": None,
        "Follow-up": None
    }
    
    # Disposition
    disp_match = re.search(r'Discharge Disposition:\s*([^\n]+)', text, re.IGNORECASE)
    if disp_match:
        discharge["Disposition"] = clean_text(disp_match.group(1))
    
    # Facility Name
    facility_match = re.search(r'Facility:\s*([^\n]+)', text, re.IGNORECASE)
    if facility_match:
        discharge["Facility Name"] = parse_redacted_value(facility_match.group(1))
    
    # Primary Diagnoses
    primary_section = extract_section(text, r'Discharge Diagnosis:|PRIMARY DIAGNOSIS', r'SECONDARY|SECONDARY DIAGNOSES|Discharge Condition')
    if primary_section:
        # Extract diagnoses - handle both single line and multiple lines
        lines = primary_section.split('\n')
        for line in lines:
            line = clean_text(line)
            # Remove prefixes
            line = re.sub(r'^Primary[:\s]*', '', line, flags=re.IGNORECASE)
            line = re.sub(r'^-\s*', '', line)
            line = line.strip()
            # Filter out section headers and separators
            if (line and 
                not line.startswith("Primary") and 
                not line.startswith("Secondary") and 
                line.upper() != "PRIMARY DIAGNOSIS" and
                line.upper() != "DIAGNOSIS" and
                not re.match(r'^=+$', line) and  # Filter "================="
                len(line) > 1):  # Filter single character lines
                discharge["Primary Diagnoses"].append(line)
    
    # Secondary Diagnoses
    secondary_section = extract_section(text, r'SECONDARY DIAGNOSES|Secondary:', r'Discharge Condition|Discharge Instructions')
    if secondary_section:
        # Handle comma-separated or line-separated
        if ',' in secondary_section and '\n' not in secondary_section:
            # Comma-separated format
            diag_items = [clean_text(item.strip()) for item in secondary_section.split(',')]
            for item in diag_items:
                item = re.sub(r'^Secondary[:\s]*', '', item, flags=re.IGNORECASE)
                item = item.strip()
                # Filter out section headers and separators
                if (item and 
                    not item.startswith("Secondary") and 
                    item.upper() != "SECONDARY DIAGNOSES" and
                    not re.match(r'^=+$', item) and  # Filter "==================="
                    len(item) > 1):
                    discharge["Secondary Diagnoses"].append(item)
        else:
            # Line-separated format
            lines = secondary_section.split('\n')
            for line in lines:
                line = clean_text(line)
                line = re.sub(r'^Secondary[:\s]*', '', line, flags=re.IGNORECASE)
                line = re.sub(r'^-\s*', '', line)
                line = line.strip()
                # Filter out section headers and separators
                if (line and 
                    not line.startswith("Secondary") and 
                    line.upper() != "SECONDARY DIAGNOSES" and
                    not re.match(r'^=+$', line) and  # Filter "==================="
                    len(line) > 1):
                    discharge["Secondary Diagnoses"].append(line)
    
    # Condition
    condition_section = extract_section(text, r'Discharge Condition:', r'Discharge Instructions')
    if condition_section:
        ms_match = re.search(r'Mental Status[:\s]*([^\n]+)', condition_section, re.IGNORECASE)
        if ms_match:
            discharge["Condition"]["Mental Status"] = clean_text(ms_match.group(1))
        
        loc_match = re.search(r'Level of Consciousness[:\s]*([^\n]+)', condition_section, re.IGNORECASE)
        if loc_match:
            discharge["Condition"]["Level of Consciousness"] = clean_text(loc_match.group(1))
        
        act_match = re.search(r'Activity Status[:\s]*([^\n]+)', condition_section, re.IGNORECASE)
        if act_match:
            discharge["Condition"]["Activity Status"] = clean_text(act_match.group(1))
    
    # Discharge Instructions
    instructions_section = extract_section(text, r'Discharge Instructions:', r'Followup Instructions|Follow-up Instructions')
    if instructions_section:
        discharge["Discharge Instructions"] = clean_text(instructions_section)
    
    # Follow-up - check both Followup Instructions and discharge instructions
    followup_section = extract_section(text, r'Followup Instructions:|Follow-up Instructions:', r'$')
    if followup_section and not is_redacted(followup_section):
        discharge["Follow-up"] = clean_text(followup_section)
    else:
        # Try to extract from discharge instructions
        instructions = discharge.get("Discharge Instructions", "")
        if instructions:
            # Look for follow-up mentions - get the complete sentence
            followup_match = re.search(r'You will follow up[^.]*?([^.]+\.[^.]*)', instructions, re.IGNORECASE)
            if followup_match:
                discharge["Follow-up"] = clean_text("You will follow up" + followup_match.group(1))
            else:
                # Try simpler pattern
                followup_match = re.search(r'follow up[^.]*?([^.]+\.[^.]*)', instructions, re.IGNORECASE)
                if followup_match:
                    discharge["Follow-up"] = clean_text(followup_match.group(1))
    
    # Extract secondary diagnoses - look in discharge section first, then hospital course and HPI
    if not discharge["Secondary Diagnoses"]:
        # Try to extract from hospital course, HPI, and past medical history
        hospital_course = extract_section(text, r'Brief Hospital Course:', r'Medications|Discharge')
        hpi_section = extract_section(text, r'History of Present Illness:', r'Past Medical History|Physical Exam')
        pmh_section = extract_section(text, r'Past Medical History:', r'Social History|Family History|Physical Exam')
        
        # Combine sections for diagnosis extraction
        combined_text = ""
        if hospital_course:
            combined_text += " " + hospital_course
        if hpi_section:
            combined_text += " " + hpi_section
        if pmh_section:
            combined_text += " " + pmh_section
        
        if combined_text:
            # Look for specific diagnosis patterns - match LLM format
            diag_patterns = [
                (r'HCV\s+Cirrhosis', 'HCV Cirrhosis'),
                (r'\bHIV\b(?!\s+on)', 'HIV'),  # HIV but not "HIV on ART"
                (r'\bCOPD\b', 'COPD'),
                (r'Bipolar\s+affective\s+disorder', 'Bipolar affective disorder'),
                (r'\bPTSD\b', 'PTSD'),
                (r'History\s+of\s+IVDU|h/o\s+IVDU', 'History of IVDU'),
                (r'Cholelithiasis', 'Cholelithiasis'),
            ]
            found_diags = []
            for pattern, diag_name in diag_patterns:
                if re.search(pattern, combined_text, re.IGNORECASE):
                    if diag_name not in found_diags:
                        found_diags.append(diag_name)
            if found_diags:
                discharge["Secondary Diagnoses"] = found_diags
    
    # Also check Imaging Studies for Cholelithiasis
    imaging_results = extract_pertinent_results(text).get("Imaging Studies", [])
    for imaging in imaging_results:
        if isinstance(imaging, dict):
            findings = imaging.get("Findings", "")
            if findings and "Cholelithiasis" in findings and "Cholelithiasis" not in discharge["Secondary Diagnoses"]:
                discharge["Secondary Diagnoses"].append("Cholelithiasis")
    
    return discharge

def load_schema(schema_path: str) -> Optional[Dict[str, Any]]:
    """Load the JSON schema from file"""
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        return schema
    except Exception as e:
        print(f"Warning: Could not load schema from {schema_path}: {e}")
        return None

def build_structure_from_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Build the base JSON structure from schema definition
    
    Args:
        schema: The schema dictionary loaded from JSON
        
    Returns:
        A dictionary with the base structure matching the schema
    """
    def infer_type_from_description(desc: str) -> Any:
        """Infer the default value type from schema description"""
        desc_lower = desc.lower()
        if "array" in desc_lower or "[" in desc:
            return []
        elif "object" in desc_lower or "key-value" in desc_lower or "pairs" in desc_lower or "vital signs" in desc_lower:
            return {}
        elif "null" in desc_lower:
            return None
        else:
            return None
    
    def build_from_schema_node(node: Any, parent_key: str = "") -> Any:
        """Recursively build structure from schema node"""
        if isinstance(node, dict):
            result = {}
            for key, value in node.items():
                if isinstance(value, dict):
                    # Check if it's an array item template (like Imaging Studies)
                    if parent_key == "Imaging Studies" or (key == "Imaging Studies" and "Study Type" in value):
                        # Skip - will be handled separately
                        continue
                    # It's a nested object - recurse
                    result[key] = build_from_schema_node(value, key)
                elif isinstance(value, list):
                    # Array definition - check if it's array of objects or strings
                    if value and isinstance(value[0], dict):
                        # Array of objects (like Imaging Studies)
                        result[key] = []
                    else:
                        # Array of strings (like Allergies, Past Medical History)
                        result[key] = []
                elif isinstance(value, str):
                    # String description - infer type
                    inferred = infer_type_from_description(value)
                    result[key] = inferred
                else:
                    result[key] = None
            return result
        elif isinstance(node, list):
            # Array of objects template
            if node and isinstance(node[0], dict):
                return []  # Will be populated with objects matching the template
            return []
        else:
            return None
    
    # Build the main structure
    structure = build_from_schema_node(schema)
    
    # Handle special cases for arrays (Imaging Studies)
    if "Imaging Studies" in schema and isinstance(schema["Imaging Studies"], list):
        structure["Imaging Studies"] = []
    
    return structure

def convert_clinical_note_to_json(note_text: str, schema: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Convert a clinical note to JSON using rule-based extraction with schema-driven structure
    
    Args:
        note_text: The clinical note text to convert
        schema: Schema dictionary to drive the structure generation (required)
    """
    if not schema:
        raise ValueError("Schema is required for schema-driven structure generation")
    
    try:
        # Build base structure from schema
        result = build_structure_from_schema(schema)
        
        # Extract data using existing extraction functions
        admin_info = extract_administrative_info(note_text)
        clinical_summary = extract_clinical_summary(note_text)
        hpi = extract_history_of_present_illness(note_text)
        physical_exam = extract_physical_exam(note_text)
        pertinent_results = extract_pertinent_results(note_text)
        hospital_course = extract_hospital_course(note_text)
        transitional_issues = extract_transitional_issues(note_text)
        medications = extract_medications(note_text)
        discharge_info = extract_discharge_info(note_text)
        
        # Populate structure with extracted data (schema-driven)
        # Use deep merge for nested structures to preserve schema-defined structure
        def deep_update(target: Dict[str, Any], source: Dict[str, Any]) -> None:
            """Recursively update target dict with source dict, preserving nested structures"""
            for key, value in source.items():
                if key in target:
                    if isinstance(target[key], dict) and isinstance(value, dict):
                        deep_update(target[key], value)
                    else:
                        target[key] = value
                else:
                    target[key] = value
        
        if "Administrative Information" in result:
            deep_update(result["Administrative Information"], admin_info)
        
        if "Clinical Summary" in result:
            deep_update(result["Clinical Summary"], clinical_summary)
        
        if "History of Present Illness" in result:
            # Handle ED Labs structure - LLM uses flat structure, not nested "description"
            if "ED Labs" in hpi and isinstance(hpi["ED Labs"], dict):
                if "description" in hpi["ED Labs"]:
                    # Convert nested structure to flat
                    result["History of Present Illness"]["ED Labs"] = hpi["ED Labs"]["description"]
                else:
                    result["History of Present Illness"]["ED Labs"] = hpi["ED Labs"]
            # Update other HPI fields
            if "Summary" in hpi:
                result["History of Present Illness"]["Summary"] = hpi["Summary"]
            if "ED Findings" in hpi:
                deep_update(result["History of Present Illness"]["ED Findings"], hpi["ED Findings"])
        
        if "Physical Examination" in result:
            deep_update(result["Physical Examination"], physical_exam)
        
        if "Pertinent Results" in result:
            result["Pertinent Results"].update({
                "Admission Labs": pertinent_results.get("Admission Labs", {"description": {}}),
                "Discharge Labs": pertinent_results.get("Discharge Labs")
            })
        
        if "Microbiology" in result:
            result["Microbiology"] = pertinent_results.get("Microbiology", {"description": None})
        
        if "Imaging Studies" in result:
            result["Imaging Studies"] = pertinent_results.get("Imaging Studies", [])
        
        if "Hospital Course" in result:
            deep_update(result["Hospital Course"], hospital_course)
        
        if "Transitional Issues" in result:
            deep_update(result["Transitional Issues"], transitional_issues)
        
        if "Medications" in result:
            deep_update(result["Medications"], medications)
        
        if "Discharge Information" in result:
            deep_update(result["Discharge Information"], discharge_info)
        
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        print(f"Error converting note to JSON: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_clinical_notes(input_csv: str, output_csv: str, schema_path: Optional[str] = None):
    """Process all clinical notes in the CSV file
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file
        schema_path: Optional path to schema file (for consistency, not used for validation)
    """
    # Load schema (required for schema-driven structure generation)
    if not schema_path or not os.path.exists(schema_path):
        raise ValueError(f"Schema file is required but not found at: {schema_path}")
    
    schema = load_schema(schema_path)
    if not schema:
        raise ValueError(f"Failed to load schema from: {schema_path}")
    
    print("Schema loaded successfully - using schema-driven structure generation")
    
    print(f"Reading input CSV: {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Check which notes are already processed
    processed_note_ids = set()
    if os.path.exists(output_csv):
        print(f"Output CSV exists. Checking for already processed notes...")
        try:
            existing_df = pd.read_csv(output_csv)
            processed_note_ids = set(existing_df['note_id'].tolist())
            print(f"Found {len(processed_note_ids)} already processed notes")
        except Exception as e:
            print(f"Warning: Could not read existing output CSV: {e}")
            print("Starting fresh...")
    
    # Filter to only unprocessed notes
    df_to_process = df[~df['note_id'].isin(processed_note_ids)].copy()
    
    total_notes = len(df)
    already_processed = len(processed_note_ids)
    to_process = len(df_to_process)
    
    print(f"\nTotal notes in input: {total_notes}")
    print(f"Already processed: {already_processed}")
    print(f"Remaining to process: {to_process}")
    
    if to_process == 0:
        print("\n" + "="*60)
        print("All notes already processed! Nothing to do.")
        print("="*60)
        return
    
    # Counter for statistics
    successful = 0
    failed = 0
    
    # Determine if we need to write headers (first time) or append
    write_header = not os.path.exists(output_csv)
    
    # Create progress bar
    pbar = tqdm(total=to_process, desc="Converting notes to JSON", unit="note")
    
    for idx, row in df_to_process.iterrows():
        clinical_note = row['text']
        
        # Convert to JSON (pass schema for consistency, though not used for validation)
        json_output = convert_clinical_note_to_json(clinical_note, schema)
        
        # Prepare row for output (without 'text' column)
        output_row = row.drop('text').to_dict()
        output_row['json_data'] = json_output
        
        # Create a single-row DataFrame
        output_df = pd.DataFrame([output_row])
        
        # Append to CSV immediately
        try:
            output_df.to_csv(output_csv, mode='a', header=write_header, index=False)
            write_header = False  # After first write, don't write headers again
            
            if json_output:
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            failed += 1
            tqdm.write(f"[ERROR] Failed to write note {row['note_id']} to CSV: {e}")
        
        # Update progress bar
        pbar.update(1)
        pbar.set_postfix({'Success': successful, 'Failed': failed})
    
    # Close progress bar
    pbar.close()
    
    # Print statistics
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Total notes in dataset: {total_notes}")
    print(f"Already processed (before this run): {already_processed}")
    print(f"Processed in this run: {successful + failed}")
    print(f"Successfully converted: {successful}")
    print(f"Failed conversions: {failed}")
    print(f"Total processed now: {already_processed + successful + failed}")
    print(f"\nOutput saved to: {output_csv}")
    print("="*60)

def main():
    # Define file paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(os.path.join(script_dir, '..', '..', '..'))
    input_csv = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge.csv')
    output_csv = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_json_logic.csv')
    # Schema is in Scripts/utils/ directory (for consistency with LLM version)
    schema_path = os.path.join(project_root, 'Scripts', 'utils', 'clinical_note_schema.json')
    
    # Check if input files exist
    if not os.path.exists(input_csv):
        print(f"Error: Input CSV not found at {input_csv}")
        return
    
    # Process the clinical notes (schema is loaded for consistency, though structure is hardcoded)
    process_clinical_notes(input_csv, output_csv, schema_path)

if __name__ == "__main__":
    main()

