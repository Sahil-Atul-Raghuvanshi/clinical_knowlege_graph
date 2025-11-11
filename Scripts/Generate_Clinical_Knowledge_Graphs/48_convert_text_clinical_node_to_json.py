"""
Clinical Note to JSON Converter with Multiple API Key Support

This script converts clinical discharge notes to structured JSON format using Google's Gemini API.

FEATURES:
- Automatic API key rotation when rate limits are hit
- Supports up to 3 API keys for extended processing
- Resume capability (skips already processed notes)
- Incremental saving (saves each note immediately)

HOW TO USE MULTIPLE API KEYS:
1. Replace 'YOUR_SECOND_API_KEY_HERE' and 'YOUR_THIRD_API_KEY_HERE' below with your actual keys
2. The script will automatically switch to the next key if one hits rate limits
3. You can use 1, 2, or 3 keys - the script adapts accordingly

API KEY ROTATION BEHAVIOR:
- Primary key is used first
- If rate limit is hit, automatically switches to next key
- Continues processing without interruption
- If all keys are exhausted, the script will stop and report status
"""

import pandas as pd
import json
import google.generativeai as genai
import os
from datetime import datetime
import time

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION: Add Your API Keys Here
# ═══════════════════════════════════════════════════════════════
# Add your API keys below - the script will rotate through them if one hits rate limits
API_KEYS = [
    'AIzaSyDG9VKp-kD1y0-xT-5ivV7Ldni-YDR-wOk',  # Primary key
    'AIzaSyDt0APSsM1N9G_7mpPhpJzNOB-8h3W4SNc',                    # Backup key 1
    'AIzaSyCtkc5BuyPggHUWhoz_iNToHVDCs6Pn3Pc',  
    'AIzaSyDE5Ck37g-cNIL9WCu_PIT91q1_ySuhNRY'                   # Backup key 2
]
# ═══════════════════════════════════════════════════════════════

# Filter out placeholder keys
API_KEYS = [key for key in API_KEYS if key and not key.startswith('YOUR_')]

if not API_KEYS:
    raise ValueError("Please provide at least one valid API key in the API_KEYS list")

print(f"Loaded {len(API_KEYS)} API key(s)")

# Global variable to track current API key index
current_key_index = 0

def get_current_model():
    """Get the model with the current API key"""
    global current_key_index
    genai.configure(api_key=API_KEYS[current_key_index])
    return genai.GenerativeModel('gemini-2.5-pro')

def switch_api_key():
    """Switch to the next available API key"""
    global current_key_index
    old_index = current_key_index
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    
    if current_key_index == old_index and len(API_KEYS) == 1:
        print(f"[WARNING] Only one API key available - cannot switch")
        return False
    
    print(f"[API KEY SWITCH] Switching from key #{old_index + 1} to key #{current_key_index + 1}")
    return True

def is_rate_limit_error(error):
    """Check if the error is a rate limit/quota error"""
    error_str = str(error).lower()
    rate_limit_indicators = [
        'quota',
        'rate limit',
        'resource exhausted',
        '429',
        'too many requests',
        'limit exceeded'
    ]
    return any(indicator in error_str for indicator in rate_limit_indicators)

def load_schema(schema_path):
    """Load the JSON schema from file"""
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema

def create_prompt(clinical_note, schema):
    """Create a prompt for Gemini to convert clinical note to JSON"""
    
    schema_str = json.dumps(schema, indent=2)
    
    prompt = f"""You are a medical data extraction expert. Your task is to extract structured information from a clinical discharge note and convert it into a clean, organized JSON format. Preserve all content from the original note.

**CRITICAL FORMATTING RULES:**
1. REMOVE all newline characters (\\n) - write text as single-line strings
2. REMOVE or replace all redacted placeholders (___) with appropriate values:
   - For names/identifiers: use empty string "" or null
   - For numbers/values: use empty string "" or extract actual values if visible nearby
3. PRESERVE all original text content - DO NOT summarize or shorten:
   - Extract complete text from all sections
   - Keep full descriptions and details as written
   - Maintain complete patient history, exam findings, and clinical narratives
4. CLEAN and structure the data:
   - Extract numerical values cleanly (e.g., "Temperature: 99" not "T:99")
   - Format medications as readable strings: "Aspirin 81 mg PO daily"
   - Organize labs as key-value pairs with clean names
5. Follow the JSON schema structure EXACTLY as provided
6. If a field is not present, use null
7. For dates, use ISO 8601 format (YYYY-MM-DD) when identifiable
8. Return ONLY valid JSON without markdown formatting or code blocks

**SCHEMA TO FOLLOW:**
{schema_str}

**CLINICAL NOTE TO PROCESS:**
{clinical_note}

**REQUIRED OUTPUT:**
Provide clean JSON following the schema with complete content preserved. Remember: NO newlines in strings, replace ___ with empty string "", PRESERVE all original text content without summarization."""

    return prompt

def convert_clinical_note_to_json(note_text, schema, max_retries=3):
    """Convert a clinical note to JSON using Gemini API with automatic key rotation"""
    
    prompt = create_prompt(note_text, schema)
    keys_tried = set()  # Track which keys we've tried for this note
    
    for attempt in range(max_retries):
        try:
            # Get current model with active API key
            model = get_current_model()
            print(f"[API KEY] Using key #{current_key_index + 1}")
            
            # Generate content using Gemini
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,  # Lower temperature for more consistent output
                    max_output_tokens=16384,  # Increased for large outputs
                ),
                safety_settings=[
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ]
            )
            
            # Check if response has valid content
            if not response.candidates:
                print(f"Attempt {attempt + 1}/{max_retries} - No candidates returned")
                time.sleep(8)
                continue
                
            candidate = response.candidates[0]
            if not hasattr(candidate, 'content') or not candidate.content.parts:
                print(f"Attempt {attempt + 1}/{max_retries} - Empty response (finish_reason: {candidate.finish_reason})")
                time.sleep(3)
                continue
            
            # Extract the text response
            json_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if json_text.startswith('```json'):
                json_text = json_text[7:]
            elif json_text.startswith('```'):
                json_text = json_text[3:]
            
            if json_text.endswith('```'):
                json_text = json_text[:-3]
            
            json_text = json_text.strip()
            
            # Parse JSON to validate it
            parsed_json = json.loads(json_text)
            
            # Return as JSON string
            return json.dumps(parsed_json, ensure_ascii=False)
            
        except json.JSONDecodeError as e:
            print(f"Attempt {attempt + 1}/{max_retries} - JSON parsing error: {e}")
            if attempt == max_retries - 1:
                print(f"Failed to parse JSON after {max_retries} attempts")
                print(f"Last response length: {len(json_text) if 'json_text' in locals() else 'N/A'}")
                return None
            time.sleep(3)  # Wait before retry
            
        except Exception as e:
            error_msg = str(e)
            print(f"Attempt {attempt + 1}/{max_retries} - Error: {error_msg}")
            
            # Check if it's a rate limit error
            if is_rate_limit_error(e):
                keys_tried.add(current_key_index)
                print(f"[RATE LIMIT] API key #{current_key_index + 1} hit rate limit")
                
                # Try switching to next key if available
                if len(keys_tried) < len(API_KEYS):
                    if switch_api_key():
                        print(f"[RETRY] Retrying with new API key...")
                        time.sleep(2)  # Short delay before retry
                        continue  # Retry immediately with new key
                    else:
                        print(f"[ERROR] All {len(API_KEYS)} API keys exhausted")
                        return None
                else:
                    print(f"[ERROR] All {len(API_KEYS)} API keys have been tried and hit rate limits")
                    return None
            
            # For non-rate-limit errors, continue with normal retry logic
            if attempt == max_retries - 1:
                print(f"Failed to process note after {max_retries} attempts")
                return None
            time.sleep(10)  # Wait before retry
    
    return None

def process_clinical_notes(input_csv, output_csv, schema_path):
    """Process all clinical notes in the CSV file"""
    
    print("Loading schema...")
    schema = load_schema(schema_path)
    
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
    
    for idx, row in df_to_process.iterrows():
        print(f"\nProcessing note {successful + failed + 1}/{to_process}")
        print(f"Note ID: {row['note_id']}")
        print(f"Subject ID: {row['subject_id']}")
        
        clinical_note = row['text']
        
        # Convert to JSON
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
                print(f"[SUCCESS] Successfully converted and saved note {row['note_id']}")
            else:
                failed += 1
                print(f"[FAILED] Failed to convert note {row['note_id']} (saved with null json_data)")
                
        except Exception as e:
            failed += 1
            print(f"[ERROR] Failed to write note {row['note_id']} to CSV: {e}")
        
        # Add a small delay to avoid rate limiting
        time.sleep(10)
    
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
    print(f"\nAPI Key Status:")
    print(f"  - Total API keys available: {len(API_KEYS)}")
    print(f"  - Final active key: #{current_key_index + 1}")
    if len(API_KEYS) > 1:
        print(f"  - Key rotation was {'ENABLED' if len(API_KEYS) > 1 else 'DISABLED'}")
    print(f"\nOutput saved to: {output_csv}")
    print("="*60)

def main():
    # Define file paths (relative to script location)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..', '..')
    input_csv = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge.csv')
    output_csv = os.path.join(project_root, 'Filtered_Data', 'note', 'discharge_clinical_note_json.csv')
    schema_path = os.path.join(script_dir, 'clinical_note_schema.json')
    
    # Check if input files exist
    if not os.path.exists(input_csv):
        print(f"Error: Input CSV not found at {input_csv}")
        return
    
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}")
        return
    
    # Process the clinical notes
    process_clinical_notes(input_csv, output_csv, schema_path)

if __name__ == "__main__":
    main()

