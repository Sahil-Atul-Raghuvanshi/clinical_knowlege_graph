import pandas as pd
import json
import google.generativeai as genai
import os
from datetime import datetime
import time

# Configure Gemini API
# API key is hardcoded below (Note: For production, use environment variables)
API_KEY = 'AIzaSyDG9VKp-kD1y0-xT-5ivV7Ldni-YDR-wOk'

if not API_KEY:
    raise ValueError("Please set GEMINI_API_KEY environment variable")

genai.configure(api_key=API_KEY)

# Initialize the model
model = genai.GenerativeModel('gemini-2.5-pro')

def load_schema(schema_path):
    """Load the JSON schema from file"""
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema

def create_prompt(clinical_note, schema):
    """Create a prompt for Gemini to convert clinical note to JSON"""
    
    schema_str = json.dumps(schema, indent=2)
    
    prompt = f"""You are a medical data extraction and summarization expert. Your task is to extract and SUMMARIZE structured information from a clinical discharge note and convert it into a clean, organized JSON format.

**CRITICAL FORMATTING RULES:**
1. REMOVE all newline characters (\\n) - write text as single-line strings
2. REMOVE or replace all redacted placeholders (___) with appropriate values:
   - For names/identifiers: use null or empty string
   - For numbers/values: extract actual values if visible
3. SUMMARIZE long text blocks - DO NOT copy verbatim:
   - History of Present Illness: 2-3 sentence summary
   - Hospital Course: Brief summary for each problem (1-2 sentences each)
   - Physical exam: Keep concise descriptions
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
Provide clean, summarized JSON following the schema. Remember: NO newlines in strings, NO ___ placeholders, SUMMARIZE long text, extract clean values."""

    return prompt

def convert_clinical_note_to_json(note_text, schema, max_retries=3):
    """Convert a clinical note to JSON using Gemini API"""
    
    prompt = create_prompt(note_text, schema)
    
    for attempt in range(max_retries):
        try:
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
            print(f"Attempt {attempt + 1}/{max_retries} - Error: {e}")
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
    print(f"Output saved to: {output_csv}")
    print("="*60)

def main():
    # Define file paths
    input_csv = "Filtered_Data/HeartPatient/discharge_clinical_note.csv"
    output_csv = "Filtered_Data/HeartPatient/discharge_clinical_note_json.csv"
    schema_path = "Scripts/clinical_note_schema.json"
    
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

