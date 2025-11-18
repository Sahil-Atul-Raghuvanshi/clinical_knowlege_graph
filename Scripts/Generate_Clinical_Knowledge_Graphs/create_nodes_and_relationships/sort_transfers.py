"""
Sort transfers.csv by intime and save to a new CSV file.
"""

import pandas as pd
import os
from pathlib import Path

# Get the project root directory (assuming script is in Scripts/Generate_Clinical_Knowledge_Graphs/create_nodes_and_relationships/)
script_dir = Path(__file__).parent
project_root = script_dir.parent.parent.parent

# Define input and output file paths
input_file = project_root / "Filtered_Data" / "hosp" / "transfers.csv"
output_file = project_root / "Filtered_Data" / "hosp" / "transfers_sorted.csv"

print(f"Reading transfers from: {input_file}")

# Read the CSV file
df = pd.read_csv(input_file)

print(f"Original file has {len(df)} rows")

# Convert intime to datetime for proper sorting
df['intime'] = pd.to_datetime(df['intime'], errors='coerce')

# Sort by intime (ascending - earliest first)
df_sorted = df.sort_values(by='intime', na_position='last').reset_index(drop=True)

# Convert intime back to string format for CSV output (preserve original format)
df_sorted['intime'] = df_sorted['intime'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Save to new CSV file
df_sorted.to_csv(output_file, index=False)

print(f"Sorted transfers saved to: {output_file}")
print(f"Sorted file has {len(df_sorted)} rows")
print(f"\nFirst 5 rows (sorted by intime):")
print(df_sorted.head().to_string())
print(f"\nLast 5 rows (sorted by intime):")
print(df_sorted.tail().to_string())

