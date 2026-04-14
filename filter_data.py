import pandas as pd
import os
import shutil
from pathlib import Path


def create_directory_structure():
    """Create the same directory structure in Filtered_Data as in Complete_Data."""
    base_path = Path("Filtered_Data")
    for dir_name in ["hosp", "icu", "ed", "note"]:
        dir_path = base_path / dir_name
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {dir_path}")


def load_subject_ids(csv_file: str) -> list[int]:
    """Load subject_ids from 1000_patients.csv."""
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Patient file not found: {csv_file}")

    df = pd.read_csv(csv_file)
    subject_ids = df["subject_id"].dropna().astype(int).unique().tolist()
    print(f"Loaded {len(subject_ids):,} unique subject_ids from {csv_file}")
    return subject_ids


def derive_hadm_ids(subject_ids: list[int], admissions_file: str) -> list[int]:
    """Derive hadm_ids from admissions.csv for the given subject_ids."""
    if not os.path.exists(admissions_file):
        raise FileNotFoundError(f"Admissions file not found: {admissions_file}")

    df = pd.read_csv(admissions_file, usecols=["subject_id", "hadm_id"])
    filtered = df[df["subject_id"].isin(subject_ids)]
    hadm_ids = filtered["hadm_id"].dropna().astype(int).unique().tolist()
    print(f"Derived {len(hadm_ids):,} unique hadm_ids from {admissions_file}")
    return hadm_ids


def filter_csv_chunked(
    input_file: str,
    output_file: str,
    subject_ids: list[int],
    hadm_ids: list[int],
    chunk_size: int = 10_000,
) -> bool:
    """
    Filter a CSV file by hadm_id (preferred) or subject_id using chunked reading.
    If neither column exists, the file is copied as-is (lookup/dictionary tables).
    """
    try:
        print(f"\nProcessing: {input_file}")

        if not os.path.exists(input_file):
            print(f"  [WARNING] File not found: {input_file}")
            return False

        file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
        print(f"  File size: {file_size_mb:.1f} MB")

        first_chunk = pd.read_csv(input_file, nrows=1)
        has_subject_id = "subject_id" in first_chunk.columns
        has_hadm_id = "hadm_id" in first_chunk.columns

        if not has_subject_id and not has_hadm_id:
            shutil.copy2(input_file, output_file)
            print(f"  [SUCCESS] Copied dictionary file (no subject_id/hadm_id column)")
            return True

        filter_column = "hadm_id" if has_hadm_id else "subject_id"
        filter_ids = hadm_ids if has_hadm_id else subject_ids

        total_rows = 0
        filtered_rows = 0
        header_written = False

        for chunk in pd.read_csv(input_file, chunksize=chunk_size):
            total_rows += len(chunk)
            filtered_chunk = chunk[chunk[filter_column].isin(filter_ids)]

            if not filtered_chunk.empty:
                mode = "w" if not header_written else "a"
                filtered_chunk.to_csv(output_file, index=False, mode=mode, header=not header_written)
                header_written = True
                filtered_rows += len(filtered_chunk)

            if total_rows % (chunk_size * 10) == 0:
                print(f"  Processed {total_rows:,} rows... (filtered: {filtered_rows:,})")

        if filtered_rows > 0:
            print(f"  [SUCCESS] Filtered {filtered_rows:,} / {total_rows:,} rows (using {filter_column})")
        else:
            first_chunk.iloc[0:0].to_csv(output_file, index=False)
            print(f"  [SUCCESS] No matching rows found in {total_rows:,} rows (using {filter_column})")

        return True

    except Exception as e:
        print(f"  [ERROR] {input_file}: {e}")
        return False


def main():
    patients_file = "1000_patients.csv"
    admissions_file = os.path.join("Complete_Data", "hosp", "admissions.csv")

    subject_ids = load_subject_ids(patients_file)
    hadm_ids = derive_hadm_ids(subject_ids, admissions_file)

    print(f"\n{'='*80}")
    print("FILTERING DATA FOR 1000 PATIENTS")
    print(f"  Source      : Complete_Data/")
    print(f"  Destination : Filtered_Data/")
    print(f"  subject_ids : {len(subject_ids):,}")
    print(f"  hadm_ids    : {len(hadm_ids):,}")
    print(f"{'='*80}\n")

    create_directory_structure()

    files_to_process = {
        os.path.join("Complete_Data", "hosp"): [
            "patients.csv",
            "admissions.csv",
            "transfers.csv",
            "services.csv",
            "prescriptions.csv",
            "microbiologyevents.csv",
            "drgcodes.csv",
            "labevents.csv",
            "d_labitems.csv",
            "diagnoses_icd.csv",
            "d_icd_diagnoses.csv",
            "procedures_icd.csv",
            "d_icd_procedures.csv",
            "omr.csv",
            "pharmacy.csv",
            "poe.csv",
            "poe_detail.csv",
            "emar.csv",
            "emar_detail.csv",
            "hcpcsevents.csv",
            "d_hcpcs.csv",
            "provider.csv",
        ],
        os.path.join("Complete_Data", "icu"): [
            "icustays.csv",
            "procedureevents.csv",
            "chartevents.csv",
            "outputevents.csv",
            "datetimeevents.csv",
            "inputevents.csv",
            "ingredientevents.csv",
            "d_items.csv",
            "caregiver.csv",
        ],
        os.path.join("Complete_Data", "ed"): [
            "edstays.csv",
            "medrecon.csv",
            "pyxis.csv",
            "triage.csv",
            "diagnosis.csv",
            "vitalsign.csv",
        ],
        os.path.join("Complete_Data", "note"): [
            "discharge.csv",
        ],
    }

    total_processed = 0
    total_successful = 0

    for source_dir, csv_files in files_to_process.items():
        target_dir = source_dir.replace("Complete_Data", "Filtered_Data")

        print(f"\nDirectory: {source_dir}")
        print("-" * 60)

        if not os.path.exists(source_dir):
            print(f"  Source directory not found, skipping.")
            continue

        for csv_file in csv_files:
            input_path = os.path.join(source_dir, csv_file)
            output_path = os.path.join(target_dir, csv_file)

            if not os.path.exists(input_path):
                print(f"  [SKIP] {csv_file} not found in source")
                continue

            total_processed += 1
            if filter_csv_chunked(input_path, output_path, subject_ids, hadm_ids):
                total_successful += 1

    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"  Patients      : {len(subject_ids):,} subject_ids | {len(hadm_ids):,} hadm_ids")
    print(f"  Processed     : {total_processed}")
    print(f"  Successful    : {total_successful}")
    print(f"  Failed        : {total_processed - total_successful}")
    print(f"  Output        : Filtered_Data/")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
