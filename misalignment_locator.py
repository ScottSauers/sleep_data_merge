import os
import pandas as pd

def check_mapid_all_consistency(df, file_path, errors):
    columns_to_check = [
        "ID_superset", "dem_mapid_mod_b4_cdr", "ID_mod_b4_cdr",
        "dem_mapid_mod_csf_markers", "dem_mapid_mod_barthelemy_tau_species",
        "ID_mod_barthelemy_tau_species", "ID_mod_psychometrics",
        "ID_mod_demographics", "ID_mod_apoe", "mapid_superset",
        "dem_mapid_sleep", "dem_mapid_eeg"]

    if 'mapid_all' not in df.columns:
        print(f"'mapid_all' column not found in file '{file_path}', skipping mapid_all consistency check.")
        return

    for index, row in df.iterrows():
        mapid_all = row['mapid_all']
        if not pd.isna(mapid_all) and pd.to_numeric(mapid_all, errors='coerce') is not None:
            for col in columns_to_check:
                if col in df.columns and pd.notna(row[col]) and row[col] != mapid_all:
                    error_message = f"Inconsistent value in file '{file_path}' at row {index + 1}: mapid_all={mapid_all} but {col}={row[col]}"
                    errors.append(error_message)

def check_sid_consistency(df, file_path, errors):
    sid_columns = ["dem_sid_y", "StudyID", "dem_sid"]
    for index, row in df.iterrows():
        values = {row[col] for col in sid_columns if col in df.columns and pd.notna(row[col])}
        if len(values) > 1:
            error_message = f"Mismatch in SID values in file '{file_path}' at row {index + 1}: {values}"
            errors.append(error_message)

def process_csv_files(folder_path, errors):
    csv_found = False

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                csv_found = True
                file_path = os.path.join(root, file)
                print(f"Processing file: {file_path}")
                df = pd.read_csv(file_path)
                check_mapid_all_consistency(df, file_path, errors)
                check_sid_consistency(df, file_path, errors)
                if file_path not in [error.split(":")[0] for error in errors]:
                    print(f"File {file_path} passed all checks successfully.")
    if not csv_found:
        raise FileNotFoundError(f"No CSV files found in folder {folder_path}")

def main():
    base_folders = ['/Users/scott/Downloads/merger/combine/visit_1', '/Users/scott/Downloads/merger/combine/visit_2', '/Users/scott/Downloads/merger/combine/visit_3']
    errors = []
    for folder in base_folders:
        print(f"Checking folder: {folder}")
        try:
            process_csv_files(folder, errors)
        except FileNotFoundError as e:
            print(e)
    if errors:
        print("\nSummary of all failures:")
        for error in errors:
            print(error)
    else:
        print("All checks passed successfully in all files.")
    print("All checks completed.")