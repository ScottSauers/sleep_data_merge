import os
import pandas as pd

def merge_checks():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]

    # Merge steps file paths
    merge_steps = {
        "merge1": ["HASD_proj2_EEG_power_2024-0412.csv", "FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv"],
        "merge2": ["merge1.csv", "1-HASD_proj2_sleep_data_2024-0412.csv"],
        "merge3": [
            "mod_apoe.csv",
            "mod_demographics.csv",
            "mod_psychometrics.csv",
            "mod_barthelemy_tau_species.csv",
            "mod_csf_markers.csv",
            "mod_horie_mbtr_species.csv",
            "mod_c2n_plasma.csv",
            "fasted_precivityad.csv",
            "mod_b4_cdr.csv"],
        "merge4": ["merge2.csv", "merge3.csv"]}

    # Function to perform spot checks
    def perform_checks(df, merge_step):
        checks = []

        # Check 1: Verify that the DataFrame is not empty
        checks.append(("Not Empty", not df.empty, "DataFrame is empty"))

        # Check 2: Verify that columns are properly present by checking for at least one expected column
        if merge_step == "merge1":
            expected_columns = ["dem_sid", "StudyID"]
            checks.append(("Columns Exist", any(col in df.columns for col in expected_columns), 
                        f"None of the expected columns found: {expected_columns}"))
        elif merge_step == "merge2":
            expected_columns = ["dem_sid", "StudyID", "dem_mapid_eeg", "dem_mapid_sleep"]
            checks.append(("Columns Exist", any(col in df.columns for col in expected_columns), 
                        f"None of the expected columns found: {expected_columns}"))
        elif merge_step == "merge3":
            expected_columns = ["ID"]
            checks.append(("Columns Exist", any(col in df.columns for col in expected_columns), 
                        f"None of the expected columns found: {expected_columns}"))
        elif merge_step == "merge4":
            expected_columns = ["mapid_superset", "ID_superset"]
            checks.append(("Columns Exist", any(col in df.columns for col in expected_columns), 
                        f"None of the expected columns found: {expected_columns}"))
        # Check 3: Verify that there are no misalignments by checking for duplicate rows based on ID columns
        id_columns = [col for col in df.columns if col.startswith('ID') or col in ["dem_sid", "StudyID", "dem_mapid_eeg", "dem_mapid_sleep", "mapid_superset", "ID_superset"]]
        if len(id_columns) > 0:
            duplicated_rows = df[df.duplicated(subset=id_columns, keep=False)]
            checks.append(("No Misalignments", duplicated_rows.empty, f"Found duplicated rows based on ID columns: {duplicated_rows}"))
        # Check 4: Verify that the ID_superset or mapid_superset columns are correctly populated
        if "ID_superset" in df.columns:
            checks.append(("ID_superset Populated", df["ID_superset"].notna().any(), "ID_superset column has missing values"))
        if "mapid_superset" in df.columns:
            checks.append(("mapid_superset Populated", df["mapid_superset"].notna().any(), "mapid_superset column has missing values"))
        return checks

    # Store all failures for summary
    all_failures = []

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)

        # Perform merge checks for each merge step
        for merge_step, files in merge_steps.items():
            file_paths = [os.path.join(folder_path, file) for file in files]

            if not all(os.path.exists(file_path) for file_path in file_paths):
                print(f"\nMissing files for {merge_step} in {folder_path}. Skipping this merge step.")
                continue

            # Load the files to be checked
            dfs = [pd.read_csv(file_path) for file_path in file_paths]
            # Perform checks
            for i, df in enumerate(dfs):
                checks = perform_checks(df, merge_step)
                # Print check results
                print(f"\nCheck results for {merge_step} file {file_paths[i]} in {folder_path}:")
                for check_name, result, message in checks:
                    if result:
                        print(f"  - {check_name}: Passed")
                    else:
                        print(f"  - {check_name}: Failed - {message}")
                        all_failures.append((folder_path, merge_step, file_paths[i], check_name, message))

    # Print summary of all failures
    print("\nSummary of all failures:")
    for folder_path, merge_step, file_path, check_name, message in all_failures:
        print(f"Folder: {folder_path}, Merge Step: {merge_step}, File: {file_path}")
        print(f"  - {check_name}: Failed - {message}")