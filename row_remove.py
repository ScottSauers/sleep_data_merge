import os
import pandas as pd

def row_remove():
    # Define the base directory and folders
    base_dir = "/Users/scott/Downloads/merger/combine"
    folders = ["visit_1", "visit_2", "visit_3"]

    # Iterate through each folder
    for folder in folders:
        print(f"Processing folder: {folder}")
        # Load ID_list for the current folder
        id_list_path = os.path.join(base_dir, folder, "ID_list.csv")
        if not os.path.exists(id_list_path):
            print(f"ID_list.csv not found in {folder}. Skipping...")
            continue
        id_list = pd.read_csv(id_list_path)
        # Load merge4 for the current folder
        merge4_path = os.path.join(base_dir, folder, "merge4.csv")
        if not os.path.exists(merge4_path):
            print(f"merge4.csv not found in {folder}. Skipping...")
            continue
        merge4 = pd.read_csv(merge4_path)
        # Filter rows based on the conditions
        def keep_row(row):
            id_superset = row["ID_superset"]
            mapid_superset = row["mapid_superset"]
            study_id = row.get("StudyID", None)
            if id_superset in id_list["ID_long"].values or mapid_superset in id_list["ID_long"].values:
                return True
            if study_id in id_list["ID_short"].values:
                return True
            return False
        filtered_merge4 = merge4[merge4.apply(keep_row, axis=1)]
        # Check for consistency in IDs and print warnings
        def check_id_consistency(row):
            ids = [row.get(col) for col in ["ID_superset", "mapid_superset", "dem_mapid_eeg", "dem_mapid_sleep"] if pd.notna(row.get(col))]
            if len(set(ids)) > 1:
                print(f"Warning: Inconsistent IDs in row {row.name} - {ids}")
        filtered_merge4.apply(check_id_consistency, axis=1)
        # Save the filtered merge4 as merge4_cut
        merge4_cut_path = os.path.join(base_dir, folder, "merge4_cut.csv")
        filtered_merge4.to_csv(merge4_cut_path, index=False)
        print(f"Filtered merge4 saved as merge4_cut.csv in {folder}")
    print("Processing completed.")