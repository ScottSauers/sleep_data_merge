import os
import pandas as pd

def ID_list_checker():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # File names to be processed
    merge4_cut_file = "merge4_cut.csv"
    id_list_file = "ID_list.csv"
    def check_id_long_in_supersets(folder_path):
        # Load the merge4_cut file
        merge4_cut_path = os.path.join(folder_path, merge4_cut_file)
        id_list_path = os.path.join(folder_path, id_list_file)
        if not os.path.exists(merge4_cut_path) or not os.path.exists(id_list_path):
            print(f"Missing files in {folder_path}. Skipping this folder.")
            return
        merge4_cut_df = pd.read_csv(merge4_cut_path)
        id_list_df = pd.read_csv(id_list_path)
        # Filter out NaN values from ID_long
        id_list_df = id_list_df.dropna(subset=['ID_long'])
        # Necessary columns are present
        if "ID_superset" not in merge4_cut_df.columns or "mapid_superset" not in merge4_cut_df.columns:
            raise ValueError(f"Required superset columns not found in {merge4_cut_file} in {folder_path}")
        if "ID_long" not in id_list_df.columns:
            raise ValueError(f"Column 'ID_long' not found in {id_list_file} in {folder_path}")
        # Perform the check for ID_long in either ID_superset or mapid_superset
        ids_in_supersets = pd.concat([merge4_cut_df["ID_superset"], merge4_cut_df["mapid_superset"]]).dropna().unique()
        missing_ids = id_list_df[~id_list_df["ID_long"].isin(ids_in_supersets)]
        if not missing_ids.empty:
            print(f"Warning: The following IDs in {id_list_file} in {folder_path} are not present in either ID_superset or mapid_superset:\n{missing_ids[['ID_long']]}")

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        check_id_long_in_supersets(folder_path)
    print("Finished.")