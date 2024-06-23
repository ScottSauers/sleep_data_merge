import os
import pandas as pd

def merge4():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)

        # Load merge2 and merge3 files
        merge2_path = os.path.join(folder_path, "merge2.csv")
        merge3_path = os.path.join(folder_path, "merge3.csv")
        if not os.path.exists(merge2_path) or not os.path.exists(merge3_path):
            print(f"Missing merge2.csv or merge3.csv in {folder_path}. Skipping this folder.")
            continue
        merge2_df = pd.read_csv(merge2_path)
        merge3_df = pd.read_csv(merge3_path)
        if merge2_df.empty or merge3_df.empty:
            print(f"Empty merge2.csv or merge3.csv in {folder_path}. Skipping this folder.")
            continue
        # Perform the merge on mapid_superset from merge2 and ID_superset from merge3
        merge4_df = pd.merge(merge2_df, merge3_df, left_on="mapid_superset", right_on="ID_superset", how="outer", suffixes=("_merge2", "_merge3"))
        # Save the result as merge4.csv in the same folder
        merge4_path = os.path.join(folder_path, "merge4.csv")
        merge4_df.to_csv(merge4_path, index=False)
        print(f"Processed and saved merge4 in {folder_path}")