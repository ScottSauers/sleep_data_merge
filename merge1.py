import os
import pandas as pd

def merge1():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # File names to be merged
    eeg_file = "HASD_proj2_EEG_power_2024-0412.csv"
    alan_li_file = "FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv"

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        # Load the EEG and Alan Li files
        eeg_path = os.path.join(folder_path, eeg_file)
        alan_li_path = os.path.join(folder_path, alan_li_file)
        if not os.path.exists(eeg_path) or not os.path.exists(alan_li_path):
            print(f"Missing files in {folder_path}. Skipping this folder.")
            continue
        eeg_df = pd.read_csv(eeg_path)
        alan_li_df = pd.read_csv(alan_li_path)
        # Perform the merge on dem_sid from EEG and StudyID from Alan Li
        merge1_df = pd.merge(eeg_df, alan_li_df, left_on="dem_sid", right_on="StudyID", how="outer")
        # Save the result as merge1.csv in the same folder
        merge1_path = os.path.join(folder_path, "merge1.csv")
        merge1_df.to_csv(merge1_path, index=False)
        print(f"Processed and saved merge1 in {folder_path}")