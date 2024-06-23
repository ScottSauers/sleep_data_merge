import os
import pandas as pd

def merge2():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # File names to be processed
    merge1_file = "merge1.csv"
    sleep_data_file = "1-HASD_proj2_sleep_data_2024-0412.csv"

    def create_merge1_key(row, sleep_data_df):
        # Create a key based on existing identifiers or default to a unique row identifier
        if pd.notna(row['StudyID']) and row['StudyID'] in sleep_data_df['dem_sid_y'].values:
            return f"{int(row['StudyID'])}_short"
        elif pd.notna(row['dem_sid_x']) and row['dem_sid_x'] in sleep_data_df['dem_sid_y'].values:
            return f"{int(row['dem_sid_x'])}_short"
        elif pd.notna(row['dem_mapid_eeg']):
            return f"{int(row['dem_mapid_eeg'])}_long"
        else:
            return f"row_{row.name}"  # Unique fallback for unmatched cases

    def create_sleep_key(row, merge1_df):
        # Create a key based on existing identifiers or default to a unique row identifier
        if pd.notna(row['dem_sid_y']) and (
            row['dem_sid_y'] in merge1_df['dem_sid_x'].values or row['dem_sid_y'] in merge1_df['StudyID'].values):
            return f"{int(row['dem_sid_y'])}_short"
        elif pd.notna(row['dem_mapid_sleep']):
            return f"{int(row['dem_mapid_sleep'])}_long"
        else:
            return f"row_{row.name}"  # Unique fallback for unmatched cases

    def process_files(folder_path):
        # Load the merge1 and sleep data files
        merge1_path = os.path.join(folder_path, merge1_file)
        sleep_data_path = os.path.join(folder_path, sleep_data_file)
        if not os.path.exists(merge1_path) or not os.path.exists(sleep_data_path):
            print(f"Missing files in {folder_path}. Skipping this folder.")
            return

        merge1_df = pd.read_csv(merge1_path)
        sleep_data_df = pd.read_csv(sleep_data_path)

        # Rename columns as specified
        merge1_df = merge1_df.rename(columns={"dem_mapid": "dem_mapid_eeg", "dem_sid": "dem_sid_x"})
        sleep_data_df = sleep_data_df.rename(columns={"dem_mapid": "dem_mapid_sleep", "dem_sid": "dem_sid_y"})
        # Create merge1_key in merge1_df
        merge1_df['merge1_key'] = merge1_df.apply(lambda row: create_merge1_key(row, sleep_data_df), axis=1)
        # Create sleep_key in sleep_data_df
        sleep_data_df['sleep_key'] = sleep_data_df.apply(lambda row: create_sleep_key(row, merge1_df), axis=1)
        # Perform the outer merge
        merge2_df = pd.merge(merge1_df, sleep_data_df, left_on='merge1_key', right_on='sleep_key', how='outer')
        # Create mapid_superset column
        merge2_df["mapid_superset"] = merge2_df["dem_mapid_eeg"].combine_first(merge2_df["dem_mapid_sleep"])
        # Save the result as merge2.csv in the same folder
        merge2_path = os.path.join(folder_path, "merge2.csv")
        merge2_df.to_csv(merge2_path, index=False)
        print(f"Processed and saved merge2 in {folder_path}")

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        process_files(folder_path)
