import os
import pandas as pd

def ID_list_maker():
    # Define the base directory and folders
    base_dir = "/Users/scott/Downloads/merger/combine"
    folders = ["visit_1", "visit_2", "visit_3"]
    # Iterate through each folder
    for folder in folders:
        print(f"Processing folder: {folder}")
        # Initialize an empty DataFrame for ID_list
        id_list = pd.DataFrame(columns=["ID_long", "ID_short"])
        # Define the file paths
        sleep_data_path = os.path.join(base_dir, folder, "1-HASD_proj2_sleep_data_2024-0412.csv")
        alan_li_data_path = os.path.join(base_dir, folder, "FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv")
        # Check if files exist before reading
        if os.path.exists(sleep_data_path) and os.path.exists(alan_li_data_path):
            # Read the data
            sleep_data = pd.read_csv(sleep_data_path)
            alan_li_data = pd.read_csv(alan_li_data_path)
            # Extract the IDs
            sleep_data_ids = sleep_data[["dem_sid_y", "dem_mapid_sleep"]].copy()
            sleep_data_ids.columns = ["ID_short", "ID_long"]
            alan_li_ids = alan_li_data[["StudyID"]].copy()
            alan_li_ids.columns = ["ID_short"]
            # Append to the ID_list
            id_list = pd.concat([id_list, sleep_data_ids, alan_li_ids], ignore_index=True)
            # Drop duplicates from ID_list
            id_list = id_list.drop_duplicates().reset_index(drop=True)
            # Save the ID_list to a new CSV file within the respective folder
            id_list_path = os.path.join(base_dir, folder, "ID_list.csv")
            id_list.to_csv(id_list_path, index=False)
            print(f"ID_list created and saved successfully for {folder}.")
        else:
            print(f"Missing one or more files in {folder}. Skipping...")

    print("Processing completed.")