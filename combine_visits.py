import os
import pandas as pd

def combine_visits():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to processs
    folders = ["visit_1", "visit_2", "visit_3"]
    # List to store DataFrames
    dfs = []

    # Load merge4_cut files from each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        merge4_cut_path = os.path.join(folder_path, "merge4_cut.csv")
        if os.path.exists(merge4_cut_path):
            df = pd.read_csv(merge4_cut_path)
            dfs.append(df)
        else:
            print(f"File {merge4_cut_path} not found. Skipping this folder.")
    # Concatenate the DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    # Create the mapid_all column
    combined_df["mapid_all"] = combined_df["ID_superset"].combine_first(combined_df["mapid_superset"])
    # Save the final combined DataFrame
    output_path = os.path.join(base_dir, "combined_visits.csv")
    combined_df.to_csv(output_path, index=False)
    print(f"Combined file saved to {output_path}")