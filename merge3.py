import os
import pandas as pd

def merge3():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # List of files to be merged in the third step
    files_to_merge = [
        "mod_apoe.csv",
        "mod_demographics.csv",
        "mod_psychometrics.csv",
        "mod_barthelemy_tau_species.csv",
        "mod_csf_markers.csv",
        "mod_horie_mbtr_species.csv",
        "mod_c2n_plasma.csv",
        "fasted_precivityad.csv",
        "mod_b4_cdr.csv"]

    # Function to perform the merge operation
    def merge_files_in_folder(folder_path):
        merged_df = None
        id_columns = []

        # Load and rename ID columns for each file
        for file_name in files_to_merge:
            file_path = os.path.join(folder_path, file_name)
            if not os.path.exists(file_path):
                print(f"Missing file {file_name} in {folder_path}. Skipping this file.")
                continue
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"Empty sheet {file_name} in {folder_path}. Skipping this file.")
                continue

            # Rename ID column to include file-specific suffix
            id_column = f"ID_{file_name.split('.')[0]}"
            df = df.rename(columns={"ID": id_column})
            id_columns.append(id_column)

            # Merge dataframes based on the suffixed ID columns
            if merged_df is None:
                merged_df = df
            else:
                # Ensure the merge is performed on the correct ID columns
                merged_df = pd.merge(merged_df, df, left_on=[id_columns[0]], right_on=[id_column], how="outer", suffixes=("", f"_{file_name.split('.')[0]}"))

        if merged_df is not None:
            # Create the ID_superset column
            merged_df["ID_superset"] = merged_df[id_columns].bfill(axis=1).iloc[:, 0]
            # Save the result as merge3.csv in the same folder
            merge3_path = os.path.join(folder_path, "merge3.csv")
            merged_df.to_csv(merge3_path, index=False)
            print(f"Processed and saved merge3 in {folder_path}")
        else:
            print(f"No files to merge in {folder_path}.")

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        merge_files_in_folder(folder_path)