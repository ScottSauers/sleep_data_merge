import os
import pandas as pd

def next_visit_separation():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"
    # Directories for visit 1 (baseline), visit 2 (follow up 1), and visit 3 (follow up 2)
    visit1_dir = os.path.join(input_dir, "visit_1")
    visit2_dir = os.path.join(input_dir, "visit_2")
    visit3_dir = os.path.join(input_dir, "visit_3")
    # Create subdirectories if they do not exist
    os.makedirs(visit1_dir, exist_ok=True)
    os.makedirs(visit2_dir, exist_ok=True)
    os.makedirs(visit3_dir, exist_ok=True)
    # List of files to process
    files_to_process = [
        "fasted_precivityad.csv",
        "mod_csf_markers.csv",
        "mod_b4_cdr.csv",
        "mod_barthelemy_tau_species.csv",
        "mod_psychometrics.csv",
        "mod_horie_mbtr_species.csv",
        "mod_c2n_plasma.csv"]

    # Function to separate the data based on visit_number
    def separate_by_visit_number(file_name):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        visit1_df = df[df["visit_number"] == 1]
        visit2_df = df[df["visit_number"] == 2]
        visit3_df = df[df["visit_number"] == 3]
        visit1_df.to_csv(os.path.join(visit1_dir, file_name), index=False)
        visit2_df.to_csv(os.path.join(visit2_dir, file_name), index=False)
        visit3_df.to_csv(os.path.join(visit3_dir, file_name), index=False)
        print(f"Processed and saved {file_name} into visit_1, visit_2, and visit_3 directories")

    # Process each file
    for file_name in files_to_process:
        separate_by_visit_number(file_name)