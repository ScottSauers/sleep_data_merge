import os
import pandas as pd

def to_csv():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"

    # List of files to convert
    files_to_convert = [
        "mod_barthelemy_tau_species.xlsx",
        "mod_apoe.xlsx",
        "mod_psychometrics.xlsx",
        "mod_demographics.xlsx",
        "fasted_precivityad.xlsx",
        "mod_b4_cdr.xlsx",
        "mod_horie_mbtr_species.xlsx",
        "HASD_proj2_EEG_power_2024-0412.xlsx",
        "FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.xlsx",
        "mod_c2n_plasma.xlsx",
        "mod_csf_markers.xlsx",
        "1-HASD_proj2_sleep_data_2024-0412.xlsx"]

    # Function to convert Excel to CSV
    def convert_to_csv(file_path):
        # Read the Excel file
        df = pd.read_excel(file_path)
        # Generate the CSV file path
        csv_file_path = file_path.replace('.xlsx', '.csv')
        # Save the dataframe to CSV
        df.to_csv(csv_file_path, index=False)
        print(f"Converted {file_path} to {csv_file_path}")

    # Convert each file
    for file_name in files_to_convert:
        file_path = os.path.join(input_dir, file_name)
        convert_to_csv(file_path)


    file_path = os.path.join(input_dir, "mod_demographics.csv")
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return
    df = pd.read_csv(file_path)
    if "version" in df.columns:
        df.rename(columns={"version": "version_demographics"}, inplace=True)
        df.to_csv(file_path, index=False)
        print(f"Renamed 'version' to 'version_demographics' in {file_path}.")
    else:
        print(f"'version' column not found in {file_path}.")