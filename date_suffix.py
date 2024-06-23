import os
import pandas as pd

def date_suffix():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"
    # List of files and their corresponding date columns to be suffixed
    date_columns = {
        "fasted_precivityad.csv": "Plasma_Date",
        "mod_csf_markers.csv": "CSF_LP_DATE",
        "mod_b4_cdr.csv": "TESTDATE",
        "mod_barthelemy_tau_species.csv": "CSF_LP_DATE",
        "mod_psychometrics.csv": "psy_date",
        "mod_horie_mbtr_species.csv": "csf_LP_DATE",
        "mod_c2n_plasma.csv": "draw_date"}
    # Function to suffix the date columns with the file name
    def suffix_date_columns(file_name, date_column):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        new_column_name = f"{date_column}_{file_name.split('.')[0]}"
        df.rename(columns={date_column: new_column_name}, inplace=True)
        df.to_csv(file_path, index=False)
        print(f"Renamed {date_column} to {new_column_name} in {file_name}")
    # Process each file
    for file_name, date_column in date_columns.items():
        suffix_date_columns(file_name, date_column)