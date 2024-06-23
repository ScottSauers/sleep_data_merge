import pandas as pd

def check_mapid_all_consistency(df):
    # Define columns to check against mapid_all
    columns_to_check = [
        "ID_superset", "dem_mapid_mod_b4_cdr", "ID_mod_b4_cdr",
        "dem_mapid_mod_csf_markers", "dem_mapid_mod_barthelemy_tau_species",
        "ID_mod_barthelemy_tau_species", "ID_mod_psychometrics",
        "ID_mod_demographics", "ID_mod_apoe", "mapid_superset",
        "dem_mapid_sleep", "dem_mapid_eeg"]

    for index, row in df.iterrows():
        mapid_all = row['mapid_all']
        if not pd.isna(mapid_all) and pd.to_numeric(mapid_all, errors='coerce') is not None:
            for col in columns_to_check:
                if pd.notna(row[col]) and row[col] != mapid_all:
                    raise ValueError(f'Inconsistent value at row {index + 1}: mapid_all={mapid_all} but {col}={row[col]}')

def check_sid_consistency(df):
    # Columns to check for identity
    sid_columns = ["dem_sid_y", "StudyID", "dem_sid_x"]
    for index, row in df.iterrows():
        # Filter out NaN values and check if all values are the same
        values = {row[col] for col in sid_columns if pd.notna(row[col])}
        if len(values) > 1:
            raise ValueError(f'Mismatch in SID values at row {index + 1}: {values}')

def main():
    # Load the data
    df = pd.read_csv('/Users/scott/Downloads/merger/combine/combined_visits.csv')

    # Perform checks
    check_mapid_all_consistency(df)
    check_sid_consistency(df)
    print("All checks passed successfully.")