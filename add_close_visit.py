import os
import pandas as pd
from datetime import datetime, timedelta

def add_close_visit():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"
    # Date mapping file path
    date_mapping_path = os.path.join(input_dir, "date_mapping.csv")

    # List of files and their corresponding date columns with suffixed names
    date_columns = {
        "fasted_precivityad.csv": "Plasma_Date_fasted_precivityad",
        "mod_csf_markers.csv": "CSF_LP_DATE_mod_csf_markers",
        "mod_b4_cdr.csv": "TESTDATE_mod_b4_cdr",
        "mod_barthelemy_tau_species.csv": "CSF_LP_DATE_mod_barthelemy_tau_species",
        "mod_psychometrics.csv": "psy_date_mod_psychometrics",
        "mod_horie_mbtr_species.csv": "csf_LP_DATE_mod_horie_mbtr_species",
        "mod_c2n_plasma.csv": "draw_date_mod_c2n_plasma"}

    # Load the date mapping file
    date_mapping_df = pd.read_csv(date_mapping_path)

    # Remove the time component " 00:00:00" from date_pdx_or_act
    date_mapping_df["date_pdx_or_act"] = date_mapping_df["date_pdx_or_act"].str.replace(" 00:00:00", "")
    # Convert dem_mapid to numeric to handle both int and float representations
    date_mapping_df["dem_mapid"] = pd.to_numeric(date_mapping_df["dem_mapid"], errors='coerce')
    # Store the original date_pdx_or_act values to track raw values
    date_mapping_df["raw_date_pdx_or_act"] = date_mapping_df["date_pdx_or_act"]
    # Is date_pdx_or_act is correctly parsed as datetime and strip the time part
    date_mapping_df["date_pdx_or_act"] = pd.to_datetime(date_mapping_df["date_pdx_or_act"], errors='coerce').dt.date
    # Function to find the closest date within 1.5 years
    def find_closest_date(row_date, dates):
        if pd.isna(row_date):
            return None, None
        min_date = row_date - timedelta(days=547)  # 1.5 years = 547 days
        max_date = row_date + timedelta(days=547)
        valid_dates = [d for d in dates if pd.notna(d) and min_date <= d <= max_date]
        if not valid_dates:
            closest_date = min([d for d in dates if pd.notna(d)], key=lambda d: abs(d - row_date), default=None)
            return None, closest_date
        closest_date = min(valid_dates, key=lambda d: abs(d - row_date))
        return closest_date, closest_date
    # Process each file
    for file_name, date_column in date_columns.items():
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        # Remove the time component " 00:00:00" from the date column
        df[date_column] = df[date_column].astype(str).str.replace(" 00:00:00", "")
        # Convert ID to numeric to handle both int and float representations
        df["ID"] = pd.to_numeric(df["ID"], errors='coerce')
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.date
        new_rows = []
        for _, date_mapping_row in date_mapping_df.iterrows():
            dem_mapid = date_mapping_row["dem_mapid"]
            visit_number = date_mapping_row["visit_number"]
            date_pdx_or_act = date_mapping_row["date_pdx_or_act"]
            raw_date_pdx_or_act = date_mapping_row["raw_date_pdx_or_act"]
            if pd.isna(date_pdx_or_act):
                raise ValueError(f"Mapping date for dem_mapid {dem_mapid} is NaT. Raw value: '{raw_date_pdx_or_act}'")
            # Filter the rows that match the dem_mapid
            matching_rows = df[df["ID"] == dem_mapid]
            if matching_rows.empty:
                print(f"No matching dem_mapid {dem_mapid} found in file {file_name}. Mapping date: {date_pdx_or_act} (Raw value: '{raw_date_pdx_or_act}')")
                continue
            closest_date, within_1_5_years_date = find_closest_date(date_pdx_or_act, matching_rows[date_column])
            if within_1_5_years_date is None:
                print(f"No date within 1.5 years for dem_mapid {dem_mapid} in file {file_name}. Mapping date: {date_pdx_or_act} (Raw value: '{raw_date_pdx_or_act}'), Closest date: {closest_date}")
                continue

            if closest_date is not None:
                best_match_row = matching_rows[matching_rows[date_column] == closest_date].iloc[0]
                best_match_row["dem_mapid"] = dem_mapid
                best_match_row["visit_number"] = visit_number
                best_match_row["date_pdx_or_act"] = date_pdx_or_act
                new_rows.append(best_match_row)
                #print(f"Match found for dem_mapid {dem_mapid} in file {file_name}. Mapping date: {date_pdx_or_act} (Raw value: '{raw_date_pdx_or_act}'), Closest date: {closest_date}")
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            new_df = new_df.dropna(subset=["visit_number"])
            new_df.to_csv(file_path, index=False)
            print(f"Processed and saved {file_name}")
