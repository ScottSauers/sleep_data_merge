import os
import pandas as pd

def check_date_mapping():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"
    date_mapping_file = os.path.join(input_dir, "date_mapping.csv")
    sleep_data_file = os.path.join(input_dir, "1-HASD_proj2_sleep_data_2024-0412.csv")

    # Load the data
    date_mapping_df = pd.read_csv(date_mapping_file)
    sleep_data_df = pd.read_csv(sleep_data_file)

    # Create a dictionary to map the visit numbers to the correct event names and instances
    visit_mapping = {
        1: {"redcap_event_name": "baseline_arm_1", "redcap_repeat_instance": None},
        2: {"redcap_event_name": "followup1_arm_1", "redcap_repeat_instance": 1},
        3: {"redcap_event_name": "followup1_arm_1", "redcap_repeat_instance": 2}}

    # Function to get the visit number from event name and repeat instance
    def get_visit_number(row):
        for visit_number, criteria in visit_mapping.items():
            if row["redcap_event_name"] == criteria["redcap_event_name"]:
                if criteria["redcap_repeat_instance"] is None or row["redcap_repeat_instance"] == criteria["redcap_repeat_instance"]:
                    return visit_number
        return None

    # Extract necessary information for verification
    sleep_data_df["visit_number"] = sleep_data_df.apply(get_visit_number, axis=1)
    # Select the required columns and drop rows with missing visit numbers
    verification_df = sleep_data_df[["dem_mapid", "visit_number"] + [col for col in sleep_data_df.columns if "date" in col.lower()]].dropna(subset=["visit_number"])
    # Initialize a DataFrame to collect discrepancies
    discrepancies = pd.DataFrame(columns=["dem_mapid", "visit_number", "date_mapping", "date_verification", "source_column"])
    # Check the date mapping for each entry
    for _, row in date_mapping_df.iterrows():
        dem_mapid = row["dem_mapid"]
        visit_number = row["visit_number"]
        date_mapping = row["date_pdx_or_act"]
        # Find the corresponding rows in the sleep data
        matching_rows = verification_df[(verification_df["dem_mapid"] == dem_mapid) & (verification_df["visit_number"] == visit_number)]

        if not matching_rows.empty:
            # Check each date column for a match
            match_found = False
            for col in [col for col in verification_df.columns if "date" in col.lower()]:
                if date_mapping in matching_rows[col].values:
                    match_found = True
                    break
            if not match_found:
                for col in [col for col in verification_df.columns if "date" in col.lower()]:
                    discrepancies = discrepancies.append({
                        "dem_mapid": dem_mapid,
                        "visit_number": visit_number,
                        "date_mapping": date_mapping,
                        "date_verification": matching_rows[col].values[0] if not matching_rows[col].isna().all() else None,
                        "source_column": col
                    }, ignore_index=True)
        else:
            discrepancies = discrepancies.append({
                "dem_mapid": dem_mapid,
                "visit_number": visit_number,
                "date_mapping": date_mapping,
                "date_verification": None,
                "source_column": "No matching row found"
            }, ignore_index=True)
    if discrepancies.empty:
        print("Date mapping is correct.")
    else:
        print("Discrepancies found in date mapping:")
        print(discrepancies)