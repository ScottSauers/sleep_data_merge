import os
import pandas as pd

def create_date_mapping():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"
    output_file = os.path.join(input_dir, "date_mapping.csv")
    # File to process
    sleep_data_file = "1-HASD_proj2_sleep_data_2024-0412.csv"
    sleep_data_path = os.path.join(input_dir, sleep_data_file)
    # Load the sleep data
    df = pd.read_csv(sleep_data_path)

    # Define the date columns in order of preference
    date_columns = [
        "pdx_n1_date", "act_n1_date", "act_n2_date", "act_n3_date",
        "act_n4_date", "act_n5_date", "act_n6_date", "pdx_date"]

    # Function to get the date using the fallback method
    def get_fallback_date(row):
        for col in date_columns:
            if not pd.isna(row[col]):
                return row[col]
        return None

    # Determine visit number
    def get_visit_number(row):
        if row["redcap_event_name"] == "baseline_arm_1":
            return 1
        elif row["redcap_event_name"] == "followup1_arm_1":
            if row["redcap_repeat_instance"] == 1:
                return 2
            elif row["redcap_repeat_instance"] == 2:
                return 3
        return None

    # Extract necessary information
    df["visit_number"] = df.apply(get_visit_number, axis=1)
    df["date_pdx_or_act"] = df.apply(get_fallback_date, axis=1)
    # Select the required columns and drop rows with missing dates or visit numbers
    date_mapping_df = df[["dem_mapid", "visit_number", "date_pdx_or_act"]].dropna()
    # Save the resulting DataFrame to a CSV file
    date_mapping_df.to_csv(output_file, index=False)
    print(f"date_mapping file created at: {output_file}")