import os
import pandas as pd

def remove_floats():
    # Directory where the folders are located
    base_dir = "/Users/scott/Downloads/merger/combine"
    # List of folders to process
    folders = ["visit_1", "visit_2", "visit_3"]
    # File names to be processed
    merge1_file = "merge1.csv"
    sleep_data_file = "1-HASD_proj2_sleep_data_2024-0412.csv"
    def convert_floats_to_ints_manually(df, columns):
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(float(x)) if pd.notna(x) and '.0' in str(x) else x)
                df[col] = df[col].astype(str).str.replace('.0', '')
                df[col] = df[col].apply(lambda x: int(x) if x.isdigit() else x)
        return df

    def check_columns(df, expected_columns, file_name):
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in {file_name}: {missing_columns}")

    def check_for_decimals_in_csv(file_path, columns):
        with open(file_path, 'r') as file:
            for line_number, line in enumerate(file, start=1):
                for col in columns:
                    if f"{col}." in line:
                        raise ValueError(f"Decimal values found in column {col} of {file_path} on line {line_number}.")

    def process_files(folder_path):
        # Load the merge1 and sleep data files
        merge1_path = os.path.join(folder_path, merge1_file)
        sleep_data_path = os.path.join(folder_path, sleep_data_file)

        if not os.path.exists(merge1_path) or not os.path.exists(sleep_data_path):
            print(f"Missing files in {folder_path}. Skipping this folder.")
            return

        merge1_df = pd.read_csv(merge1_path)
        sleep_data_df = pd.read_csv(sleep_data_path)

        # Rename columns as specified
        merge1_df = merge1_df.rename(columns={"dem_mapid": "dem_mapid_eeg", "dem_sid": "dem_sid_x"})
        sleep_data_df = sleep_data_df.rename(columns={"dem_mapid": "dem_mapid_sleep", "dem_sid": "dem_sid_y"})
        # Columns to be converted
        columns_to_convert = ['StudyID', 'dem_sid_x', 'dem_mapid_eeg', 'dem_sid_y', 'dem_mapid_sleep']
        # Check if all required columns are present
        check_columns(merge1_df, columns_to_convert[:3], merge1_file)
        check_columns(sleep_data_df, columns_to_convert[3:], sleep_data_file)
        # Convert float values to integers in specified columns manually
        merge1_df = convert_floats_to_ints_manually(merge1_df, columns_to_convert[:3])
        sleep_data_df = convert_floats_to_ints_manually(sleep_data_df, columns_to_convert[3:])
        # Save the modified dataframes back to CSV
        merge1_df.to_csv(merge1_path, index=False)
        sleep_data_df.to_csv(sleep_data_path, index=False)
        # Check for any remaining decimal values in the columns by re-opening the saved CSV files
        check_for_decimals_in_csv(merge1_path, columns_to_convert[:3])
        check_for_decimals_in_csv(sleep_data_path, columns_to_convert[3:])
        print(f"Processed and saved files in {folder_path}")

    # Process each folder
    for folder in folders:
        folder_path = os.path.join(base_dir, folder)
        process_files(folder_path)