import os
import pandas as pd

def mapid_copy():
    # Directory where the file is located
    input_dir = "/Users/scott/Downloads/merger/combine"
    # File to process
    sleep_data_file = "1-HASD_proj2_sleep_data_2024-0412.csv"
    sleep_data_path = os.path.join(input_dir, sleep_data_file)
    # Load the sleep data
    df = pd.read_csv(sleep_data_path)
    # Fill missing dem_mapid values based on dem_sid
    for dem_sid in df['dem_sid'].unique():
        # Get the non-empty dem_mapid value for the current dem_sid
        dem_mapid_value = df.loc[df['dem_sid'] == dem_sid, 'dem_mapid'].dropna().unique()
        if len(dem_mapid_value) == 1:  # There's exactly one unique non-empty dem_mapid
            df.loc[(df['dem_sid'] == dem_sid) & (df['dem_mapid'].isna()), 'dem_mapid'] = dem_mapid_value[0]
    # Save the modified DataFrame back to CSV
    df.to_csv(sleep_data_path, index=False)
    print(f"Processed and saved {sleep_data_file}")