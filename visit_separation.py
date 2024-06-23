import os
import pandas as pd

def visit_separation():
    # Directory where the files are located
    input_dir = "/Users/scott/Downloads/merger/combine"

    # Subdirectories for visit 1 (baseline), visit 2 (follow up 1), and visit 3 (follow up 2)
    visit1_dir = os.path.join(input_dir, "visit_1")
    visit2_dir = os.path.join(input_dir, "visit_2")
    visit3_dir = os.path.join(input_dir, "visit_3")

    # Create subdirectories if they do not exist
    os.makedirs(visit1_dir, exist_ok=True)
    os.makedirs(visit2_dir, exist_ok=True)
    os.makedirs(visit3_dir, exist_ok=True)
    # Files that do not change over time
    static_files = ["mod_apoe.csv", "mod_demographics.csv"]
    # Files with specific conditions
    eeg_file = "HASD_proj2_EEG_power_2024-0412.csv"
    alan_li_file = "FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv"
    sleep_data_file = "1-HASD_proj2_sleep_data_2024-0412.csv"
    # Function to save static files to all directories
    def save_static_files(file_name):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        df.to_csv(os.path.join(visit1_dir, file_name), index=False)
        df.to_csv(os.path.join(visit2_dir, file_name), index=False)
        df.to_csv(os.path.join(visit3_dir, file_name), index=False)
    # Process static files
    for file in static_files:
        save_static_files(file)
    # Function to filter and save EEG data
    def process_eeg_data(file_name):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        df[df["redcap_event_name"] == "baseline_arm_1"].to_csv(os.path.join(visit1_dir, file_name), index=False)
        df[df["redcap_event_name"] == "followup1_arm_1"].to_csv(os.path.join(visit2_dir, file_name), index=False)
        df[df["redcap_event_name"] == "followup2_arm_1"].to_csv(os.path.join(visit3_dir, file_name), index=False)
    # Function to filter and save Alan Li data
    def process_alan_li_data(file_name):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        df[df["Visit_NUM"] == 1].to_csv(os.path.join(visit1_dir, file_name), index=False)
        df[df["Visit_NUM"] == 2].to_csv(os.path.join(visit2_dir, file_name), index=False)
        df[df["Visit_NUM"] == 3].to_csv(os.path.join(visit3_dir, file_name), index=False)
    # Function to filter and save sleep data
    def process_sleep_data(file_name):
        file_path = os.path.join(input_dir, file_name)
        df = pd.read_csv(file_path)
        df[df["redcap_event_name"] == "baseline_arm_1"].to_csv(os.path.join(visit1_dir, file_name), index=False)
        df[(df["redcap_event_name"] == "followup1_arm_1") & (df["redcap_repeat_instance"] == 1)].to_csv(os.path.join(visit2_dir, file_name), index=False)
        df[(df["redcap_event_name"] == "followup1_arm_1") & (df["redcap_repeat_instance"] == 2)].to_csv(os.path.join(visit3_dir, file_name), index=False)
    # Process EEG data
    process_eeg_data(eeg_file)
    # Process Alan Li data
    process_alan_li_data(alan_li_file)
    # Process sleep data
    process_sleep_data(sleep_data_file)