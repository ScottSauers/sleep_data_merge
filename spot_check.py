import os
import pandas as pd
import random

def check_mapid_all_consistency(df, errors):
    columns_to_check = [
        "ID_superset", "dem_mapid_mod_b4_cdr", "ID_mod_b4_cdr",
        "dem_mapid_mod_csf_markers", "dem_mapid_mod_barthelemy_tau_species",
        "ID_mod_barthelemy_tau_species", "ID_mod_psychometrics",
        "ID_mod_demographics", "ID_mod_apoe", "mapid_superset",
        "dem_mapid_sleep", "dem_mapid_eeg"]

    for index, row in df.iterrows():
        mapid_all = int(round(row.get('mapid_all'))) if pd.notna(row.get('mapid_all')) else 'nan'
        if mapid_all != 'nan':
            for col in columns_to_check:
                if col in df.columns:
                    col_value = int(round(row[col])) if pd.notna(row[col]) else 'nan'
                    if col_value != 'nan' and col_value != mapid_all:
                        errors.append(f"Inconsistent value in row {index + 1}: mapid_all={mapid_all} but {col}={col_value}")

def check_sid_consistency(df, errors):
    sid_columns = ["dem_sid_y", "StudyID", "dem_sid_x"]
    for index, row in df.iterrows():
        values = {str(row[col]).lower() for col in sid_columns if col in df.columns and pd.notna(row[col])}
        if len(values) > 1:
            errors.append(f"Mismatch in SID values in row {index + 1}: {values}")

def find_closest_match(df, id_value):
    df_mapid_all = df['mapid_all'].dropna().astype(int)
    closest_value = df_mapid_all.iloc[(df_mapid_all - id_value).abs().argsort()[:1]].values[0]
    return closest_value

def spot_check_combined_visits(df, source_files, errors):
    for file in source_files:
        total_matched_rows = 0
        failed_matches = 0
        failed_examples = []
        source_df = pd.read_csv(file)
        source_columns = source_df.columns

        if "ID" not in source_columns:
            errors.append(f"ID column not found in {file}, skipping this file.")
            continue

        if "date_pdx_or_act" not in source_columns:
            if 'mod_apoe.csv' in file or 'mod_demographics.csv' in file:
                id_column = "ID"
                middle_column = source_columns[len(source_columns) // 2]
                last_column = source_columns[-4]
                for index, row in source_df.iterrows():
                    id_value = int(round(row[id_column])) if pd.notna(row[id_column]) else 'nan'
                    if id_value in df['mapid_all'].dropna().astype(int).values:
                        total_matched_rows += 1
                        match_rows = df[df['mapid_all'].dropna().astype(int) == id_value]
                        if {middle_column, last_column}.issubset(match_rows.columns):
                            try:
                                match_found = any(
                                    (int(round(match_row['mapid_all'])) == id_value and 
                                    str(match_row[middle_column]).rstrip('.0').lower() == str(row[middle_column]).rstrip('.0').lower() and 
                                    str(match_row[last_column]).rstrip('.0').lower() == str(row[last_column]).rstrip('.0').lower()) for _, match_row in match_rows.iterrows()
                                )
                            except KeyError as e:
                                errors.append(f"KeyError: {e} in file {file} while accessing matched rows for ID {id_value}.")
                                continue
                            if not match_found:
                                closest_value = find_closest_match(df, id_value)
                                if closest_value == id_value:
                                    total_matched_rows += 1
                                else:
                                    failed_matches += 1
                                    failed_examples.append((index + 1, id_value, row[middle_column], row[last_column], match_rows[[middle_column, last_column]], closest_value))
                        else:
                            missing_columns = {middle_column, last_column} - set(match_rows.columns)
                            errors.append(f"Required columns {missing_columns} missing in the matched rows of {file} for ID {id_value}.")
                if total_matched_rows == 0:
                    errors.append(f"No rows matched on mapid_all in file {file}.")
                else:
                    fail_percentage = (failed_matches / total_matched_rows) * 100
                    errors.append(f"First-Middle-Last test for {file}: {fail_percentage:.6f}% of matched rows failed ({failed_matches}/{total_matched_rows}).")
                    if failed_examples:
                        example = random.choice(failed_examples)
                        example_info = f"Example failure in {file} at row {example[0]}: \nTrying to find: ID={example[1]}, {middle_column}={example[2]}, {last_column}={example[3]}, Closest mapid_all={example[5]}\n"
                        example_info += f"Matched rows:\n{example[4].to_string(index=False, header=True)}"
                        errors.append(example_info)
            else:
                errors.append(f"date_pdx_or_act column not found in {file}, skipping this file.")
                continue
        else:
            id_column = "ID"
            middle_column = source_columns[len(source_columns) // 2]
            last_column = source_columns[-4]
            for index, row in source_df.iterrows():
                id_value = int(round(row[id_column])) if pd.notna(row[id_column]) else 'nan'
                date_pdx_or_act = str(row['date_pdx_or_act']).lower() if pd.notna(row['date_pdx_or_act']) else 'nan'
                if id_value in df['mapid_all'].dropna().astype(int).values:
                    total_matched_rows += 1
                    match_rows = df[(df['mapid_all'].dropna().astype(int) == id_value) & 
                                    ((df['date_pdx_or_act'].astype(str).str.lower() == date_pdx_or_act) | (df['date_pdx_or_act'].astype(str).str.lower() != 'nan'))]
                    if {middle_column, last_column}.issubset(match_rows.columns):
                        try:
                            match_found = any(
                                (int(round(match_row['mapid_all'])) == id_value and 
                                str(match_row[middle_column]).rstrip('.0').lower() == str(row[middle_column]).rstrip('.0').lower() and 
                                str(match_row[last_column]).rstrip('.0').lower() == str(row[last_column]).rstrip('.0').lower()) for _, match_row in match_rows.iterrows()
                            )
                        except KeyError as e:
                            errors.append(f"KeyError: {e} in file {file} while accessing matched rows for ID {id_value}.")
                            continue
                        if not match_found:
                            closest_value = find_closest_match(df, id_value)
                            if closest_value == id_value:
                                total_matched_rows += 1
                            else:
                                failed_matches += 1
                                failed_examples.append((index + 1, row[id_column], row[middle_column], row[last_column], row['date_pdx_or_act'], match_rows[[middle_column, last_column, 'date_pdx_or_act']], closest_value))
                    else:
                        missing_columns = {middle_column, last_column} - set(match_rows.columns)
                        errors.append(f"Required columns {missing_columns} missing in the matched rows of {file} for ID {id_value}.")
            if total_matched_rows == 0:
                errors.append(f"No rows matched on mapid_all in file {file}.")
            else:
                fail_percentage = (failed_matches / total_matched_rows) * 100
                errors.append(f"First-Middle-Last test for {file}: {fail_percentage:.6f}% of matched rows failed ({failed_matches}/{total_matched_rows}).")
                if failed_examples:
                    example = random.choice(failed_examples)
                    example_info = f"Example failure in {file} at row {example[0]}: \nTrying to find: ID={example[1]}, {middle_column}={example[2]}, {last_column}={example[3]}, date_pdx_or_act={example[4]}, Closest mapid_all={example[6]}\n"
                    example_info += f"Matched rows:\ndate_pdx_or_act:\n{example[5].to_string(index=False, header=True)}"
                    errors.append(example_info)

def spot_check_middle_chunk(df, source_file, errors):
    source_df = pd.read_csv(source_file)
    middle_index = len(source_df) // 2
    chunk_columns = source_df.columns[len(source_df.columns) // 2 - 2: len(source_df.columns) // 2 + 3]
    chunk = source_df.loc[middle_index: middle_index + 4, chunk_columns]

    column_to_print = {
        '1-HASD_proj2_sleep_data_2024-0412.csv': 'dem_mapid',
        'FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv': 'StudyID',
        'HASD_proj2_EEG_power_2024-0412.csv': 'dem_mapid'}

    total_rows = len(chunk)
    failed_examples = []
    for index, row in chunk.iterrows():
        match_found = any(
            all(str(row[col]).lower() in df[col].astype(str).str.lower().values for col in chunk.columns if col in df.columns)
            for _, match_row in df.iterrows()
        )
        if not match_found:
            col_value = row[column_to_print[os.path.basename(source_file)]]
            failed_examples.append((index + 1, col_value))

    if failed_examples:
        example_info = f"Example of failed middle chunk for {source_file}:\n{chunk.to_string(index=False, header=True)}\n"
        errors.append(example_info)
        for example in failed_examples:
            errors.append(f"No match found for row {example[0]} ({column_to_print[os.path.basename(source_file)]}={example[1]}) in {source_file} during middle chunk test")
    success_percentage = ((total_rows - len(failed_examples)) / total_rows) * 100
    errors.append(f"Middle chunk test for {source_file}: {success_percentage:.6f}% of rows matched successfully ({total_rows - len(failed_examples)}/{total_rows}).")

def main():
    combined_visits_file = '/Users/scott/Downloads/merger/combine/combined_visits.csv'
    source_files = [
        '/Users/scott/Downloads/merger/combine/fasted_precivityad.csv',
        '/Users/scott/Downloads/merger/combine/mod_apoe.csv',
        '/Users/scott/Downloads/merger/combine/mod_b4_cdr.csv',
        '/Users/scott/Downloads/merger/combine/mod_barthelemy_tau_species.csv',
        '/Users/scott/Downloads/merger/combine/mod_c2n_plasma.csv',
        '/Users/scott/Downloads/merger/combine/mod_csf_markers.csv',
        '/Users/scott/Downloads/merger/combine/mod_demographics.csv',
        '/Users/scott/Downloads/merger/combine/mod_horie_mbtr_species.csv',
        '/Users/scott/Downloads/merger/combine/mod_psychometrics.csv']
    spot_check_files = [
        '/Users/scott/Downloads/merger/combine/1-HASD_proj2_sleep_data_2024-0412.csv',
        '/Users/scott/Downloads/merger/combine/FInal HASD_Alan_Li_analyses_DATABASE-A1 A2 A3.csv',
        '/Users/scott/Downloads/merger/combine/HASD_proj2_EEG_power_2024-0412.csv']

    df = pd.read_csv(combined_visits_file)
    errors = []
    check_mapid_all_consistency(df, errors)
    check_sid_consistency(df, errors)
    spot_check_combined_visits(df, source_files, errors)

    for file in spot_check_files:
        spot_check_middle_chunk(df, file, errors)
    if errors:
        print("\nSummary of all failures:")
        for error in errors:
            print(error)
    else:
        print("All checks passed successfully in all files.")
    print("All checks completed.")

if __name__ == "__main__":
    main()