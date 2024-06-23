import to_csv
import mapid_copy
import visit_separation
import date_suffix
import create_date_mapping
import check_date_mapping
import add_close_visit
import next_visit_separation
import merge1
import remove_floats
import merge2
import merge3
import merge4
import merge_checks
import ID_list_maker
import row_remove
import ID_list_checker
import combine_visits
import check_combine
import misalignment_locator
import spot_check

def run_all_functions():
    functions = [
        ("to_csv", to_csv.to_csv),
        ("mapid_copy", mapid_copy.mapid_copy),
        ("visit_separation", visit_separation.visit_separation),
        ("date_suffix", date_suffix.date_suffix),
        ("create_date_mapping", create_date_mapping.create_date_mapping),
        ("check_date_mapping", check_date_mapping.check_date_mapping),
        ("add_close_visit", add_close_visit.add_close_visit),
        ("next_visit_separation", next_visit_separation.next_visit_separation),
        ("merge1", merge1.merge1),
        ("remove_floats", remove_floats.remove_floats),
        ("merge2", merge2.merge2),
        ("merge3", merge3.merge3),
        ("merge4", merge4.merge4),
        ("merge_checks", merge_checks.merge_checks),
        ("ID_list_maker", ID_list_maker.ID_list_maker),
        ("row_remove", row_remove.row_remove),
        ("ID_list_checker", ID_list_checker.ID_list_checker),
        ("combine_visits", combine_visits.combine_visits),
        ("check_combine", check_combine.main),
        ("misalignment_locator", misalignment_locator.main),
        ("spot_check", spot_check.main)]
    for name, func in functions:
        print("\n" + "="*20 + f"\nRunning {name}\n" + "="*20 + "\n")
        func()
if __name__ == "__main__":
    run_all_functions()