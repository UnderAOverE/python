from typing import List, Dict, Union, Optional, Set

def check_overall_red_status(
    app_statuses: List[Dict[str, str]],
    red_threshold_percentage: Optional[float] = None,
    specific_apps_must_be_red: Optional[List[str]] = None,
    all_must_be_red: bool = False
) -> bool:
    """
    Checks if an overall "Red" status is met based on a list of app statuses.

    The function returns True (indicating overall Red) if:
    1. `all_must_be_red` is True AND all apps are "Red".
    2. OR, `red_threshold_percentage` is provided AND the percentage of "Red" apps
       meets or exceeds this threshold.
    3. OR, `specific_apps_must_be_red` is provided AND ALL apps in this list
       are "Red".

    If none of these conditions for "Red" are met, and assuming there's at least one app,
    it implies an overall "Green" or "Amber" state, so it returns False.
    If the list is empty, it's considered not "Red" (returns False).

    Args:
        app_statuses: A list of dictionaries, where each dict has "appname" (str)
                      and "status" (str, e.g., "Green", "Amber", "Red").
        red_threshold_percentage: If set, the percentage (0.0 to 100.0) of apps
                                  that must be "Red" to trigger an overall Red status.
        specific_apps_must_be_red: If set, a list of app names. If ALL of these
                                     specific apps are "Red", triggers overall Red.
        all_must_be_red: If True, all apps in the list must be "Red" to trigger
                         overall Red. This takes precedence if multiple conditions could apply.

    Returns:
        bool: True if the criteria for an overall "Red" status are met, False otherwise.
    """
    if not app_statuses:
        return False  # No apps, so not "Red" by any criteria

    total_apps = len(app_statuses)
    red_app_count = 0
    red_app_names: Set[str] = set()

    for app_info in app_statuses:
        app_name = app_info.get("appname")
        status = app_info.get("status", "").strip().lower() # Normalize status

        if status == "red":
            red_app_count += 1
            if app_name: # Store names of red apps if app_name is present
                red_app_names.add(app_name)

    # --- Condition 1: All apps must be Red ---
    if all_must_be_red:
        return red_app_count == total_apps

    # --- Condition 2: Specific apps must be Red ---
    if specific_apps_must_be_red:
        # Convert to set for efficient lookup
        required_red_apps_set = set(specific_apps_must_be_red)
        # Check if all apps in specific_apps_must_be_red are present in red_app_names
        if required_red_apps_set.issubset(red_app_names):
            # Check if all apps in required_red_apps_set actually exist in the input list
            # This handles cases where a required app might not even be in app_statuses
            all_required_apps_present_and_red = True
            input_app_names = {app.get("appname") for app in app_statuses if app.get("appname")}
            for required_app in required_red_apps_set:
                if required_app not in input_app_names: # Required app not in input list
                    all_required_apps_present_and_red = False
                    break # No need to check further for this condition
                if required_app not in red_app_names: # Required app is present but not red
                    all_required_apps_present_and_red = False
                    break

            if all_required_apps_present_and_red:
                return True # All specified critical apps are Red

    # --- Condition 3: Percentage threshold ---
    if red_threshold_percentage is not None:
        if not (0.0 <= red_threshold_percentage <= 100.0):
            raise ValueError("red_threshold_percentage must be between 0.0 and 100.0")
        
        calculated_percentage = (red_app_count / total_apps) * 100
        if calculated_percentage >= red_threshold_percentage:
            return True # Percentage of Red apps meets or exceeds threshold

    # If none of the "Red" conditions were met, it means it's effectively "Green" or "Amber"
    # for the purpose of this function's True/False output.
    return False

# --- Example Usage ---
app_data_1 = [
    {"appname": "AppA", "status": "Red"},
    {"appname": "AppB", "status": "Red"},
    {"appname": "AppC", "status": "Red"},
]

app_data_2 = [
    {"appname": "AppA", "status": "Red"},
    {"appname": "AppB", "status": "Amber"},
    {"appname": "AppC", "status": "Green"},
    {"appname": "AppD", "status": "Red"},
]

app_data_3 = [
    {"appname": "AppA", "status": "Green"},
    {"appname": "AppB", "status": "Amber"},
    {"appname": "AppC", "status": "Green"},
]

app_data_4 = [
    {"appname": "CriticalApp1", "status": "Red"},
    {"appname": "CriticalApp2", "status": "Red"},
    {"appname": "OtherApp", "status": "Green"},
]

app_data_5 = [
    {"appname": "CriticalApp1", "status": "Red"},
    {"appname": "CriticalApp2", "status": "Amber"}, # One critical app is not Red
    {"appname": "OtherApp", "status": "Green"},
]

app_data_6 = [ # For testing specific_apps where one required is missing from input
    {"appname": "CriticalApp1", "status": "Red"},
    {"appname": "OtherApp", "status": "Green"},
]


print("--- Test all_must_be_red ---")
print(f"app_data_1 (all red): {check_overall_red_status(app_data_1, all_must_be_red=True)}")  # Expected: True
print(f"app_data_2 (mixed): {check_overall_red_status(app_data_2, all_must_be_red=True)}")    # Expected: False
print(f"app_data_3 (no red): {check_overall_red_status(app_data_3, all_must_be_red=True)}")   # Expected: False

print("\n--- Test red_threshold_percentage ---")
# app_data_2 has 2 Red apps out of 4 (50%)
print(f"app_data_2, threshold 50%: {check_overall_red_status(app_data_2, red_threshold_percentage=50.0)}") # Expected: True
print(f"app_data_2, threshold 60%: {check_overall_red_status(app_data_2, red_threshold_percentage=60.0)}") # Expected: False
print(f"app_data_1, threshold 100%: {check_overall_red_status(app_data_1, red_threshold_percentage=100.0)}")# Expected: True
print(f"app_data_3, threshold 1%: {check_overall_red_status(app_data_3, red_threshold_percentage=1.0)}")    # Expected: False

print("\n--- Test specific_apps_must_be_red ---")
critical_set_1 = ["CriticalApp1", "CriticalApp2"]
print(f"app_data_4, critical {critical_set_1}: {check_overall_red_status(app_data_4, specific_apps_must_be_red=critical_set_1)}") # Expected: True
print(f"app_data_5, critical {critical_set_1}: {check_overall_red_status(app_data_5, specific_apps_must_be_red=critical_set_1)}") # Expected: False (CriticalApp2 is Amber)

critical_set_2 = ["AppA"]
print(f"app_data_2, critical {critical_set_2}: {check_overall_red_status(app_data_2, specific_apps_must_be_red=critical_set_2)}") # Expected: True (AppA is Red)
print(f"app_data_3, critical {critical_set_2}: {check_overall_red_status(app_data_3, specific_apps_must_be_red=critical_set_2)}") # Expected: False (AppA is Green)

critical_set_3_one_missing = ["CriticalApp1", "MissingCriticalApp"]
print(f"app_data_4, critical {critical_set_3_one_missing}: {check_overall_red_status(app_data_4, specific_apps_must_be_red=critical_set_3_one_missing)}") # Expected: False (MissingCriticalApp not in data_4 and thus not red)
print(f"app_data_6, critical {critical_set_1}: {check_overall_red_status(app_data_6, specific_apps_must_be_red=critical_set_1)}") # Expected: False (CriticalApp2 not present in data_6)


print("\n--- Test combinations and default behavior (no red criteria means effectively green) ---")
# No "Red" criteria specified, should return False if not all are Red by some implicit rule.
# The current logic ORs the conditions. If none are met, it's False.
print(f"app_data_1 (all red), no criteria: {check_overall_red_status(app_data_1)}") # Expected: False (because no specific 'red' condition was asked for)
print(f"app_data_2 (mixed), no criteria: {check_overall_red_status(app_data_2)}")   # Expected: False
print(f"app_data_3 (no red), no criteria: {check_overall_red_status(app_data_3)}")  # Expected: False

# Example: trigger if 50% OR AppA is Red
print(f"app_data_2, 50% OR AppA is Red: {check_overall_red_status(app_data_2, red_threshold_percentage=50.0, specific_apps_must_be_red=['AppA'])}") # Expected: True (both are true)

app_data_7 = [ # 1 red out of 4 (25%)
    {"appname": "AppA", "status": "Red"},
    {"appname": "AppB", "status": "Green"},
    {"appname": "AppC", "status": "Green"},
    {"appname": "AppD", "status": "Green"},
]
print(f"app_data_7, threshold 30% OR AppA is Red: {check_overall_red_status(app_data_7, red_threshold_percentage=30.0, specific_apps_must_be_red=['AppA'])}") # Expected: True (because AppA is Red)

print("\n--- Test empty list ---")
print(f"Empty list: {check_overall_red_status([])}") # Expected: False
print(f"Empty list, all_must_be_red: {check_overall_red_status([], all_must_be_red=True)}") # Expected: False
print(f"Empty list, threshold 50%: {check_overall_red_status([], red_threshold_percentage=50.0)}") # Expected: False
print(f"Empty list, specific apps: {check_overall_red_status([], specific_apps_must_be_red=['AppX'])}") # Expected: False
