from typing import List, Dict, Any, Optional

def get_app_active_status(
    data_list: List[Dict[str, Any]],
    target_name: str,
    target_appname: str
) -> Optional[bool]:
    """
    Searches a list of dictionaries to find an item matching 'target_name',
    then searches its 'impactedapps' for an app matching 'target_appname',
    and returns the 'active' status of that app.

    Args:
        data_list: The list of dictionaries to search. Each dictionary should have
                   a 'name' (str) and 'impactedapps' (list of dicts).
                   Each dict in 'impactedapps' should have 'appname' (str) and
                   'active' (bool).
        target_name: The 'name' of the service/item to find in the main list.
        target_appname: The 'appname' to find within the 'impactedapps' of the
                        matched service/item.

    Returns:
        The boolean 'active' status if found, otherwise None.
    """
    for item in data_list:
        if item.get("name") == target_name:
            # Found the item with the target_name
            impacted_apps = item.get("impactedapps", []) # Default to empty list if key missing
            if not isinstance(impacted_apps, list):
                # Handle cases where impactedapps might not be a list (data integrity check)
                # You might want to log a warning here
                continue

            for app in impacted_apps:
                if isinstance(app, dict) and app.get("appname") == target_appname:
                    # Found the app with the target_appname
                    active_status = app.get("active")
                    if isinstance(active_status, bool):
                        return active_status
                    else:
                        # 'active' key exists but is not a boolean, or key is missing
                        # You might want to log a warning or handle this differently
                        return None # Or raise an error, or return a default
            # If we reach here, the target_appname was not found in this item's impactedapps
            return None # Or indicate app not found in this service specifically
    # If we reach here, the target_name was not found in the data_list
    return None

# Example Usage:
data = [
    {
        "name": "ServiceAlpha",
        "impactedapps": [
            {"appname": "WebApp1", "active": True},
            {"appname": "MobileAppX", "active": False},
            {"appname": "AdminTool", "active": True},
        ]
    },
    {
        "name": "ServiceBeta",
        "impactedapps": [
            {"appname": "WebApp1", "active": False},
            {"appname": "DataProcessor", "active": True},
        ]
    },
    {
        "name": "ServiceGamma", # This service has no WebApp1
        "impactedapps": [
            {"appname": "MobileAppY", "active": True},
        ]
    },
    {
        "name": "ServiceDelta", # This service has WebApp1 but 'active' is missing
        "impactedapps": [
            {"appname": "WebApp1", "some_other_key": "value"},
        ]
    },
     {
        "name": "ServiceEpsilon", # This service has WebApp1 but 'active' is not bool
        "impactedapps": [
            {"appname": "WebApp1", "active": "yes_string"},
        ]
    }
]

# Test cases
print(f"ServiceAlpha, WebApp1: {get_app_active_status(data, 'ServiceAlpha', 'WebApp1')}")  # Expected: True
print(f"ServiceAlpha, MobileAppX: {get_app_active_status(data, 'ServiceAlpha', 'MobileAppX')}") # Expected: False
print(f"ServiceBeta, WebApp1: {get_app_active_status(data, 'ServiceBeta', 'WebApp1')}")    # Expected: False
print(f"ServiceBeta, DataProcessor: {get_app_active_status(data, 'ServiceBeta', 'DataProcessor')}")# Expected: True

# Cases where matches are not found or data is imperfect
print(f"ServiceAlpha, NonExistentApp: {get_app_active_status(data, 'ServiceAlpha', 'NonExistentApp')}") # Expected: None
print(f"NonExistentService, WebApp1: {get_app_active_status(data, 'NonExistentService', 'WebApp1')}") # Expected: None
print(f"ServiceGamma, WebApp1: {get_app_active_status(data, 'ServiceGamma', 'WebApp1')}") # Expected: None (WebApp1 not in ServiceGamma)
print(f"ServiceDelta, WebApp1 (active missing): {get_app_active_status(data, 'ServiceDelta', 'WebApp1')}") # Expected: None
print(f"ServiceEpsilon, WebApp1 (active not bool): {get_app_active_status(data, 'ServiceEpsilon', 'WebApp1')}") # Expected: None

# Case where 'impactedapps' might be missing or not a list
data_malformed = [
    {"name": "ServiceOmega"} # No impactedapps key
]
print(f"ServiceOmega (no impactedapps): {get_app_active_status(data_malformed, 'ServiceOmega', 'WebApp1')}") # Expected: None

data_malformed_not_list = [
    {"name": "ServiceKappa", "impactedapps": "not_a_list"}
]
print(f"ServiceKappa (impactedapps not list): {get_app_active_status(data_malformed_not_list, 'ServiceKappa', 'WebApp1')}") # Expected: None
