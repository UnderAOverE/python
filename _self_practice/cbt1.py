from typing import List, Dict, Optional, Set, Tuple

async def check_overall_item_color_status(
    item_statuses: List[Dict[str, str]],
    target_color: str,
    threshold_percentage: Optional[float] = None,
    specific_items_must_be_color: Optional[List[str]] = None,
) -> Tuple[bool, Set[str]]:
    """
    Checks if an overall status for a target_color is met based on a list of item statuses.

    Precedence of conditions:
    1. If `specific_items_must_be_color` is provided:
       - Returns True if ALL items in this list exist in `item_statuses` AND match the `target_color`.
    2. Else if `threshold_percentage` is provided:
       - Returns True if the percentage of items matching `target_color` meets or exceeds this threshold.
    3. Else (neither specific_items nor threshold_percentage is provided):
       - Returns True if ALL items in `item_statuses` match the `target_color`.

    Args:
        item_statuses: A list of dictionaries, where each dict should have "Name" (str)
                       and "FinalStatus" (str, e.g., "Green", "Amber", "Red").
        target_color: The color string to check for (e.g., "red", "green").
        threshold_percentage: If set, the percentage (0.0 to 100.0) of items
                              that must match `target_color`.
        specific_items_must_be_color: If set, a list of item names. If ALL of these
                                        specific items exist and match `target_color`.

    Returns:
        Tuple[bool, Set[str]]:
            - bool: True if the criteria for the `target_color` status are met.
            - Set[str]: A set of names of items that matched the `target_color`.
    """
    if not item_statuses:
        return False, set()  # No items, so condition cannot be met

    normalized_target_color = target_color.strip().casefold()
    if not normalized_target_color:
        raise ValueError("target_color cannot be empty.")

    total_items = len(item_statuses)
    items_of_target_color_count = 0
    names_of_target_color_items: Set[str] = set()
    all_input_item_names: Set[str] = set() # To check existence for specific_items

    for item_info in item_statuses:
        item_name = item_info.get("Name") # Case-sensitive name from input
        status = item_info.get("FinalStatus", "").strip().casefold()

        if item_name: # Only consider items with names for specific checks
            all_input_item_names.add(item_name)

        if status == normalized_target_color:
            items_of_target_color_count += 1
            if item_name:
                names_of_target_color_items.add(item_name)

    # --- Condition 1: Specific items must match the target_color (Highest Precedence) ---
    if specific_items_must_be_color:
        if not specific_items_must_be_color: # Empty list means condition trivially true if no items to check
             # This behavior might need adjustment depending on requirements for an empty specific_items list.
             # For now, let's say an empty requirement list doesn't trigger True on its own.
             # If you want an empty list to mean "no specific items must be red, so proceed to next rule",
             # then the logic should skip this block if specific_items_must_be_color is empty.
             # Assuming non-empty list of specific items for this rule to be meaningful.
            pass
        else:
            all_specified_items_are_target_color = True
            for required_item_name in specific_items_must_be_color:
                if required_item_name not in all_input_item_names:
                    # A specifically required item is not even in the input list.
                    all_specified_items_are_target_color = False
                    break
                if required_item_name not in names_of_target_color_items:
                    # A specifically required item is in the input, but not of the target color.
                    all_specified_items_are_target_color = False
                    break
            
            if all_specified_items_are_target_color:
                return True, names_of_target_color_items # Return all items that were target_color
            else:
                # If specific items condition is set but not met, the overall status is False for this rule.
                # We don't fall through to percentage check in this case as per your precedence rule.
                return False, names_of_target_color_items # Return items that *were* target_color, even if rule failed

    # --- Condition 2: Percentage threshold (If specific_items condition was not met or not provided) ---
    elif threshold_percentage is not None: # Use elif for precedence
        if not (0.0 <= threshold_percentage <= 100.0):
            raise ValueError("threshold_percentage must be between 0.0 and 100.0")
        
        # Avoid division by zero if total_items is 0 (already handled by initial check, but good practice)
        calculated_percentage = (items_of_target_color_count / total_items) * 100 if total_items > 0 else 0.0
        
        if calculated_percentage >= threshold_percentage:
            return True, names_of_target_color_items
        else:
            return False, names_of_target_color_items

    # --- Condition 3: Default - ALL items must match target_color (If no other conditions specified) ---
    else:
        if items_of_target_color_count == total_items and total_items > 0: # All items are of target_color
            return True, names_of_target_color_items
        else:
            return False, names_of_target_color_items


# --- Example Usage ---
async def run_tests():
    print("--- Test Cases for check_overall_item_color_status ---")

    data_all_red = [
        {"Name": "TXN1", "FinalStatus": "Red"},
        {"Name": "TXN2", "FinalStatus": "RED"}, # Case variation
        {"Name": "TXN3", "FinalStatus": " Red "}, # Space variation
    ]
    data_mixed_some_red = [
        {"Name": "TXN1", "FinalStatus": "Red"},
        {"Name": "TXN2", "FinalStatus": "Amber"},
        {"Name": "TXN3", "FinalStatus": "Green"},
        {"Name": "TXN4", "FinalStatus": "Red"},
    ]
    data_all_green = [
        {"Name": "TXN1", "FinalStatus": "Green"},
        {"Name": "TXN2", "FinalStatus": "Green"},
    ]
    data_one_red_one_green = [
        {"Name": "CriticalTXN", "FinalStatus": "Red"},
        {"Name": "OptionalTXN", "FinalStatus": "Green"},
    ]
    data_specific_test = [
        {"Name": "Alpha", "FinalStatus": "Red"},
        {"Name": "Beta", "FinalStatus": "Red"},
        {"Name": "Gamma", "FinalStatus": "Green"},
    ]
    data_specific_test_one_missing = [
        {"Name": "Alpha", "FinalStatus": "Red"},
        # Beta is missing
        {"Name": "Gamma", "FinalStatus": "Green"},
    ]


    # Test 1: Default behavior (all must be target_color)
    print("\n1. Default (all must be target_color):")
    status, names = await check_overall_item_color_status(data_all_red, target_color="Red")
    print(f"  All Red, target Red: Status={status} (Exp: True), Names={names}") # True
    status, names = await check_overall_item_color_status(data_mixed_some_red, target_color="Red")
    print(f"  Mixed, target Red: Status={status} (Exp: False), Names={names}") # False
    status, names = await check_overall_item_color_status(data_all_green, target_color="Red")
    print(f"  All Green, target Red: Status={status} (Exp: False), Names={names}") # False
    status, names = await check_overall_item_color_status(data_all_green, target_color="Green")
    print(f"  All Green, target Green: Status={status} (Exp: True), Names={names}") # True

    # Test 2: Percentage threshold
    print("\n2. Percentage threshold:")
    # data_mixed_some_red has 2/4 = 50% Red
    status, names = await check_overall_item_color_status(data_mixed_some_red, target_color="Red", threshold_percentage=50.0)
    print(f"  Mixed (50% Red), target Red, threshold 50%: Status={status} (Exp: True), Names={names}") # True
    status, names = await check_overall_item_color_status(data_mixed_some_red, target_color="Red", threshold_percentage=60.0)
    print(f"  Mixed (50% Red), target Red, threshold 60%: Status={status} (Exp: False), Names={names}") # False
    # data_all_green has 0% Red
    status, names = await check_overall_item_color_status(data_all_green, target_color="Red", threshold_percentage=0.0) # 0% is >= 0%
    print(f"  All Green (0% Red), target Red, threshold 0%: Status={status} (Exp: True), Names={names}") # True
    status, names = await check_overall_item_color_status(data_all_green, target_color="Red", threshold_percentage=1.0)
    print(f"  All Green (0% Red), target Red, threshold 1%: Status={status} (Exp: False), Names={names}") # False

    # Test 3: Specific items must be target_color
    print("\n3. Specific items must be target_color:")
    status, names = await check_overall_item_color_status(data_one_red_one_green, target_color="Red", specific_items_must_be_color=["CriticalTXN"])
    print(f"  One Red/One Green, target Red, specific ['CriticalTXN']: Status={status} (Exp: True), Names={names}") # True
    status, names = await check_overall_item_color_status(data_one_red_one_green, target_color="Red", specific_items_must_be_color=["OptionalTXN"])
    print(f"  One Red/One Green, target Red, specific ['OptionalTXN']: Status={status} (Exp: False), Names={names}") # False (OptionalTXN is Green)
    status, names = await check_overall_item_color_status(data_one_red_one_green, target_color="Red", specific_items_must_be_color=["CriticalTXN", "OptionalTXN"])
    print(f"  One Red/One Green, target Red, specific ['CriticalTXN', 'OptionalTXN']: Status={status} (Exp: False), Names={names}") # False
    status, names = await check_overall_item_color_status(data_specific_test, target_color="Red", specific_items_must_be_color=["Alpha", "Beta"])
    print(f"  data_specific_test, target Red, specific ['Alpha', 'Beta']: Status={status} (Exp: True), Names={names}") # True
    status, names = await check_overall_item_color_status(data_specific_test, target_color="Red", specific_items_must_be_color=["Alpha", "Gamma"])
    print(f"  data_specific_test, target Red, specific ['Alpha', 'Gamma']: Status={status} (Exp: False), Names={names}") # False (Gamma is Green)
    status, names = await check_overall_item_color_status(data_specific_test_one_missing, target_color="Red", specific_items_must_be_color=["Alpha", "Beta"])
    print(f"  data_specific_test_one_missing, target Red, specific ['Alpha', 'Beta' (Beta missing)]: Status={status} (Exp: False), Names={names}") # False (Beta not in input)

    # Test 4: Precedence: specific_items overrides percentage
    print("\n4. Precedence (specific items > percentage):")
    # data_specific_test has 2/3 Red (66.6%)
    # Specific items ["Alpha", "Gamma"] fails because Gamma is Green
    status, names = await check_overall_item_color_status(
        data_specific_test,
        target_color="Red",
        threshold_percentage=60.0, # This condition would be True
        specific_items_must_be_color=["Alpha", "Gamma"] # This condition is False and takes precedence
    )
    print(f"  Specific (False) and Percentage (True), target Red: Status={status} (Exp: False), Names={names}") # False

    # Specific items ["Alpha", "Beta"] is True, percentage is also True
    status, names = await check_overall_item_color_status(
        data_specific_test,
        target_color="Red",
        threshold_percentage=60.0,
        specific_items_must_be_color=["Alpha", "Beta"]
    )
    print(f"  Specific (True) and Percentage (True), target Red: Status={status} (Exp: True), Names={names}") # True

    # Test 5: Empty list
    print("\n5. Empty list input:")
    status, names = await check_overall_item_color_status([], target_color="Red")
    print(f"  Empty list, target Red: Status={status} (Exp: False), Names={names}") # False

    # Test 6: Case-insensitivity and spaces for target_color
    print("\n6. Target color normalization:")
    status, names = await check_overall_item_color_status(data_all_red, target_color=" red ")
    print(f"  All Red, target ' red ': Status={status} (Exp: True), Names={names}") # True

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_tests())
