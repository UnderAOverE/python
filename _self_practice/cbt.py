async def check_overall_red_status(
    cbs_statuses: list[dict[str, str]],
    red_threshold_percentage: Optional[float] = None,
    specific_cbts_must_be_red: Optional[list[str]] = None,
    all_must_be_red: bool = False,
) -> tuple[bool, set[str]]:
    if not cbs_statuses:
        return False, set()
    # endif

    total_cbts = len(cbs_statuses)
    red_cbts_count = 0
    red_cbts_names = set()

    for cbt_info in cbs_statuses:
        cbt_name = cbt_info.get("Name")
        status = cbt_info.get("FinalStatus", "").strip().casefold()
        if status == "red":
            red_cbts_count += 1
            if cbt_name:
                red_cbts_names.add(cbt_name)
            # endif
        # endif
    # endfor

    # Condition 1: Specific cbts must be Red
    if specific_cbts_must_be_red:
        all_required_cbts_present_and_red = True

        for required_cbt in specific_cbts_must_be_red:
            if required_cbt not in red_cbts_names:
                # required cbt is not red; need to check if it actually exists in the input list. This handles cases where
                # required cbt might not even be in cbs_statuses
                found_in_input = any(
                    required_cbt == cbt.get("Name") for cbt in cbs_statuses
                )
                if not found_in_input:
                    continue  # Required cbt not in input list, skip further check
                # endif

                # Required cbt is NOT red, so check further for final condition
                all_required_cbts_present_and_red = False
                break
            # endif
        # endfor

        if all_required_cbts_present_and_red:
            return True, red_cbts_names
        # endif
    # endif

    # Condition 2: Percentage threshold
    if red_threshold_percentage is not None:
        if not (0 <= red_threshold_percentage <= 100.0):
            raise ValueError("red_threshold_percentage must be between 0.0 and 100.0")
        # endif

        red_cbts_percentage = (red_cbts_count / total_cbts) * 100
        if red_cbts_percentage >= red_threshold_percentage:
            return True, red_cbts_names
        # endif
    # endif

    # Condition 3: All cbts must be red — which is the default condition.
    if all_must_be_red:
        if red_cbts_count == total_cbts and red_cbts_count > 0:
            return True, red_cbts_names
        # endif
    # endif

    # If none of the "Red" conditions were met, it’s effectively “Green” or “Amber”
    return False, red_cbts_names
# endasync def