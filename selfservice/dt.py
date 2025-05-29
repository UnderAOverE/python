from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError # Python 3.9+

class DateTimeConverter:
    """
    A utility class for handling and converting datetime objects and ISO 8601 strings,
    with a focus on timezone awareness and conversions.

    Internally, all datetimes are stored as timezone-aware UTC.
    """

    def __init__(self, dt_input, source_tz_str: str = None):
        """
        Initializes the DateTimeConverter.

        Args:
            dt_input (datetime | str):
                The input datetime. Can be:
                - A timezone-aware datetime object.
                - A naive datetime object (will be interpreted based on source_tz_str or assumed UTC).
                - An ISO 8601 string (e.g., "2023-10-27T10:00:00Z", "2023-10-27T15:30:00+05:30",
                  "2023-10-27T10:00:00").
            source_tz_str (str, optional):
                The IANA timezone string (e.g., "America/New_York", "Europe/London", "UTC")
                for the dt_input IF dt_input is a naive datetime object or an ISO string
                without an explicit offset.
                If dt_input is naive and source_tz_str is None, UTC is assumed.
                If dt_input is an aware datetime or an ISO string with an offset, this is ignored.

        Raises:
            ValueError: If dt_input is invalid or source_tz_str is an invalid timezone.
            ZoneInfoNotFoundError: If source_tz_str is not a recognized timezone.
        """
        self._aware_dt_utc: datetime = None

        if isinstance(dt_input, str):
            try:
                # Try parsing as ISO 8601
                parsed_dt = datetime.fromisoformat(dt_input.replace("Z", "+00:00")) # Handles 'Z' for UTC
                if parsed_dt.tzinfo is None or parsed_dt.tzinfo.utcoffset(parsed_dt) is None:
                    # ISO string was naive (e.g., "2023-10-27T10:00:00")
                    if source_tz_str:
                        source_tz = ZoneInfo(source_tz_str)
                        self._aware_dt_utc = parsed_dt.replace(tzinfo=source_tz).astimezone(timezone.utc)
                    else:
                        # Assume UTC for naive ISO string if no source_tz_str
                        self._aware_dt_utc = parsed_dt.replace(tzinfo=timezone.utc)
                else:
                    # ISO string was already aware
                    self._aware_dt_utc = parsed_dt.astimezone(timezone.utc)
            except ValueError as e:
                raise ValueError(f"Invalid ISO 8601 string format: {dt_input}. Error: {e}")
            except ZoneInfoNotFoundError:
                raise ZoneInfoNotFoundError(f"Source timezone '{source_tz_str}' not found.")

        elif isinstance(dt_input, datetime):
            if dt_input.tzinfo is None or dt_input.tzinfo.utcoffset(dt_input) is None:
                # Naive datetime object
                if source_tz_str:
                    try:
                        source_tz = ZoneInfo(source_tz_str)
                        # For naive datetimes, .replace() is used to attach timezone info.
                        # Be mindful of ambiguous times during DST transitions if not UTC.
                        self._aware_dt_utc = dt_input.replace(tzinfo=source_tz).astimezone(timezone.utc)
                    except ZoneInfoNotFoundError:
                        raise ZoneInfoNotFoundError(f"Source timezone '{source_tz_str}' not found.")
                else:
                    # Assume UTC for naive datetime if no source_tz_str
                    self._aware_dt_utc = dt_input.replace(tzinfo=timezone.utc)
            else:
                # Aware datetime object
                self._aware_dt_utc = dt_input.astimezone(timezone.utc)
        else:
            raise ValueError("Input must be a datetime object or an ISO 8601 string.")

    @classmethod
    def from_naive(cls, naive_dt: datetime, original_tz_str: str):
        """
        Class method to create an instance from a naive datetime object,
        explicitly specifying its original timezone.

        Args:
            naive_dt (datetime): The naive datetime object.
            original_tz_str (str): The IANA timezone string of the naive_dt.

        Returns:
            DateTimeConverter: A new instance.
        """
        if not isinstance(naive_dt, datetime) or \
           (naive_dt.tzinfo is not None and naive_dt.tzinfo.utcoffset(naive_dt) is not None):
            raise ValueError("Input 'naive_dt' must be a naive datetime object.")
        return cls(naive_dt, source_tz_str=original_tz_str)

    def to_datetime_object(self, target_tz_str: str = "UTC") -> datetime:
        """
        Converts the internal UTC datetime to a datetime object in the target timezone.

        Args:
            target_tz_str (str, optional): The IANA timezone string for the output.
                                         Defaults to "UTC".

        Returns:
            datetime: A timezone-aware datetime object in the target timezone.

        Raises:
            ZoneInfoNotFoundError: If target_tz_str is not a recognized timezone.
        """
        if target_tz_str.upper() == "UTC":
            return self._aware_dt_utc
        try:
            target_tz = ZoneInfo(target_tz_str)
            return self._aware_dt_utc.astimezone(target_tz)
        except ZoneInfoNotFoundError:
            raise ZoneInfoNotFoundError(f"Target timezone '{target_tz_str}' not found.")

    def to_iso_string(self, target_tz_str: str = "UTC", custom_format: str = None) -> str:
        """
        Converts the internal datetime to an ISO 8601 string or custom format string,
        optionally in a specified target timezone.

        Args:
            target_tz_str (str, optional): The IANA timezone string for the output.
                                         Defaults to "UTC". If None, uses UTC.
            custom_format (str, optional): A strftime format string.
                                          If None, standard ISO 8601 format is used.
                                          Example: "%Y-%m-%d %H:%M:%S %Z%z"

        Returns:
            str: The formatted datetime string.

        Raises:
            ZoneInfoNotFoundError: If target_tz_str is not a recognized timezone.
        """
        target_dt = self.to_datetime_object(target_tz_str or "UTC") # Default to UTC if None
        if custom_format:
            return target_dt.strftime(custom_format)
        else:
            return target_dt.isoformat()

    def __str__(self) -> str:
        """Returns the ISO 8601 string representation in UTC."""
        return self.to_iso_string()

    def __repr__(self) -> str:
        """Returns a representative string of the object."""
        return f"<DateTimeConverter({self.to_iso_string()})>"

# --- Usage Examples ---
if __name__ == "__main__":
    print("--- Initialization Examples ---")
    # 1. From ISO string with Z (UTC)
    dtc1 = DateTimeConverter("2023-10-27T10:00:00Z")
    print(f"1. ISO with Z: {dtc1} (UTC internally)")

    # 2. From ISO string with offset
    dtc2 = DateTimeConverter("2023-10-27T15:30:00+05:30") # IST
    print(f"2. ISO with offset (+05:30): {dtc2} (UTC internally)")

    # 3. From naive ISO string (assumed UTC by default)
    dtc3_utc = DateTimeConverter("2023-10-27T10:00:00")
    print(f"3a. Naive ISO (assumed UTC): {dtc3_utc}")

    # 4. From naive ISO string with specified source timezone
    dtc3_ny = DateTimeConverter("2023-10-27T10:00:00", source_tz_str="America/New_York")
    print(f"3b. Naive ISO (source 'America/New_York'): {dtc3_ny}")

    # 5. From naive datetime object (assumed UTC by default)
    naive_dt_obj = datetime(2023, 11, 5, 1, 30, 0) # Ambiguous time in NY due to DST fallback
    dtc4_utc = DateTimeConverter(naive_dt_obj)
    print(f"4a. Naive datetime obj (assumed UTC): {dtc4_utc}")

    # 6. From naive datetime object with specified source timezone
    # For ambiguous times like DST fallbacks, use fold=1 if you mean the second occurrence
    # naive_dt_ny_fold = datetime(2023, 11, 5, 1, 30, 0, fold=1) # For second 1:30 AM on DST end
    # dtc4_ny = DateTimeConverter(naive_dt_ny_fold, source_tz_str="America/New_York")
    dtc4_ny = DateTimeConverter(naive_dt_obj, source_tz_str="America/New_York")
    print(f"4b. Naive datetime obj (source 'America/New_York' at {naive_dt_obj}): {dtc4_ny}")
    # Let's see what 1:30 AM New York on Nov 5, 2023 is
    print(f"   Its representation in NY: {dtc4_ny.to_datetime_object('America/New_York')}")


    # 7. From aware datetime object
    aware_dt_obj = datetime.now(ZoneInfo("Europe/Paris"))
    dtc5 = DateTimeConverter(aware_dt_obj)
    print(f"5. Aware datetime obj (source '{aware_dt_obj.tzinfo}'): {dtc5}")

    # 8. Using from_naive classmethod
    dtc6 = DateTimeConverter.from_naive(datetime(2024, 7, 4, 12, 0, 0), "America/Los_Angeles")
    print(f"6. From_naive (source 'America/Los_Angeles'): {dtc6}")


    print("\n--- Conversion Examples (using dtc3_ny: 2023-10-27 10:00 America/New_York) ---")
    print(f"Original for examples: 10:00 AM on Oct 27, 2023, New York time")
    print(f"Internal UTC: {dtc3_ny._aware_dt_utc}") # Accessing internal for demo

    # - Convert to datetime object in different timezones
    dt_ny = dtc3_ny.to_datetime_object("America/New_York")
    dt_london = dtc3_ny.to_datetime_object("Europe/London")
    dt_tokyo = dtc3_ny.to_datetime_object("Asia/Tokyo")
    dt_utc_obj = dtc3_ny.to_datetime_object("UTC") # or dtc3_ny.to_datetime_object()

    print(f"As datetime in New York: {dt_ny} ({dt_ny.tzname()})")
    print(f"As datetime in London:   {dt_london} ({dt_london.tzname()})")
    print(f"As datetime in Tokyo:    {dt_tokyo} ({dt_tokyo.tzname()})")
    print(f"As datetime in UTC:      {dt_utc_obj} ({dt_utc_obj.tzname()})")

    # - Convert to ISO string in different timezones
    iso_ny = dtc3_ny.to_iso_string("America/New_York")
    iso_london = dtc3_ny.to_iso_string("Europe/London")
    iso_utc = dtc3_ny.to_iso_string() # Defaults to UTC

    print(f"As ISO string in New York: {iso_ny}")
    print(f"As ISO string in London:   {iso_london}")
    print(f"As ISO string in UTC:      {iso_utc}")

    # - Convert to custom formatted string in a specific timezone
    custom_fmt_ny = dtc3_ny.to_iso_string(
        target_tz_str="America/New_York",
        custom_format="%A, %B %d, %Y %I:%M:%S %p %Z"
    )
    custom_fmt_kolkata = dtc3_ny.to_iso_string(
        target_tz_str="Asia/Kolkata",
        custom_format="%Y/%m/%d %H-%M-%S %z (%Z)"
    )
    print(f"Custom format in New York: {custom_fmt_ny}")
    print(f"Custom format in Kolkata:  {custom_fmt_kolkata}")

    # Handling DST: Example - A time in US/Eastern that crosses DST
    # March 10, 2024 is when DST starts in US (clocks spring forward from 2 AM to 3 AM)
    # So, 2:30 AM on March 10, 2024, does not exist in America/New_York
    # Let's take 1:30 AM before DST and 3:30 AM after
    
    # 1:30 AM on March 10, 2024 (before DST)
    dt_before_dst_naive = datetime(2024, 3, 10, 1, 30, 0)
    converter_before_dst = DateTimeConverter(dt_before_dst_naive, "America/New_York")
    print(f"\nBefore DST (NY 2024-03-10 01:30): {converter_before_dst.to_iso_string('America/New_York')} -> UTC: {converter_before_dst}")

    # 3:30 AM on March 10, 2024 (after DST)
    dt_after_dst_naive = datetime(2024, 3, 10, 3, 30, 0)
    converter_after_dst = DateTimeConverter(dt_after_dst_naive, "America/New_York")
    print(f"After DST (NY 2024-03-10 03:30): {converter_after_dst.to_iso_string('America/New_York')} -> UTC: {converter_after_dst}")
    
    # Conversion to a country observing DST differently, e.g. Europe/London
    # DST in London starts March 31, 2024
    print(f"  NY 1:30 AM EST -> London: {converter_before_dst.to_iso_string('Europe/London')}")
    print(f"  NY 3:30 AM EDT -> London: {converter_after_dst.to_iso_string('Europe/London')}")

    # Example of trying to create an invalid time (e.g. 2:30 AM on DST start in NY)
    try:
        # datetime.replace(tzinfo) does not raise error for non-existent times on its own.
        # The error would typically surface during an astimezone() call if the combination is impossible.
        # However, zoneinfo and astimezone are quite robust.
        # When you do dt.replace(tzinfo=ZoneInfo("America/New_York")), if dt is 2:30 AM during DST jump,
        # it might be interpreted as one of the valid hours, or its `fold` attribute might be set.
        # For example, datetime(2024,3,10,2,30,0).replace(tzinfo=ZoneInfo("America/New_York"))
        # would result in datetime.datetime(2024, 3, 10, 2, 30, tzinfo=zoneinfo.ZoneInfo(key='America/New_York'))
        # and its .utcoffset() might be ambiguous without `fold`.
        # `astimezone` will resolve it correctly.
        
        problematic_naive = datetime(2024, 3, 10, 2, 30, 0)
        # If we make it aware like this, then convert to UTC, zoneinfo handles it
        # by effectively shifting it to what it would be if it were a valid time (often 3:30 EDT in this case).
        dt_problem = problematic_naive.replace(tzinfo=ZoneInfo("America/New_York"))
        print(f"\nTrying naive 2024-03-10 02:30 in NY directly with replace: {dt_problem}")
        print(f"  Its offset: {dt_problem.utcoffset()}") # Will likely show as -04:00 (EDT)
        print(f"  In UTC: {dt_problem.astimezone(timezone.utc)}") # Shows 2024-03-10 06:30:00+00:00 (same as 3:30 AM EDT)
        
        # Our class will also handle this by first making it aware, then converting to UTC.
        converter_problematic = DateTimeConverter(problematic_naive, "America/New_York")
        print(f"Class handling naive 2024-03-10 02:30 (NY): {converter_problematic.to_iso_string('America/New_York')}")
        print(f"  Internal UTC for problematic: {converter_problematic}")
        
    except Exception as e:
        print(f"Error with problematic time: {e}")

    print("\n--- Error Handling Examples ---")
    try:
        DateTimeConverter("invalid-date-string")
    except ValueError as e:
        print(f"Caught expected error: {e}")

    try:
        DateTimeConverter(12345)
    except ValueError as e:
        print(f"Caught expected error: {e}")

    try:
        DateTimeConverter("2023-10-27T10:00:00", "Invalid/Timezone")
    except ZoneInfoNotFoundError as e:
        print(f"Caught expected error: {e}")
