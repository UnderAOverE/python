from datetime import datetime, timedelta
import pytz

# Sample inputs
start_day = "mon"
end_day = "tue"
start_time = "23:00"
end_time = "02:00"
from_timezone = "US/Central"
to_timezone = "Asia/Singapore"

# Day mappings for convenience
days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
day_map = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday"
}

def get_next_occurrence(day_name):
    """Get the next occurrence of a specific weekday from today."""
    today = datetime.now().date()
    day_index = days_of_week.index(day_map[day_name.lower()])
    today_index = today.weekday()
    days_until_next = (day_index - today_index + 7) % 7
    if days_until_next == 0:  # If today is the desired day, set to next week
        days_until_next = 7
    return today + timedelta(days=days_until_next)

def convert_day_time(start_day, start_time, end_day, end_time, from_tz, to_tz):
    # Standardize day names
    start_day = day_map[start_day.lower()]
    end_day = day_map[end_day.lower()]

    # Initialize timezone objects
    from_tz = pytz.timezone(from_tz)
    to_tz = pytz.timezone(to_tz)

    # Calculate the next occurrences of the start and end days
    start_date = get_next_occurrence(start_day)
    end_date = get_next_occurrence(end_day)

    # Create timezone-aware datetime objects with start and end times
    start_datetime = from_tz.localize(datetime.combine(start_date, datetime.strptime(start_time, "%H:%M").time()))
    end_datetime = from_tz.localize(datetime.combine(end_date, datetime.strptime(end_time, "%H:%M").time()))

    # Convert to the target timezone
    start_in_target = start_datetime.astimezone(to_tz)
    end_in_target = end_datetime.astimezone(to_tz)

    # Format output
    start_day_out = days_of_week[start_in_target.weekday()]
    end_day_out = days_of_week[end_in_target.weekday()]
    start_time_out = start_in_target.strftime("%H:%M")
    end_time_out = end_in_target.strftime("%H:%M")

    return start_day_out, end_day_out, start_time_out, end_time_out

# Usage
convert_day_time(start_day, start_time, end_day, end_time, from_timezone, to_timezone)
