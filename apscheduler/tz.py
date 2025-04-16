import datetime
import time

# --- Option 1: Get start/end for TODAY in UTC ---
print("--- UTC Example ---")
today_utc = datetime.datetime.now(datetime.timezone.utc).date()

# Start of the day (00:00:00 UTC)
start_of_day_utc_dt = datetime.datetime.combine(
    today_utc,
    datetime.time.min, # 00:00:00
    tzinfo=datetime.timezone.utc
)
start_of_day_epoch = int(start_of_day_utc_dt.timestamp())

# End of the day (effectively start of the *next* day, 00:00:00 UTC)
# This is often preferred for comparisons (timestamp >= start and timestamp < end)
next_day_utc = today_utc + datetime.timedelta(days=1)
end_of_day_utc_dt = datetime.datetime.combine(
    next_day_utc,
    datetime.time.min, # 00:00:00 of next day
    tzinfo=datetime.timezone.utc
)
# Using start of next day as the exclusive upper bound
end_of_day_epoch_exclusive = int(end_of_day_utc_dt.timestamp())

# Alternative: End of the day (23:59:59.999999 UTC) - Inclusive
# Less common for ranges, potential precision issues
almost_end_of_day_utc_dt = datetime.datetime.combine(
    today_utc,
    datetime.time.max, # 23:59:59.999999
    tzinfo=datetime.timezone.utc
)
end_of_day_epoch_inclusive = int(almost_end_of_day_utc_dt.timestamp())

print(f"UTC Date: {today_utc}")
print(f"Start of Day (UTC) Datetime: {start_of_day_utc_dt}")
print(f"Start of Day (UTC) Epoch: {start_of_day_epoch}")
print(f"End of Day (UTC) Datetime (Exclusive): {end_of_day_utc_dt}")
print(f"End of Day (UTC) Epoch (Exclusive): {end_of_day_epoch_exclusive}")
print(f"End of Day (UTC) Datetime (Inclusive): {almost_end_of_day_utc_dt}")
print(f"End of Day (UTC) Epoch (Inclusive): {end_of_day_epoch_inclusive}")
print(f"Duration check (Exclusive - Start): {end_of_day_epoch_exclusive - start_of_day_epoch} seconds") # Should be 86400

print("\n--- System Local Timezone Example ---")
# --- Option 2: Get start/end for TODAY in system's local timezone ---
# WARNING: System's local timezone can be ambiguous or change (e.g., DST)
#          It's generally better to specify an explicit timezone (see pytz/zoneinfo below)

today_local = datetime.date.today() # Gets date based on system clock/tz settings

# Create naive datetime objects first
start_of_day_local_naive = datetime.datetime.combine(today_local, datetime.time.min)
end_of_day_local_naive = datetime.datetime.combine(today_local, datetime.time.max)
next_day_local_naive = datetime.datetime.combine(today_local + datetime.timedelta(days=1), datetime.time.min)

# Make them timezone-aware using the *system's* local timezone interpretation
# This is implicitly done by .timestamp() if the object is naive, but it's less explicit.
# Let's get the epoch directly from naive (less safe, relies on system interpretation)
start_of_day_local_epoch = int(start_of_day_local_naive.timestamp())
end_of_day_local_epoch_inclusive = int(end_of_day_local_naive.timestamp())
end_of_day_local_epoch_exclusive = int(next_day_local_naive.timestamp())


# To be more robust, find the system's offset *at that specific time*
# This is still tricky without a proper timezone database like pytz or zoneinfo
# Example: Get current offset (may not be correct for start/end if DST transition happens)
local_tz = datetime.datetime.now().astimezone().tzinfo

start_of_day_local_dt = start_of_day_local_naive.replace(tzinfo=local_tz) # Approximate
end_of_day_local_dt_exclusive = next_day_local_naive.replace(tzinfo=local_tz) # Approximate


print(f"Local Date: {today_local}")
print(f"Local System TZ (approx): {local_tz}")
print(f"Start of Day (Local) Epoch: {start_of_day_local_epoch}")
print(f"End of Day (Local) Epoch (Exclusive): {end_of_day_local_epoch_exclusive}")
print(f"End of Day (Local) Epoch (Inclusive): {end_of_day_local_epoch_inclusive}")
print(f"Duration check (Exclusive - Start): {end_of_day_local_epoch_exclusive - start_of_day_local_epoch} seconds") # May NOT be 86400 due to DST!
