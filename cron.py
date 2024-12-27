from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import pytz

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database_name']
collection = db['your_collection_name']

# List of timezones to process
timezones = ["EST", "CST", "UTC", "CET", "SGT", "Japan", "BET"]

# Define sector, environment, and active fields for filtering
sector = "desired_sector"
environment = "desired_environment"
active = True

# Batch job processing
bulk_updates = []  # To store all bulk updates across timezones

for tz_name in timezones:
    # Get timezone object
    timezone = pytz.timezone(tz_name)
    
    # Calculate current date and time in the specified timezone
    current_datetime = datetime.now(timezone)
    current_month = current_datetime.strftime('%B')  # e.g., "January"
    current_day = current_datetime.strftime('%A')    # e.g., "Monday"
    current_time = current_datetime.time()  # Current time in that timezone

    # Query to find matching documents for the current timezone
    query = {
        "sector": sector,
        "environment": environment,
        "active": active,
        f"scheduler_details.{current_month}.{current_day}": {"$exists": True}
    }

    # Cursor to iterate over matching documents
    documents = collection.find(query)

    for doc in documents:
        day_objects = doc['scheduler_details'][current_month][current_day]

        # To store the closest upcoming object for this timezone
        closest_object = None
        closest_time_difference = None

        for obj in day_objects:
            # Check if status is null or "PENDING"
            if obj['status'] in [None, 'PENDING']:
                # Parse service_level_time considering the timezone
                service_time = datetime.strptime(obj['service_level_time'], '%H:%M').time()

                # Convert service_level_time to a full datetime
                service_datetime = datetime.combine(current_datetime.date(), service_time).astimezone(timezone)

                # Handle multi_day logic
                if obj.get('multi_day', False):
                    # For multi-day jobs, only mark as FAILED if more than 48 hours have passed
                    if current_datetime > service_datetime + timedelta(hours=48):
                        obj['status'] = 'FAILED'
                        bulk_updates.append(
                            UpdateOne(
                                {"_id": doc['_id'], f"scheduler_details.{current_month}.{current_day}.service_level_time": obj['service_level_time']},
                                {"$set": {f"scheduler_details.{current_month}.{current_day}.$.status": "FAILED"}}
                            )
                        )
                else:
                    # For non-multi-day jobs, handle cross-day time zone changes
                    if service_datetime < current_datetime:
                        # If the service time has already passed in the given timezone, mark as FAILED
                        obj['status'] = 'FAILED'
                        bulk_updates.append(
                            UpdateOne(
                                {"_id": doc['_id'], f"scheduler_details.{current_month}.{current_day}.service_level_time": obj['service_level_time']},
                                {"$set": {f"scheduler_details.{current_month}.{current_day}.$.status": "FAILED"}}
                            )
                        )
                    else:
                        # Find the closest object to the current time
                        time_difference = service_datetime - current_datetime
                        if closest_time_difference is None or time_difference < closest_time_difference:
                            closest_time_difference = time_difference
                            closest_object = obj

        # Log or process the closest object if found
        if closest_object:
            print(f"Closest object for document {doc['_id']} in timezone {tz_name}: {closest_object}")

# Execute all bulk updates in a single batch for efficiency
if bulk_updates:
    collection.bulk_write(bulk_updates)
    print(f"{len(bulk_updates)} statuses updated.")