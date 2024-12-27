from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import pytz

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['your_database_name']
collection = db['your_collection_name']

# Get the current date and time
current_date = datetime.now()
current_month = current_date.strftime('%B')  # e.g., "January"
current_day = current_date.strftime('%A')    # e.g., "Monday"

# Define sector, environment, and active fields for filtering
sector = "desired_sector"
environment = "desired_environment"
active = True

# Query to find matching documents
query = {
    "sector": sector,
    "environment": environment,
    "active": active,
    f"scheduler_details.{current_month}.{current_day}": {"$exists": True}
}

# Cursor to iterate over matching documents
documents = collection.find(query)

bulk_updates = []  # To store bulk updates
timezone_cache = {}  # Cache timezone objects for efficiency

for doc in documents:
    day_objects = doc['scheduler_details'][current_month][current_day]
    current_time = current_date.time()

    # To store the closest upcoming object
    closest_object = None
    closest_time_difference = None

    for obj in day_objects:
        # Check if status is null or "PENDING"
        if obj['status'] in [None, 'PENDING']:
            # Parse service_level_time considering the timezone
            service_time = datetime.strptime(obj['service_level_time'], '%H:%M').time()
            
            # Cache timezone objects for efficiency
            timezone_name = obj['timezone'] if obj['timezone'] else "UTC"
            if timezone_name not in timezone_cache:
                timezone_cache[timezone_name] = pytz.timezone(timezone_name)
            timezone = timezone_cache[timezone_name]
            
            service_datetime = datetime.combine(current_date, service_time).astimezone(timezone)
            
            # If the status is PENDING and not past current time, return this object
            if obj['status'] == "PENDING" and service_datetime.time() >= current_time:
                print(f"Returning PENDING object for document {doc['_id']}: {obj}")
                closest_object = obj
                break  # Stop processing further objects for this document
            
            # Mark as "FAILED" if past time
            if service_datetime.time() < current_time:
                obj['status'] = 'FAILED'
                bulk_updates.append(
                    UpdateOne(
                        {"_id": doc['_id'], f"scheduler_details.{current_month}.{current_day}.service_level_time": obj['service_level_time']},
                        {"$set": {f"scheduler_details.{current_month}.{current_day}.$.status": "FAILED"}}
                    )
                )
            else:
                # Find the closest object if not yet found
                time_difference = service_datetime - datetime.now(tz=timezone)
                if closest_time_difference is None or time_difference < closest_time_difference:
                    closest_time_difference = time_difference
                    closest_object = obj

    # Optional: Log or process the closest object if found
    if closest_object:
        print(f"Closest object for document {doc['_id']}: {closest_object}")

# Execute bulk updates
if bulk_updates:
    collection.bulk_write(bulk_updates)
    print(f"{len(bulk_updates)} statuses updated.")