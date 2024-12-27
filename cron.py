from pymongo import MongoClient
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

# Fetch the document for the current month and day
document = collection.find_one({
    f"scheduler_details.{current_month}.{current_day}": {"$exists": True}
})

if document:
    # Extract the day's objects
    day_objects = document['scheduler_details'][current_month][current_day]
    current_time = current_date.time()

    # To store the closest upcoming object
    closest_object = None
    closest_time_difference = None

    for obj in day_objects:
        # Check if status is null or "PENDING"
        if obj['status'] in [None, 'PENDING']:
            # Parse service_level_time considering the timezone
            service_time = datetime.strptime(obj['service_level_time'], '%H:%M').time()
            timezone = pytz.timezone(obj['timezone']) if obj['timezone'] else pytz.UTC
            service_datetime = datetime.combine(current_date, service_time).astimezone(timezone)
            
            # Mark as "FAILED" if past time
            if service_datetime.time() < current_time:
                obj['status'] = 'FAILED'
                collection.update_one(
                    {f"scheduler_details.{current_month}.{current_day}.service_level_time": obj['service_level_time']},
                    {"$set": {"status": "FAILED"}}
                )
            else:
                # Find the closest object
                time_difference = service_datetime - datetime.now(tz=timezone)
                if closest_time_difference is None or time_difference < closest_time_difference:
                    closest_time_difference = time_difference
                    closest_object = obj

    # Print the closest object
    print(closest_object)
else:
    print("No matching document found.")