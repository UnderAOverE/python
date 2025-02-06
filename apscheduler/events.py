from typing import Optional

import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler import events
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import BaseModel, Field
import pymongo
import time
from datetime import datetime

# Constants for MongoDB Configuration
MONGODB_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
DATABASE_NAME = "scheduler_db"
EVENT_COLLECTION_NAME = "scheduler_events"

# 1. Pydantic Model for Events
class SchedulerEvent(BaseModel):
    event_id: int
    event_type: str  # Examples: "job_added", "job_executed", "job_error"
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    job_run_time: Optional[str] = None  # strftime("%Y-%m-%d %H:%M:%S")
    exception: Optional[str] = None
    log_datetime: datetime = Field(default_factory=datetime.utcnow) # Add field for time.

# 2. Function to Store Events in MongoDB
async def store_event_in_mongodb(event: SchedulerEvent, mongodb_uri: str, database_name: str, collection_name: str):
    """Stores a scheduler event in MongoDB."""
    try:
        client = pymongo.MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db[collection_name]
        collection.insert_one(event.model_dump())  # Insert the Pydantic model as a dict
        client.close()
    except Exception as e:
        print(f"Error storing event in MongoDB: {e}")

# 3. Event Listener Function
async def scheduler_event_listener(event, app: FastAPI): # Access app.state here
    """Listens for APScheduler events and stores them in MongoDB."""
    event_data = {
        "event_id": event.code,
        "event_type": events.EVENT_NAMES[event.code],
        "job_id": event.job_id,
    }

    if hasattr(event, 'job'):
       event_data["job_name"] = event.job.name
    if hasattr(event, 'scheduled_run_time'):
       event_data["job_run_time"] = time.strftime("%Y-%m-%d %H:%M:%S", event.scheduled_run_time.timetuple())
    if event.exception:
        event_data["exception"] = str(event.exception)
        # Add the logging date
        scheduler_event = SchedulerEvent(**event_data)  # Create Pydantic model
        await store_event_in_mongodb(scheduler_event, MONGODB_URI, DATABASE_NAME, EVENT_COLLECTION_NAME)

async def create_example(app: FastAPI):
    """Create sample item"""
    # Add example job to create an index if index does not exist.
    await store_event_in_mongodb(SchedulerEvent(**{"event_id": 0,"event_type": "Start"}), MONGODB_URI, DATABASE_NAME, EVENT_COLLECTION_NAME)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan event handler."""

    client = pymongo.MongoClient(MONGODB_URI)
    # You also need to set some options to client before you can call get_database
    db = client[DATABASE_NAME]
    db[EVENT_COLLECTION_NAME].create_index([("log_datetime", pymongo.ASCENDING)], expireAfterSeconds=86400)  #TTL setup code.
    
    scheduler = AsyncIOScheduler(timezone="UTC")
    # Add it to scheduler for the database to have index before you call.
    app.state.mongodb_client = client
    app.state.scheduler = scheduler

    # Add it to scheduler first
    scheduler.add_job(create_example, args=[app])

    # Add other jobs here
    def my_job():
        print("My job is running!")

    scheduler.add_job(my_job, 'interval', seconds=10)

    # Add event listener
    job_events_mask = (
            events.EVENT_JOB_ADDED |
            events.EVENT_JOB_MODIFIED |
            events.EVENT_JOB_REMOVED |
            events.EVENT_JOB_SUBMITTED |
            events.EVENT_JOB_MAX_INSTANCES |
            events.EVENT_JOB_EXECUTED |
            events.EVENT_JOB_ERROR |
            events.EVENT_JOB_MISSED
    )

    scheduler.add_listener(lambda event: asyncio.create_task(scheduler_event_listener(event, app)), job_events_mask)

    # Check for index after run.

    # 3. Start the scheduler
    scheduler.start()

    yield # this runs before shutdown

    # Stop function for app
    # shut down scheduler first.

    print("Shutting down, closing client")
    scheduler.shutdown()

    client.close()

# 4. Set Up the FastAPI App
app = FastAPI(lifespan=lifespan)
