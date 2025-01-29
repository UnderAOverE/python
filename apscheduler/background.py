from fastapi import FastAPI, BackgroundTasks
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import os
import socket
import datetime
from pymongo import MongoClient

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")

# Define the list of worker names
worker_names = ["worker1", "worker2", "worker3", "worker4"]

# FastAPI app
app = FastAPI()

# Create the APScheduler instance
scheduler = BackgroundScheduler()

# Job to register or update worker metadata in MongoDB
def register_worker_in_mongo():
    worker_id = "worker1"  # Assign worker ID as appropriate (this could be random from list)
    db = client["monitoring_db"]
    workers_collection = db["workers"]

    # Upsert worker status
    workers_collection.update_one(
        {"worker_id": worker_id},
        {
            "$set": {
                "worker_id": worker_id,
                "host": socket.gethostname(),
                "status": "running",
                "process_id": os.getpid(),
                "last_heartbeat": datetime.datetime.utcnow()
            }
        },
        upsert=True
    )
    print(f"Worker {worker_id} status updated.")

# Background task to run periodically
@app.on_event("startup")
async def startup_event():
    # Start the scheduler for periodic tasks
    scheduler.add_job(register_worker_in_mongo, "interval", seconds=10)  # Update every 10 seconds
    scheduler.start()

@app.on_event("shutdown")
def shutdown_event():
    # Graceful shutdown of APScheduler
    scheduler.shutdown()
    print("Scheduler shut down gracefully.")

# Route to start background task manually
@app.get("/start-background-task")
async def start_background_task(background_tasks: BackgroundTasks):
    # You can run other background tasks as well via FastAPI's BackgroundTasks
    background_tasks.add_task(register_worker_in_mongo)  # Manually add task to run in background
    return {"message": "Background task started"}

# Health check route to see current worker info
@app.get("/health")
async def health_check():
    worker_id = "worker1"  # This can be dynamically fetched from your worker list

    # Get worker status from MongoDB
    db = client["monitoring_db"]
    workers_collection = db["workers"]
    worker_data = workers_collection.find_one({"worker_id": worker_id}, {"_id": 0})

    if worker_data:
        return worker_data
    else:
        return {"status": "Worker not found"}
