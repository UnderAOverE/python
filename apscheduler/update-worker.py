import random
import time
import threading
import socket
import os
from datetime import datetime
from fastapi import FastAPI
from pymongo import MongoClient

# Initialize MongoDB client
client = MongoClient("mongodb://localhost:27017/")

# Define the list of worker names
worker_names = ["worker1", "worker2", "worker3", "worker4"]

app = FastAPI()

def get_worker_from_db():
    """
    Get a worker that is not currently running in the DB.
    """
    workers_collection = client["monitoring_db"]["workers"]
    available_workers = [worker for worker in worker_names if worker not in [w['worker_id'] for w in workers_collection.find({"status": "running"})]]
    if available_workers:
        worker_id = random.choice(available_workers)
        return worker_id
    else:
        return None  # If no available workers, return None

worker_id = get_worker_from_db()  # Get worker when the app starts

def register_worker_in_mongo():
    """
    Periodically register or update the worker's metadata in MongoDB.
    """
    while True:
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
                    "last_heartbeat": datetime.utcnow()
                }
            },
            upsert=True
        )
        time.sleep(10)  # Update every 10 seconds

@app.on_event("shutdown")
def shutdown_event():
    """
    When the application shuts down, update worker status to 'stopped'.
    """
    db = client["monitoring_db"]
    workers_collection = db["workers"]

    # Update status to 'stopped' when app shuts down
    workers_collection.update_one(
        {"worker_id": worker_id},
        {"$set": {"status": "stopped", "last_heartbeat": datetime.utcnow()}}
    )
    print(f"Worker {worker_id} marked as stopped.")

# Start the background registration thread
threading.Thread(target=register_worker_in_mongo, daemon=True).start()

@app.get("/all-workers")
def get_all_workers():
    """
    Get the status of all registered workers from MongoDB.
    """
    workers_collection = client["monitoring_db"]["workers"]
    workers = list(workers_collection.find({}, {"_id": 0}))
    return {"workers": workers}

@app.get("/health")
def health_check():
    """
    Endpoint to check worker health.
    """
    return {
        "worker_id": worker_id,
        "host": socket.gethostname(),
        "status": "running",
        "process_id": os.getpid()
    }
