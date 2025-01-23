from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["scheduler_db"]
lock_collection = db["locks"]

# Lock TTL in seconds
LOCK_TTL = 600  # 10 minutes

def acquire_lock(job_name: str) -> bool:
    """
    Acquire a distributed lock for a specific job.
    Returns True if the lock was acquired successfully.
    """
    now = datetime.utcnow()
    lock = lock_collection.find_one({"job_name": job_name})

    # If lock exists and is not expired, do not acquire
    if lock and lock["expires_at"] > now:
        return False

    # Set or update the lock
    expires_at = now + timedelta(seconds=LOCK_TTL)
    lock_collection.update_one(
        {"job_name": job_name},
        {"$set": {"job_name": job_name, "expires_at": expires_at}},
        upsert=True,
    )
    return True

def release_lock(job_name: str):
    """
    Release a distributed lock for a specific job.
    """
    lock_collection.delete_one({"job_name": job_name})
# -------------------------------------------------------------------------------------------------------------------------

from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

async def cron_job_function():
    job_name = "my_cron_job"
    if acquire_lock(job_name):  # Only execute if the lock is acquired
        try:
            print("Executing job...")
            # Perform your job logic here
            await some_task()
        finally:
            release_lock(job_name)  # Release lock after execution
    else:
        print("Job is already running or locked.")

# Add the job to the scheduler
scheduler.add_job(cron_job_function, "interval", minutes=1)
scheduler.start()

# -------------------------------------------------------------------------------------------------------------------------

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from threading import Lock
from common.database import get_mongo_client


class Scheduler:
    _instance = None  # Holds the single instance
    _lock = Lock()    # Thread-safe lock for initialization

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:  # Ensure thread-safe initialization
                if not cls._instance:
                    cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):  # Avoid reinitializing the singleton
            self._initialized = True  # Mark as initialized
            self.mongo_client = get_mongo_client()  # MongoDB client
            self.job_store = MongoDBJobStore(client=self.mongo_client, database="scheduler_db", collection="jobs")
            self.scheduler = AsyncIOScheduler(
                jobstores={"default": self.job_store},
                executors={"default": ThreadPoolExecutor(10)},  # Adjust thread pool size as needed
                job_defaults={
                    "misfire_grace_time": None,  # No grace time for misfired jobs
                    "coalesce": False,          # Don't combine missed executions
                    "max_instances": 1          # Prevent overlapping job executions
                },
            )
            self.scheduler_running = False  # Track the running state

    def start(self):
        """Start the scheduler if it's not already running."""
        if not self.scheduler_running:
            self.scheduler.start()
            self.scheduler_running = True
            print("Scheduler started.")

    def stop(self):
        """Stop the scheduler if it's currently running."""
        if self.scheduler_running:
            self.scheduler.shutdown()
            self.scheduler_running = False
            print("Scheduler stopped.")

    def add_job(self, **kwargs):
        """Add a new job to the scheduler."""
        return self.scheduler.add_job(**kwargs)

    def get_jobs(self):
        """Retrieve all jobs from the scheduler."""
        return self.scheduler.get_jobs()

    def pause_job(self, job_id):
        """Pause a specific job by its ID."""
        try:
            self.scheduler.pause_job(job_id)
            print(f"Job {job_id} paused.")
        except Exception as e:
            print(f"Error pausing job {job_id}: {e}")

    def resume_job(self, job_id):
        """Resume a specific paused job by its ID."""
        try:
            self.scheduler.resume_job(job_id)
            print(f"Job {job_id} resumed.")
        except Exception as e:
            print(f"Error resuming job {job_id}: {e}")

    def running(self):
        """Check if the scheduler is currently running."""
        return self.scheduler_running


# Example Usage:
# scheduler = Scheduler()
# scheduler.start()
# scheduler.add_job(func=my_task, trigger="interval", seconds=10, id="my_job")
# scheduler.pause_job("my_job")
# scheduler.resume_job("my_job")
# scheduler.stop()


# -------------------------------------------------------------------------------------------------------------------------

def my_task():
    with client.start_session() as session:
        with session.start_transaction():
            existing_doc = db["mycollection"].find_one({"task_id": "unique_task_id"}, session=session)
            new_log_date = datetime.utcnow()
            
            if not existing_doc or existing_doc["log_date"] < new_log_date:
                db["mycollection"].update_one(
                    {"task_id": "unique_task_id"},
                    {
                        "$set": {
                            "log_date": new_log_date,
                            "status": "completed",
                        }
                    },
                    upsert=True,
                    session=session,
                )
