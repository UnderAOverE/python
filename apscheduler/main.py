# app/common/__init__.py
# app/common/scheduler.py
from typing import Protocol, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.job import Job
import logging
import traceback
from threading import Lock
from app.common.database import get_database_client
from fastapi import Depends

logger = logging.getLogger("my_logger")


class Scheduler(Protocol):
    def add_job(self, func: Callable, **kwargs: Any) -> None: ...
    def remove_job(self, job_id: str) -> None: ...
    def get_jobs(self) -> list[Any]: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def update_job(self, job_id: str, **kwargs: Any) -> None: ...

class APSchedulerWrapper:
    _instance = None  # Holds the single instance
    _lock = Lock()    # Thread-safe lock for initialization

    def __new__(cls, uri: str, *args, **kwargs):
        if not cls._instance:
             with cls._lock:  # Ensure thread-safe initialization
                if not cls._instance:
                  cls._instance = super().__new__(cls)
                  cls._instance.mongo_client = get_database_client(uri).get_client()  # MongoDB client
                  cls._instance.job_store = MongoDBJobStore(client=cls._instance.mongo_client, database="mydb", collection="jobs")
                  cls._instance._scheduler = AsyncIOScheduler(
                      jobstores={"default": cls._instance.job_store},
                      executors={"default": ThreadPoolExecutor(10)},  # Adjust thread pool size as needed
                      job_defaults={
                          "misfire_grace_time": None,  # No grace time for misfired jobs
                          "coalesce": False,          # Don't combine missed executions
                          "max_instances": 1          # Prevent overlapping job executions
                      },
                   )
                  cls._instance.scheduler_running = False  # Track the running state
                  cls._instance._jobs_loaded = False
        return cls._instance

    def add_job(self, func: Callable, **kwargs: Any) -> None:
        """Adds a job to the scheduler."""
        self._scheduler.add_job(func, **kwargs)

    def remove_job(self, job_id: str) -> None:
        """Removes a job from the scheduler."""
        self._scheduler.remove_job(job_id)

    def get_jobs(self) -> list[Job]:
        """Gets all scheduled jobs."""
        return self._scheduler.get_jobs()

    def update_job(self, job_id: str, **kwargs: Any) -> None:
        """Updates a scheduled job."""
        self._scheduler.modify_job(job_id, **kwargs)
    
    def start(self) -> None:
        """Starts the scheduler."""
        try:
          self._scheduler.start()
        except Exception as e:
          logger.error(f"Error starting scheduler: {e}")

    def shutdown(self) -> None:
        """Shuts down the scheduler."""
        try:
            self._scheduler.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down scheduler: {e}")

    def _resolve_job_function(self, job: Job) -> Callable | None:
      """Resolve and return function, handle errors and return None if its not resolvable"""
      try:
        if isinstance(job.func, str):
          func_path = job.func.split(".")
          module_name = ".".join(func_path[:-1])
          function_name = func_path[-1]
          module = __import__(module_name, fromlist=[function_name])
          func = getattr(module, function_name)
          return func
        return job.func
      except Exception as e:
          logger.error(f"Failed to resolve job function for job id '{job.id}': {e}, {traceback.format_exc()}")
          return None

    def _process_all_jobs(self):
      """Process all the jobs and remove stale jobs"""
      jobs = self._scheduler.get_jobs()
      for job in jobs:
            func = self._resolve_job_function(job)
            if func is None:
              try:
                  logger.error(f"Job with ID {job.id} is stale due to missing function definition, marking it disabled")
                  self._scheduler.remove_job(job.id)
              except Exception as remove_e:
                logger.error(f"Failed to remove job id {job.id} {remove_e}")

    
    def start_and_process_jobs(self):
      """Start the scheduler and process jobs."""
      try:
        if not self._jobs_loaded:
           self.start()
           self._process_all_jobs()
           self._jobs_loaded = True
      except Exception as e:
         logger.error(f"Error during start and process jobs {e}")
def get_scheduler(uri: str) -> Scheduler:
    """Returns the scheduler instance."""
    return APSchedulerWrapper(uri)
# app/common/database.py
from typing import Protocol, Any
from pymongo import MongoClient
from pymongo.database import Database
from threading import Lock
_mongo_client = None
_mongo_lock = Lock()

class DatabaseClient(Protocol):
    def get_client(self) -> Database: ...

class MongoDBClient:
    """Wrapper class for MongoDB client."""
    
    def __new__(cls, uri: str) -> "MongoDBClient":
        global _mongo_client
        if _mongo_client is None:
            with _mongo_lock:
                 if _mongo_client is None:
                    _mongo_client = super(MongoDBClient, cls).__new__(cls)
                    _mongo_client.client = MongoClient(uri)
                    _mongo_client.db = _mongo_client.client.get_database("mydb")
        return _mongo_client

    def get_client(self) -> Database:
        """Returns the MongoDB client."""
        return self.db

def get_database_client(uri: str) -> DatabaseClient:
  return MongoDBClient(uri)
# app/common/monitor.py
import functools
from typing import Callable, Any, Dict, TypeVar
from datetime import datetime
from abc import ABC, abstractmethod
from app.common.database import DatabaseClient
import traceback
from pymongo.database import Database
from app.common.logger import logger
from fastapi import Depends

T = TypeVar("T")

class MonitorObserver(ABC):
  """Observer interface."""
  @abstractmethod
  def update(self, message: str, level: str, data: Dict = None) -> None:
    """Update method for observers."""
    pass

class DatabaseObserver(MonitorObserver):
    """Observer that logs to MongoDB."""

    def __init__(self, db_client: DatabaseClient):
        """Initialize with a database client."""
        self.db = db_client.get_client()
        self.collection = self.db.get_collection("monitoring")

    def update(self, message: str, level: str, data: Dict = None) -> None:
        """Log the error to MongoDB."""
        log_entry = {
            "timestamp": datetime.utcnow(),
            "level": level,
            "message": message,
            "data": data,
        }
        self.collection.insert_one(log_entry)

class EmailObserver(MonitorObserver):
    """Observer that sends emails (implementation here can use smtplib)."""
    def update(self, message: str, level: str, data: Dict = None) -> None:
      """Sends an email based on the level and message"""
      # Email Sending Implementation goes here
      print(f"Email Sent for level: {level}, message:{message} data:{data}")

class Monitor:
    """Monitors functions for errors, warnings, and fatals."""
    _instance = None

    def __new__(cls, db_client: DatabaseClient):
        if cls._instance is None:
            cls._instance = super(Monitor, cls).__new__(cls)
            cls._instance.observers = [
                DatabaseObserver(db_client),
                EmailObserver() # Add more observers here
            ]
        return cls._instance

    def attach(self, observer: MonitorObserver) -> None:
      """Attach an observer."""
      self.observers.append(observer)
    
    def detach(self, observer: MonitorObserver) -> None:
      """Detach an observer."""
      self.observers.remove(observer)
    
    def notify(self, message: str, level: str, data: Dict = None) -> None:
        """Notify all observers."""
        for observer in self.observers:
          observer.update(message, level, data)

    def log_error(self, message: str, data: Dict = None) -> None:
        """Logs an error."""
        self.notify(message, "error", data)

    def log_warning(self, message: str, data: Dict = None) -> None:
        """Logs a warning."""
        self.notify(message, "warning", data)

    def log_fatal(self, message: str, data: Dict = None) -> None:
        """Logs a fatal error."""
        self.notify(message, "fatal", data)

    def monitor(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to monitor a function."""
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_message = f"Error in {func.__name__}: {e}"
                trace = traceback.format_exc()
                self.log_error(error_message, {"traceback": trace, "args": args, "kwargs": kwargs})
                raise
        return wrapper

def get_monitor(db_client: DatabaseClient) -> Monitor:
  return Monitor(db_client)
# app/batch/__init__.py
# app/batch/app1/__init__.py
# app/batch/app1/job109.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str, arg2: int) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
        arg2 (int): An integer argument
    Returns:
        None
    Raises:
        ValueError: If arg2 is negative.
    """
    print(f"Job 109 running with args: {arg1}, {arg2}")
    if arg2 < 0:
        raise ValueError("arg2 must be a non negative value")

    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job109"})

    if lock_document:
      print("job 109 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job109"})

    try:
        print(f"job 109 processing the task {arg1} and {arg2}")
    finally:
        # Release the lock
        lock_collection.delete_one({"job_id": "job109"})
# app/batch/app1/job2.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database
import time

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str, arg2: int) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
        arg2 (int): An integer argument
    Returns:
        None
    Raises:
        ValueError: If arg2 is greater than 10.
    """
    print(f"Job 2 running with args: {arg1}, {arg2}")
    if arg2 > 10:
       raise ValueError("arg2 must be a value less than 10")
    
    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job2"})

    if lock_document:
      print("job 2 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job2"})

    try:
        time.sleep(2)
        print(f"job 2 processing the task {arg1} and {arg2}")
    finally:
        # Release the lock
        lock_collection.delete_one({"job_id": "job2"})
# app/batch/app2/__init__.py
# app/batch/app2/job33345.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database
import time

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str, arg2: int, arg3: str) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
        arg2 (int): An integer argument
        arg3 (str): A string argument
    Returns:
        None
    Raises:
      ValueError: If arg2 is not even.
    """
    print(f"Job 33345 running with args: {arg1}, {arg2}, {arg3}")
    if arg2 % 2 != 0:
        raise ValueError("arg2 must be an even value")

    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job33345"})

    if lock_document:
      print("job 33345 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job33345"})

    try:
        time.sleep(2)
        print(f"job 33345 processing the task {arg1} and {arg2} and {arg3}")
    finally:
        # Release the lock
        lock_collection.delete_one({"job_id": "job33345"})
# app/batch/app2/job78.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database
import time

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
    Returns:
        None
    """
    print(f"Job 78 running with args: {arg1}")

    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job78"})

    if lock_document:
      print("job 78 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job78"})
    
    try:
      time.sleep(2)
      print(f"job 78 processing the task {arg1}")
    finally:
       # Release the lock
       lock_collection.delete_one({"job_id": "job78"})
# app/batch/app2/job0909.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database
import time
import random

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str, arg2: int) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
        arg2 (int): An integer argument
    Returns:
        None
    Raises:
      ValueError: If arg2 is equal to 0.
    """
    print(f"Job 0909 running with args: {arg1}, {arg2}")
    if arg2 == 0:
        raise ValueError("arg2 cannot be 0")
    
    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job0909"})

    if lock_document:
      print("job 0909 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job0909"})

    try:
        time.sleep(2)
        print(f"job 0909 processing the task {arg1} and {arg2}")
        rand_num = random.random()
        if rand_num < 0.2:
            raise Exception(f"random exception {rand_num}")
    finally:
      # Release the lock
        lock_collection.delete_one({"job_id": "job0909"})
# app/batch/app2/job7.py
from app.common.monitor import get_monitor
from app.common.database import get_database_client
from pymongo.database import Database
import time

monitor = get_monitor(get_database_client("mongodb://localhost:27017"))
db: Database = get_database_client("mongodb://localhost:27017").get_client()

@monitor.monitor
def main(arg1: str, arg2: str, arg3: bool) -> None:
    """
    This is the main function of the job
    Args:
        arg1 (str): A string argument
        arg2 (str): A string argument
        arg3 (bool): A boolean argument
    Returns:
        None
    Raises:
      ValueError: If arg3 is false.
    """
    print(f"Job 7 running with args: {arg1}, {arg2}, {arg3}")
    if not arg3:
        raise ValueError("arg3 cannot be false")
    
    # Check if a lock exists
    lock_collection = db.get_collection("locks")
    lock_document = lock_collection.find_one({"job_id": "job7"})

    if lock_document:
      print("job 7 is locked")
      return

    # Acquire the lock
    lock_collection.insert_one({"job_id": "job7"})

    try:
        time.sleep(2)
        print(f"job 7 processing the task {arg1} and {arg2} and {arg3}")
    finally:
      # Release the lock
        lock_collection.delete_one({"job_id": "job7"})
# app/api/main.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from typing import Any, Dict
from app.api.utils.database import get_database_client
from app.common.scheduler import Scheduler, get_scheduler
from app.common.monitor import get_monitor
from pymongo.database import Database
from contextlib import asynccontextmanager
import traceback
from app.common.logger import logger
from fastapi import Depends

# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes resources for the application."""
    db_client = get_database_client("mongodb://localhost:27017")
    scheduler: Scheduler = get_scheduler("mongodb://localhost:27017")
    app.mongodb_client = db_client.get_client()
    app.scheduler = scheduler
    monitor = get_monitor(db_client)
    app.monitor = monitor
    scheduler.start_and_process_jobs()
    yield
    scheduler.shutdown()

# FastAPI initialization
app = FastAPI(lifespan=lifespan)

# Template Directory
templates = Jinja2Templates(directory="app/api/template")

# Static Directory
app.mount("/static", StaticFiles(directory="app/api/template/static"), name="static")

# Error Handlers
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handles FastAPI HTTP Exceptions."""
    error_message = f"HTTP Exception: {exc.status_code} - {exc.detail}"
    if exc.status_code == 404:
       return templates.TemplateResponse("404.html", {"request": request}, status_code=404)
    elif exc.status_code == 400:
       app.monitor.log_error(error_message)
       return templates.TemplateResponse("400.html", {"request": request, "message": exc.detail}, status_code=400)
    else:
      app.monitor.log_error(error_message)
      return templates.TemplateResponse("500.html", {"request": request, "message": "Internal Server Error"}, status_code=500)

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
  """Handles unhandled exceptions in the app."""
  error_message = f"Unhandled Exception: {exc}"
  trace = traceback.format_exc()
  app.monitor.log_fatal(error_message, {"traceback": trace})
  return templates.TemplateResponse("500.html", {"request": request, "message": "Internal Server Error"}, status_code=500)


# Routes
@app.get("/status", response_class=HTMLResponse)
async def status(request: Request):
  """Returns health check page with app routes."""
  routes = [route.path for route in app.routes]
  return templates.TemplateResponse("health.html", {"request": request, "routes": routes})

@app.get("/", response_class=HTMLResponse)
async def list_jobs(request: Request):
  """Returns jobs page."""
  jobs = app.scheduler.get_jobs()
  return templates.TemplateResponse("jobs.html", {"request": request, "jobs": jobs})

# Include routers
from app.api.routes.jobs import onboard as onboard_route
from app.api.routes.jobs import remove as remove_route
from app.api.routes.jobs import update as update_route
from app.api.routes.jobs import list as list_route

app.include_router(onboard_route.router)
app.include_router(remove_route.router)
app.include_router(update_route.router)
app.include_router(list_route.router)
# app/api/utils/__init__.py
# app/api/utils/database.py
from app.common.database import DatabaseClient, get_database_client
from fastapi import Depends

def get_database(uri: str) -> DatabaseClient:
    """Returns the Database client."""
    return get_database_client(uri)
# app/api/utils/scheduler.py
from app.common.scheduler import Scheduler, get_scheduler
from fastapi import Depends


def get_scheduler_client(uri: str) -> Scheduler:
    """Returns the Scheduler client."""
    return get_scheduler(uri)
# app/api/utils/api_utils.py
from typing import Any, Dict, List, Optional
from fastapi import HTTPException, status
from app.common.monitor import Monitor
from app.common.database import DatabaseClient
from app.common.scheduler import Scheduler
from pymongo.database import Database
from apscheduler.job import Job
from datetime import datetime
from fastapi import Depends

class JobManager:
    """Manages job operations."""
    def __init__(self, db_client: DatabaseClient = Depends(lambda: get_database("mongodb://localhost:27017")), scheduler: Scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")), monitor: Monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))):
        """Initialize with dependencies."""
        self.db: Database = db_client.get_client()
        self.scheduler = scheduler
        self.monitor = monitor
        self.jobs_collection = self.db.get_collection("jobs")

    def _get_job_data(self, job_id: str) -> Dict:
      """Helper function to get the job document by its ID"""
      job_data = self.jobs_collection.find_one({"job_id": job_id})
      if not job_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job with id '{job_id}' not found.")
      return job_data

    def onboard_job(self, job_data: Dict) -> Dict:
      """Onboards a new job."""
      job_id = job_data.get("job_id")
      if not job_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job ID is required.")
      if self.jobs_collection.find_one({"job_id": job_id}):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Job with id '{job_id}' already exists.")
      
      job_details = job_data.get("job_details")
      if not job_details or not job_details.get("func"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job function details are required.")
        
      try:
        func_path = job_details["func"].split(".")
        module_name = ".".join(func_path[:-1])
        function_name = func_path[-1]
        module = __import__(module_name, fromlist=[function_name])
        func = getattr(module, function_name)
        
        trigger_type = job_data.get("trigger", {}).get("type", None)
        trigger_args = job_data.get("trigger", {}).get("args", {})

        if trigger_type is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Trigger type is required.")

        if trigger_type == "cron":
          self.scheduler.add_job(func=func, id=job_id, **trigger_args)
        elif trigger_type == "date":
            trigger_args["run_date"] = datetime.fromisoformat(trigger_args["run_date"])
            self.scheduler.add_job(func=func, id=job_id, **trigger_args)
        elif trigger_type == "interval":
          self.scheduler.add_job(func=func, id=job_id, **trigger_args)
        else:
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Trigger type must be one of cron, date or interval.")

        self.jobs_collection.insert_one(job_data)
        return {"message": f"Job with id '{job_id}' onboarded successfully", "job": job_data}
      except ImportError as e:
          self.monitor.log_error(f"Failed to import job function: {e}")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid job function path. {e}")
      except AttributeError as e:
          self.monitor.log_error(f"Job function not found: {e}")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Job function not found. {e}")
      except ValueError as e:
        self.monitor.log_error(f"Invalid trigger arguments: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid trigger arguments: {e}")
      except Exception as e:
          self.monitor.log_error(f"Error onboarding job: {e}")
          raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error onboarding job: {e}")
    
    def remove_job(self, job_id: str) -> Dict:
      """Removes a job."""
      try:
        self._get_job_data(job_id)
        self.scheduler.remove_job(job_id)
        self.jobs_collection.delete_one({"job_id": job_id})
        return {"message": f"Job with id '{job_id}' removed successfully."}
      except Exception as e:
          self.monitor.log_error(f"Error removing job: {e}")
          raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error removing job: {e}")
    
    def update_job(self, job_id: str, job_data: Dict) -> Dict:
      """Updates an existing job."""
      try:
          existing_job_data = self._get_job_data(job_id)
          job_details = job_data.get("job_details")
          if not job_details or not job_details.get("func"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Job function details are required.")

          func_path = job_details["func"].split(".")
          module_name = ".".join(func_path[:-1])
          function_name = func_path[-1]
          module = __import__(module_name, fromlist=[function_name])
          func = getattr(module, function_name)

          trigger_type = job_data.get("trigger", {}).get("type", None)
          trigger_args = job_data.get("trigger", {}).get("args", {})

          if trigger_type is None:
              raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Trigger type is required.")

          if trigger_type == "cron":
            self.scheduler.update_job(job_id=job_id, func=func, **trigger_args)
          elif trigger_type == "date":
              trigger_args["run_date"] = datetime.fromisoformat(trigger_args["run_date"])
              self.scheduler.update_job(job_id=job_id, func=func, **trigger_args)
          elif trigger_type == "interval":
              self.scheduler.update_job(job_id=job_id, func=func, **trigger_args)
          else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Trigger type must be one of cron, date or interval.")

          merged_job_data = {**existing_job_data, **job_data}
          self.jobs_collection.update_one({"job_id": job_id}, {"$set": merged_job_data})
          return {"message": f"Job with id '{job_id}' updated successfully.", "job": merged_job_data}
      except ImportError as e:
          self.monitor.log_error(f"Failed to import job function: {e}")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid job function path: {e}")
      except AttributeError as e:
          self.monitor.log_error(f"Job function not found: {e}")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Job function not found: {e}")
      except ValueError as e:
        self.monitor.log_error(f"Invalid trigger arguments: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid trigger arguments: {e}")
      except Exception as e:
        self.monitor.log_error(f"Error updating job: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error updating job: {e}")
    
    def list_jobs(self, query_params: Optional[Dict] = None) -> List[Dict]:
      """Lists jobs based on query parameters if provided otherwise return all jobs."""
      try:
        if query_params:
          jobs = list(self.jobs_collection.find(query_params, {"_id": 0}))
        else:
          jobs = list(self.jobs_collection.find({}, {"_id": 0}))
        return jobs
      except Exception as e:
        self.monitor.log_error(f"Error listing jobs: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error listing jobs: {e}")
# app/api/template/css/style.css
body {
    font
	# app/api/template/css/style.css (Continued)
    font-family: sans-serif;
}

.container {
    width: 80%;
    margin: 20px auto;
}

h1, h2 {
    color: #333;
}

table {
    width: 100%;
    border-collapse: collapse;
}

th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
}

th {
    background-color: #f2f2f2;
}

a {
  text-decoration: none;
  color: blue;
}

a:hover {
  text-decoration: underline;
  color: blue;
}
# app/api/template/health.html
<!DOCTYPE html>
<html>
<head>
    <title>Health Check</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
  <div class="container">
        <h1>Application is Up and Running!</h1>
        <h2>Available Routes:</h2>
        <ul>
          {% for route in routes %}
              <li><a href="{{ route }}">{{ route }}</a></li>
          {% endfor %}
        </ul>
  </div>
</body>
</html>
# app/api/template/jobs.html
<!DOCTYPE html>
<html>
<head>
    <title>Job Status</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
  <div class="container">
      <h1>Scheduled Jobs</h1>
      <table>
        <thead>
            <tr>
              <th>Job ID</th>
              <th>Function</th>
              <th>Next Run Time</th>
              <th>Trigger</th>
            </tr>
        </thead>
        <tbody>
          {% for job in jobs %}
              <tr>
                <td>{{ job.id }}</td>
                <td>{{ job.func_ref }}</td>
                <td>{{ job.next_run_time }}</td>
                <td>{{ job.trigger }}</td>
              </tr>
          {% endfor %}
        </tbody>
      </table>
  </div>
</body>
</html>
# app/api/template/404.html
<!DOCTYPE html>
<html>
<head>
    <title>404 Not Found</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
  <div class="container">
      <h1>404 Not Found</h1>
      <p>The requested resource was not found on this server.</p>
  </div>
</body>
</html>
# app/api/template/400.html
<!DOCTYPE html>
<html>
<head>
    <title>400 Bad Request</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
  <div class="container">
      <h1>400 Bad Request</h1>
      <p>Error Message: {{ message }}</p>
  </div>
</body>
</html>
# app/api/template/500.html
<!DOCTYPE html>
<html>
<head>
    <title>500 Internal Server Error</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
  <div class="container">
      <h1>500 Internal Server Error</h1>
      <p>An internal server error occurred.</p>
      <p>Error Message: {{ message }}</p>
  </div>
</body>
</html>
# app/api/template/js
# app/api/models/__init__.py
# app/api/models/jobs/__init__.py
# app/api/models/common.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Dict, Any, List, Optional

class CronTrigger(BaseModel):
    type: str = "cron"
    args: Dict[str, Any] = Field(
        default={},
        description="Cron trigger arguments. Provide only necessary arguments",
        examples=[
            {"year": "2024"}, #run every year
            {"month": "1"}, # run every january
            {"day": "1"}, #run every 1st of the month
            {"week": "1"}, # run every 1st week of month
            {"day_of_week": "mon"}, #run every monday
            {"hour": "10"}, #run every hour 10
            {"minute": "30"},  # run every minute 30
            {"second": "0"}, #run every second 0
            {"start_date":"2024-01-01T10:00:00", "end_date":"2025-01-01T10:00:00"}, # run between these two dates
            {"timezone": "UTC"}, #run in utc timezone
            {"minute": "*/5"},  # Every 5 minutes
            {"hour": 10, "minute": 30},  # At 10:30 AM
            {"day_of_week": "mon-fri", "hour": 9} #Every weekday at 9am
        ]
    )
    model_config = ConfigDict(
      json_schema_extra={
        "description":"A cron-based trigger to run the job",
        "properties": {
          "type": {
            "enum": ["cron"],
          },
          "args":{
           "description": "Cron arguments",
            "type":"object",
            "properties":{
              "year":{
                "type":"string",
                "description":"Year to trigger on",
                "default": "*",
              },
             "month":{
                "type":"string",
                "description":"Month to trigger on",
                "default":"*"
             },
             "day":{
                 "type":"string",
                "description":"Day of month to trigger on",
                "default":"*"
             },
             "week":{
                "type":"string",
                "description":"Week of the year to trigger on",
                "default":"*"
             },
             "day_of_week":{
                 "type":"string",
                 "description":"Day of week to trigger on",
                  "default":"*"
             },
             "hour":{
                "type":"string",
                "description":"Hour to trigger on",
                 "default":"*"
             },
             "minute":{
               "type":"string",
                "description":"Minute to trigger on",
                 "default":"*"
             },
              "second":{
                "type":"string",
                "description":"Second to trigger on",
                "default":"*"
              },
             "start_date":{
                "type":"string",
                "format":"date-time",
                "description":"Start date to trigger on",
              },
              "end_date":{
                "type":"string",
                 "format":"date-time",
                "description":"End date to trigger on",
              },
              "timezone":{
                "type":"string",
                 "description":"Timezone to trigger on",
                 "default":"UTC"
             }
            }
          }
        }
      }
    )

class DateTrigger(BaseModel):
  type: str = "date"
  args: Dict[str, Any] = Field(
      default={},
      description="Date trigger arguments, specifically a run_date.",
        examples=[
            {"run_date": "2024-08-27T10:30:00", "timezone": "UTC"}
          ]
  )
  model_config = ConfigDict(
    json_schema_extra={
      "description":"A date-based trigger to run the job once on specified date.",
      "properties":{
        "type":{
           "enum": ["date"]
        },
        "args":{
          "type":"object",
           "properties":{
              "run_date":{
                  "type":"string",
                   "format":"date-time",
                  "description":"Date on which the job should be executed."
             },
              "timezone":{
                "type":"string",
                 "description":"Timezone to trigger on",
                 "default":"UTC"
             }
           }
         }
      }
   }
  )

class IntervalTrigger(BaseModel):
  type: str = "interval"
  args: Dict[str, Any] = Field(
    default={},
    description="Interval trigger arguments, provide only one of them. ",
        examples=[
            {"seconds": 10, "start_date":"2024-01-01T10:00:00", "end_date":"2025-01-01T10:00:00", "timezone": "UTC"}, # Every 10 seconds
            {"minutes": 5},  # Every 5 minutes
            {"hours": 1}, # Every hour
            {"days": 1}, # Every day
            {"weeks": 1} # Every week
        ]
  )
  model_config = ConfigDict(
    json_schema_extra={
      "description":"An interval-based trigger to run the job repeatedly.",
      "properties":{
        "type":{
           "enum": ["interval"]
         },
        "args":{
          "type":"object",
           "properties":{
             "weeks":{
               "type":"integer",
                "description":"Number of weeks to trigger on"
             },
              "days":{
               "type":"integer",
                "description":"Number of days to trigger on"
             },
              "hours":{
                "type":"integer",
                 "description":"Number of hours to trigger on"
             },
              "minutes":{
                "type":"integer",
                 "description":"Number of minutes to trigger on"
             },
              "seconds":{
                "type":"integer",
                 "description":"Number of seconds to trigger on"
             },
             "start_date":{
                "type":"string",
                "format":"date-time",
                 "description":"Start date to trigger on",
             },
              "end_date":{
                "type":"string",
                "format":"date-time",
                "description":"End date to trigger on",
             },
              "timezone":{
                "type":"string",
                 "description":"Timezone to trigger on",
                 "default":"UTC"
             }
           }
        }
      }
    }
  )

class Trigger(BaseModel):
    __root__: CronTrigger | DateTrigger | IntervalTrigger
    model_config = ConfigDict(
      json_schema_extra={
        "description":"A trigger to run the job, can be of type cron, date or interval."
      }
    )
# app/api/models/jobs/onboard.py
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from app.api.models.common import Trigger

class JobDetails(BaseModel):
    func: str = Field(description="Full path to the job's function, e.g., app.batch.app1.job109.main")
    args: Optional[List] = Field(default=None, description="List of positional arguments for the job function")
    kwargs: Optional[Dict] = Field(default=None, description="Dictionary of keyword arguments for the job function")


class OnboardJobModel(BaseModel):
    job_id: str = Field(description="Unique identifier for the job")
    title: str = Field(description="Title of the Job")
    description: str = Field(description="Description of the Job")
    environment: Optional[str] = Field(default=None, description="Environment where the job runs")
    sector: Optional[str] = Field(default=None, description="Sector where the job runs")
    region: Optional[str] = Field(default=None, description="Region where the job runs")
    job_details: JobDetails = Field(description="Details of the job")
    trigger: Trigger = Field(description="Trigger configuration for the job")
# app/api/models/jobs/remove.py
from pydantic import BaseModel, Field

class RemoveJobModel(BaseModel):
    job_id: str = Field(description="Unique identifier of the job to remove")

# app/api/models/jobs/update.py
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from app.api.models.common import Trigger, JobDetails


class UpdateJobModel(BaseModel):
    job_id: str = Field(description="Unique identifier for the job to update")
    title: Optional[str] = Field(default=None, description="Title of the Job")
    description: Optional[str] = Field(default=None, description="Description of the Job")
    environment: Optional[str] = Field(default=None, description="Environment where the job runs")
    sector: Optional[str] = Field(default=None, description="Sector where the job runs")
    region: Optional[str] = Field(default=None, description="Region where the job runs")
    job_details: Optional[JobDetails] = Field(default=None, description="Details of the job")
    trigger: Optional[Trigger] = Field(default=None, description="Trigger configuration for the job")
# app/api/models/jobs/list.py
from pydantic import BaseModel, Field
from typing import Optional

class ListJobQueryModel(BaseModel):
    job_id: Optional[str] = Field(default=None, description="Filter by Job ID")
    title: Optional[str] = Field(default=None, description="Filter by Job Title")
    environment: Optional[str] = Field(default=None, description="Filter by Environment")
    sector: Optional[str] = Field(default=None, description="Filter by Sector")
    region: Optional[str] = Field(default=None, description="Filter by Region")

# app/api/routes/__init__.py
# app/api/routes/jobs/__init__.py
# app/api/routes/jobs/onboard.py
from fastapi import APIRouter, Depends, HTTPException, status
from app.api.models.jobs.onboard import OnboardJobModel
from app.api.utils.api_utils import JobManager
from app.api.utils.database import get_database
from app.api.utils.scheduler import get_scheduler_client
from app.common.monitor import get_monitor
from typing import Dict, Any

router = APIRouter()

@router.post("/api/jobs/onboard", response_model=Dict[str, Any], status_code=status.HTTP_201_CREATED)
async def onboard_job(job_data: OnboardJobModel,
                        db = Depends(lambda: get_database("mongodb://localhost:27017")),
                        scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")),
                        monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))):
    """Onboards a new job."""
    job_manager = JobManager(db, scheduler, monitor)
    return job_manager.onboard_job(job_data.dict())
# app/api/routes/jobs/remove.py
from fastapi import APIRouter, Depends, HTTPException, status
from app.api.models.jobs.remove import RemoveJobModel
from app.api.utils.api_utils import JobManager
from app.api.utils.database import get_database
from app.api.utils.scheduler import get_scheduler_client
from app.common.monitor import get_monitor
from typing import Dict

router = APIRouter()

@router.delete("/api/jobs/remove", response_model=Dict[str, str])
async def remove_job(job_data: RemoveJobModel,
                       db = Depends(lambda: get_database("mongodb://localhost:27017")),
                       scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")),
                       monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))):
    """Removes a job."""
    job_manager = JobManager(db, scheduler, monitor)
    return job_manager.remove_job(job_data.job_id)
# app/api/routes/jobs/update.py
from fastapi import APIRouter, Depends, HTTPException, status
from app.api.models.jobs.update import UpdateJobModel
from app.api.utils.api_utils import JobManager
from app.api.utils.database import get_database
from app.api.utils.scheduler import get_scheduler_client
from app.common.monitor import get_monitor
from typing import Dict, Any

router = APIRouter()

@router.put("/api/jobs/update", response_model=Dict[str, Any])
async def update_job(job_data: UpdateJobModel,
                        db = Depends(lambda: get_database("mongodb://localhost:27017")),
                        scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")),
                        monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))):
    """Updates a job."""
    job_manager = JobManager(db, scheduler, monitor)
    return job_manager.update_job(job_data.job_id, job_data.dict(exclude_none=True))
# app/api/routes/jobs/list.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from app.api.models.jobs.list import ListJobQueryModel
from app.api.utils.api_utils import JobManager
from app.api.utils.database import get_database
from app.api.utils.scheduler import get_scheduler_client
from app.common.monitor import get_monitor
from typing import List, Dict, Optional, Any

router = APIRouter()

@router.get("/api/jobs/list", response_model=List[Dict[str, Any]])
async def list_jobs_get(
    job_id: Optional[str] = Query(None, description="Filter by Job ID"),
    title: Optional[str] = Query(None, description="Filter by Job Title"),
    environment: Optional[str] = Query(None, description="Filter by Environment"),
    sector: Optional[str] = Query(None, description="Filter by Sector"),
    region: Optional[str] = Query(None, description="Filter by Region"),
    db = Depends(lambda: get_database("mongodb://localhost:27017")),
    scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")),
    monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))
):
    """Lists jobs with GET method."""
    query_params = {k: v for k, v in locals().items() if k in ListJobQueryModel.__fields__.keys() and v is not None}
    job_manager = JobManager(db, scheduler, monitor)
    return job_manager.list_jobs(query_params)

@router.post("/api/jobs/list", response_model=List[Dict[str, Any]])
async def list_jobs_post(
    query_params: Optional[ListJobQueryModel] = Body(default=None, description="Query parameters for filtering jobs"),
    db = Depends(lambda: get_database("mongodb://localhost:27017")),
    scheduler = Depends(lambda: get_scheduler_client("mongodb://localhost:27017")),
    monitor = Depends(lambda: get_monitor(get_database("mongodb://localhost:27017")))
):
    """Lists jobs with POST method."""
    job_manager = JobManager(db, scheduler, monitor)
    if query_params:
      return job_manager.list_jobs(query_params.dict(exclude_none=True))
    else:
      return job_manager.list_jobs()
# app/common/logger.py
import logging.config
import json
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """Custom formatter to log in JSON."""
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "timestamp": self.formatTime(record, self.datefmt),
        }
        if record.exc_info:
          log_data["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, 'extra'):
           log_data.update(record.extra)
        return json.dumps(log_data, default=str)

logger_config = {
    "version": 1,
    "formatters": {
        "json": {
            "()": "app.common.logger.JSONFormatter",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "level": "DEBUG",
        },
    },
    "loggers": {
        "my_logger": {
          "level":"DEBUG",
          "handlers": ["console"]
        }
    },
}

logging.config.dictConfig(logger_config)

logger = logging.getLogger("my_logger")
