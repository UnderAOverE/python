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
