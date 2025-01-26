# app/common/scheduler.py
from typing import Protocol, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.job import Job
import logging
import traceback
import inspect

logger = logging.getLogger("my_logger")


class Scheduler(Protocol):
    def add_job(self, func: Callable, **kwargs: Any) -> None: ...
    def remove_job(self, job_id: str) -> None: ...
    def get_jobs(self) -> list[Any]: ...
    def start(self) -> None: ...
    def shutdown(self) -> None: ...
    def update_job(self, job_id: str, **kwargs: Any) -> None: ...

class APSchedulerWrapper:
    """Wrapper class for APScheduler."""
    _instance = None

    def __new__(cls, uri: str) -> "APSchedulerWrapper":
        if cls._instance is None:
            cls._instance = super(APSchedulerWrapper, cls).__new__(cls)
            jobstore = MongoDBJobStore(database="mydb", client_kwargs={"host": uri})
            cls._instance._scheduler = AsyncIOScheduler(jobstores={'default': jobstore})
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
        """Starts the scheduler and process jobs."""
        try:
           self.start()
           self._process_all_jobs()
        except Exception as e:
           logger.error(f"Error during start and process jobs {e}")
def get_scheduler(uri: str) -> Scheduler:
    """Returns the scheduler instance."""
    return APSchedulerWrapper(uri)
