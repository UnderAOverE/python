# app/common/scheduler.py
from typing import Protocol, Any, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.job import Job
import logging

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
          logger.error(f"Error starting scheduler {e}")

    def shutdown(self) -> None:
        """Shuts down the scheduler."""
        try:
            self._scheduler.shutdown()
        except Exception as e:
          logger.error(f"Error shutting down scheduler {e}")

    def _process_job_removal_errors(self):
       """Process jobs with removal errors."""
       jobs = self._scheduler.get_jobs()
       for job in jobs:
            if job.name == 'apscheduler.jobstores.base.JobLookupError' or job.name == 'apscheduler.jobstores.base.JobDeserializationError' :
                try:
                  logger.error(f"Error: Job with ID {job.id} is stale due to missing function definition: {job.name}, marking it disabled")
                  self._scheduler.remove_job(job.id)
                except Exception as e:
                    logger.error(f"Failed to remove stale job {job.id} {e}")

    def _process_all_jobs(self):
        """Process all the jobs by resolving the function."""
        jobs = self._scheduler.get_jobs()
        for job in jobs:
            try:
                # force resolve the functions which will throw LookupError if not found
                job.func
            except Exception as e:
                logger.error(f"Error: Job with ID {job.id} has error : {e} , removing it")
                try:
                  self._scheduler.remove_job(job.id)
                except Exception as remove_e:
                    logger.error(f"Failed to remove job id {job.id} {remove_e}")

    
    def start_and_process_jobs(self):
      """Start the scheduler and process jobs."""
      try:
        self.start()
        self._process_job_removal_errors()
        self._process_all_jobs()
      except Exception as e:
        logger.error(f"Error during start and process jobs {e}")

def get_scheduler(uri: str) -> Scheduler:
    """Returns the scheduler instance."""
    return APSchedulerWrapper(uri)
