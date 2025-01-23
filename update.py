from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, timedelta
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging

app = FastAPI()

# Example scheduler setup
scheduler = AsyncIOScheduler()
scheduler.start()

# Logger for uncaught exceptions
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic request model
class UpdateJobRequest(BaseModel):
    job_id: str = Field(
        title="Job ID",
        description="The unique identifier of the job to be updated. Use 'all' (case-insensitive) to update all jobs.",
        example="job-12345"
    )
    trigger_type: str = Field(
        title="Trigger Type",
        description="The type of trigger to apply: 'cron', 'date', or 'interval'.",
        example="cron"
    )
    trigger_args: dict = Field(
        title="Trigger Arguments",
        description=(
            "Arguments required for the specified trigger type. For example, "
            "a 'cron' trigger might use {'hour': 14, 'minute': 30}, while an "
            "'interval' trigger might use {'seconds': 30}."
        ),
        example={"hour": 14, "minute": 30}
    )

    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "job_id": "job-12345",
                    "trigger_type": "cron",
                    "trigger_args": {"hour": 14, "minute": 30}
                },
                {
                    "job_id": "ALL",
                    "trigger_type": "interval",
                    "trigger_args": {"seconds": 60}
                }
            ]
        }

# Pydantic response model
class UpdateJobResponse(BaseModel):
    message: str = Field(
        title="Message",
        description="A message indicating the result of the operation.",
        example="Job updated successfully."
    )
    updated_jobs: List[str] = Field(
        title="Updated Jobs",
        description="A list of job IDs that were updated.",
        example=["job-12345", "job-67890"]
    )

    class Config:
        json_schema_extra = {
            "examples": [
                {"message": "Job updated successfully.", "updated_jobs": ["job-12345"]},
                {"message": "All jobs updated successfully.", "updated_jobs": ["job-12345", "job-67890"]}
            ]
        }

@app.post(
    "/update-job",
    response_model=UpdateJobResponse,
    status_code=status.HTTP_200_OK
)
async def update_job(request: UpdateJobRequest):
    """
    Updates or reschedules a job based on its job_id.
    If job_id is "all" (case-insensitive), updates all jobs.
    """
    try:
        updated_jobs = []

        if request.job_id.lower() == "all":
            jobs = scheduler.get_jobs()
            if not jobs:
                return UpdateJobResponse(
                    message="No jobs to update. Scheduler is empty.",
                    updated_jobs=[]
                )
            for job in jobs:
                _update_job_trigger(job, request.trigger_type, request.trigger_args)
                updated_jobs.append(job.id)
            return UpdateJobResponse(
                message="All jobs updated successfully.",
                updated_jobs=updated_jobs
            )
        else:
            # Update a single job
            job = scheduler.get_job(request.job_id)
            if not job:
                raise JobLookupError(f"Job with ID '{request.job_id}' not found.")
            _update_job_trigger(job, request.trigger_type, request.trigger_args)
            updated_jobs.append(request.job_id)
            return UpdateJobResponse(
                message="Job updated successfully.",
                updated_jobs=updated_jobs
            )
    except JobLookupError as e:
        # Job with the given ID does not exist
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        # Log uncaught exceptions and return 500
        logger.exception("Unexpected error while updating job.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while updating the job."
        )

def _update_job_trigger(job, trigger_type: str, trigger_args: dict):
    """
    Helper function to update a job's trigger.
    """
    trigger = None

    if trigger_type == "cron":
        trigger = CronTrigger(**trigger_args)
    elif trigger_type == "date":
        trigger = DateTrigger(**trigger_args)
    elif trigger_type == "interval":
        trigger = IntervalTrigger(**trigger_args)
    else:
        raise ValueError(f"Unsupported trigger_type '{trigger_type}'")

    scheduler.reschedule_job(job.id, trigger=trigger)
