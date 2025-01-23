from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError
import logging

app = FastAPI()
scheduler = AsyncIOScheduler()
scheduler.start()

# Logger for uncaught exceptions
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic request model
class RemoveJobRequest(BaseModel):
    job_id: str = Field(
        title="Job ID",
        description="The unique identifier of the job to be removed. Use 'all' (case-insensitive) to remove all jobs.",
        example="job-12345"
    )

    class Config:
        json_schema_extra = {
            "examples": [
                {"job_id": "job-12345"},
                {"job_id": "ALL"}
            ]
        }

# Pydantic response model
class RemoveJobResponse(BaseModel):
    message: str = Field(
        title="Message",
        description="A message indicating the result of the operation.",
        example="Job removed successfully."
    )
    job_id: str = Field(
        title="Job ID",
        description="The ID of the job that was removed. If all jobs were removed, this will be 'all'.",
        example="job-12345"
    )

    class Config:
        json_schema_extra = {
            "examples": [
                {"message": "Job removed successfully.", "job_id": "job-12345"},
                {"message": "All jobs removed successfully.", "job_id": "all"}
            ]
        }

@app.post(
    "/remove-job",
    response_model=RemoveJobResponse,
    status_code=status.HTTP_200_OK
)
async def remove_job(request: RemoveJobRequest):
    """
    Removes a job from the scheduler based on its job_id.
    If job_id is "all" (case-insensitive), removes all jobs.
    """
    try:
        if request.job_id.lower() == "all":
            jobs = scheduler.get_jobs()
            if not jobs:
                return RemoveJobResponse(
                    message="No jobs to remove. Scheduler is empty.",
                    job_id="all"
                )
            # Remove all jobs
            for job in jobs:
                scheduler.remove_job(job.id)
            return RemoveJobResponse(
                message="All jobs removed successfully.",
                job_id="all"
            )
        else:
            # Remove specific job
            scheduler.remove_job(request.job_id)
            return RemoveJobResponse(
                message="Job removed successfully.",
                job_id=request.job_id
            )
    except JobLookupError:
        # Job with the given ID does not exist
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Job with ID '{request.job_id}' not found"
        )
    except Exception as e:
        # Log uncaught exceptions and return 500
        logger.exception("Unexpected error while removing job.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while removing the job."
        )
