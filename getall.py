from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.job import Job

app = FastAPI()

# Mock Scheduler
scheduler = AsyncIOScheduler()

# Pydantic Models
class JobDetails(BaseModel):
    job_id: str = Field(..., title="Job ID", description="The unique identifier of the job.")
    name: str = Field(..., title="Job Name", description="The name of the job.")
    next_run_time: Optional[str] = Field(
        None, title="Next Run Time", description="The next scheduled run time for the job."
    )
    trigger: str = Field(..., title="Trigger Type", description="The type of trigger for the job (e.g., cron, interval).")
    trigger_args: dict = Field(..., title="Trigger Arguments", description="Arguments for the job's trigger.")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job1",
                "name": "Example Job",
                "next_run_time": "2025-01-23T12:00:00",
                "trigger": "cron",
                "trigger_args": {"hour": "12", "minute": "0"},
            }
        }


class JobListResponse(BaseModel):
    jobs: List[JobDetails] = Field(..., title="Jobs", description="List of all scheduled jobs.")
    total_jobs: int = Field(..., title="Total Jobs", description="The total number of scheduled jobs.")

    class Config:
        json_schema_extra = {
            "example": {
                "jobs": [
                    {
                        "job_id": "job1",
                        "name": "Example Job",
                        "next_run_time": "2025-01-23T12:00:00",
                        "trigger": "cron",
                        "trigger_args": {"hour": "12", "minute": "0"},
                    }
                ],
                "total_jobs": 1,
            }
        }

# Route to get all job details
@app.get("/jobs", response_model=JobListResponse, status_code=status.HTTP_200_OK)
async def get_all_jobs():
    """
    Fetch all jobs and their details from the scheduler.
    """
    try:
        jobs = scheduler.get_jobs()
        if not jobs:
            return JobListResponse(jobs=[], total_jobs=0)

        job_details = []
        for job in jobs:
            job_details.append(
                JobDetails(
                    job_id=job.id,
                    name=job.name,
                    next_run_time=str(job.next_run_time) if job.next_run_time else None,
                    trigger=job.trigger.__class__.__name__,
                    trigger_args=job.trigger.fields if hasattr(job.trigger, "fields") else {},
                )
            )
        return JobListResponse(jobs=job_details, total_jobs=len(job_details))

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching jobs: {str(e)}",
        )
