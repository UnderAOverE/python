from fastapi import FastAPI, Query, Body, status, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
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
    trigger_args: Dict = Field(..., title="Trigger Arguments", description="Arguments for the job's trigger.")
    func: str = Field(..., title="Function Name", description="The name of the function associated with the job.")
    func_args: List = Field(..., title="Function Arguments", description="The arguments passed to the function.")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job1",
                "name": "Example Job",
                "next_run_time": "2025-01-23T12:00:00",
                "trigger": "cron",
                "trigger_args": {"hour": "12", "minute": "0"},
                "func": "my_module.my_function",
                "func_args": ["arg1", "arg2"],
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
                        "func": "my_module.my_function",
                        "func_args": ["arg1", "arg2"],
                    }
                ],
                "total_jobs": 1,
            }
        }

class JobQuery(BaseModel):
    job_ids: Optional[List[str]] = Field(None, title="Job IDs", description="List of job IDs to fetch.")
    names: Optional[List[str]] = Field(None, title="Job Names", description="List of job names to fetch.")

    class Config:
        json_schema_extra = {
            "example": {
                "job_ids": ["job1", "job2"],
                "names": ["Example Job", "Another Job"]
            }
        }

# Route to fetch jobs with filtering options
@app.get("/jobs", response_model=JobListResponse, status_code=status.HTTP_200_OK)
@app.post("/jobs", response_model=JobListResponse, status_code=status.HTTP_200_OK)
async def get_jobs(
    job_ids: Optional[List[str]] = Query(None, title="Job IDs", description="List of job IDs to fetch."),
    names: Optional[List[str]] = Query(None, title="Job Names", description="List of job names to fetch."),
    body: Optional[JobQuery] = Body(None),
):
    """
    Fetch jobs with optional filters for job IDs and names via query parameters or request body.
    """
    try:
        # Merge query params and request body
        if body:
            if body.job_ids:
                job_ids = list(set((job_ids or []) + body.job_ids))
            if body.names:
                names = list(set((names or []) + body.names))

        jobs = scheduler.get_jobs()

        # Filter jobs based on job_ids or names
        if job_ids or names:
            filtered_jobs = []
            for job in jobs:
                if (job_ids and job.id in job_ids) or (names and job.name in names):
                    filtered_jobs.append(job)
        else:
            filtered_jobs = jobs

        if not filtered_jobs:
            return JobListResponse(jobs=[], total_jobs=0)

        # Convert job details to response format
        job_details = []
        for job in filtered_jobs:
            job_details.append(
                JobDetails(
                    job_id=job.id,
                    name=job.name,
                    next_run_time=str(job.next_run_time) if job.next_run_time else None,
                    trigger=job.trigger.__class__.__name__,
                    trigger_args=job.trigger.fields if hasattr(job.trigger, "fields") else {},
                    func=job.func_ref,  # Function reference
                    func_args=list(job.args) if job.args else [],
                )
            )
        return JobListResponse(jobs=job_details, total_jobs=len(job_details))

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching jobs: {str(e)}",
        )
