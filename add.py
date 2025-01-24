from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

# Router and Scheduler
router = APIRouter()
scheduler = AsyncIOScheduler()

# Pydantic Models
class AddJobRequest(BaseModel):
    job_id: str = Field(..., title="Job ID", description="Unique identifier for the job.")
    name: Optional[str] = Field(None, title="Job Name", description="Optional name of the job.")
    trigger_type: Literal["cron", "interval", "date"] = Field(..., title="Trigger Type", description="Type of the job trigger.")
    trigger_args: Dict[str, str] = Field(
        ..., 
        title="Trigger Arguments", 
        description="Arguments for the trigger (e.g., `{'hour': '12', 'minute': '0'}` for cron)."
    )
    job_function: str = Field(..., title="Job Function", description="Path to the job function, e.g., `module.function`.")
    job_arguments: Optional[Dict[str, Any]] = Field(
        default=None, 
        title="Job Arguments", 
        description="Optional dictionary of arguments to pass to the job function."
    )
    max_instances: Optional[int] = Field(
        1, 
        title="Max Instances", 
        description="Maximum instances of the job that can run concurrently."
    )
    replace_existing: bool = Field(
        True, 
        title="Replace Existing", 
        description="Replace an existing job with the same ID."
    )

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job1",
                "name": "Daily Backup",
                "trigger_type": "cron",
                "trigger_args": {"hour": "12", "minute": "0"},
                "job_function": "my_module.my_function",
                "job_arguments": {"arg1": "value1", "arg2": 42},
                "max_instances": 3,
                "replace_existing": True
            }
        }


class AddJobResponse(BaseModel):
    job_id: str = Field(..., title="Job ID", description="Unique identifier of the added job.")
    next_run_time: Optional[str] = Field(
        None, 
        title="Next Run Time", 
        description="The next scheduled run time for the job."
    )
    status: str = Field(..., title="Status", description="Status message indicating success.")

    class Config:
        json_schema_extra = {
            "example": {
                "job_id": "job1",
                "next_run_time": "2025-01-23T12:00:00",
                "status": "Job successfully added."
            }
        }


# Add Job Route
@router.post("/jobs", response_model=AddJobResponse, status_code=status.HTTP_201_CREATED)
async def add_job(job_data: AddJobRequest):
    """
    Add a new job to the scheduler.
    """
    try:
        # Call the standalone function to add a job
        job = add_job_to_scheduler(scheduler, job_data)

        # Response
        return AddJobResponse(
            job_id=job.id,
            next_run_time=str(job.next_run_time) if job.next_run_time else None,
            status="Job successfully added."
        )

    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to import the job function: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while adding the job: {str(e)}"
        )
