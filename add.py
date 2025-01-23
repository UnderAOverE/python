from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict
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
        # Dynamically import the job function
        module_name, func_name = job_data.job_function.rsplit('.', 1)
        module = __import__(module_name, fromlist=[func_name])
        job_func = getattr(module, func_name)

        # Add the job to the scheduler
        job = scheduler.add_job(
            job_func,
            trigger=job_data.trigger_type,
            id=job_data.job_id,
            name=job_data.name,
            replace_existing=job_data.replace_existing,
            max_instances=job_data.max_instances,
            **job_data.trigger_args
        )

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
