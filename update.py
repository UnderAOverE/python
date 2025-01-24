from fastapi import FastAPI, HTTPException, status
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import BaseModel, Field
from typing import Optional, List, Dict

app = FastAPI(title="Job Scheduler API", description="An API to manage and update scheduled jobs.", version="1.0.0")
scheduler = AsyncIOScheduler()

class UpdateJobRequest(BaseModel):
    """
    Request model for updating a job in the scheduler.
    """
    job_id: str = Field(
        ...,
        title="Job ID",
        description="The ID of the job to update. Use 'all' (case-insensitive) to update all jobs.",
        example="job_1234"
    )
    trigger_type: str = Field(
        ...,
        title="Trigger Type",
        description="The type of trigger to apply. Must be one of 'cron', 'date', or 'interval'.",
        example="cron"
    )
    trigger_args: Dict = Field(
        ...,
        title="Trigger Arguments",
        description="Arguments required for the specified trigger type. For example, for 'cron', provide fields like 'hour', 'minute', etc.",
        example={"hour": 14, "minute": 30}
    )
    func: Optional[str] = Field(
        None,
        title="Function Name",
        description="The name of the new function to execute for the job (optional).",
        example="my_task_function"
    )
    args: Optional[List] = Field(
        None,
        title="Function Arguments",
        description="A list of positional arguments to pass to the function (optional).",
        example=["arg1", "arg2"]
    )
    kwargs: Optional[Dict] = Field(
        None,
        title="Function Keyword Arguments",
        description="A dictionary of keyword arguments to pass to the function (optional).",
        example={"key1": "value1", "key2": "value2"}
    )

    class Config:
        schema_extra = {
            "example": {
                "job_id": "job_1234",
                "trigger_type": "cron",
                "trigger_args": {"hour": 14, "minute": 30},
                "func": "my_task_function",
                "args": ["arg1", "arg2"],
                "kwargs": {"key1": "value1", "key2": "value2"}
            }
        }


class UpdateJobResponse(BaseModel):
    """
    Response model for the result of updating a job in the scheduler.
    """
    message: str = Field(
        ...,
        title="Response Message",
        description="A message indicating the result of the operation.",
        example="Job(s) updated successfully."
    )
    updated_jobs: List[str] = Field(
        ...,
        title="Updated Job IDs",
        description="A list of IDs for the jobs that were updated.",
        example=["job_1234", "job_5678"]
    )

    class Config:
        schema_extra = {
            "example": {
                "message": "Job(s) updated successfully.",
                "updated_jobs": ["job_1234", "job_5678"]
            }
        }


def update_job_in_scheduler(
    scheduler: AsyncIOScheduler,
    job_id: str,
    trigger_type: str,
    trigger_args: dict,
    func: Optional[callable] = None,
    args: Optional[List] = None,
    kwargs: Optional[dict] = None
) -> List[str]:
    """
    Updates or reschedules a job based on its job_id and optionally updates the function and its arguments.

    Args:
        scheduler (AsyncIOScheduler): The scheduler instance.
        job_id (str): The ID of the job to update. Use "all" (case-insensitive) to update all jobs.
        trigger_type (str): The type of trigger to apply: 'cron', 'date', or 'interval'.
        trigger_args (dict): Arguments required for the specified trigger type.
        func (callable, optional): The new function to be executed by the job.
        args (List, optional): Positional arguments for the function.
        kwargs (dict, optional): Keyword arguments for the function.

    Returns:
        List[str]: A list of updated job IDs.

    Raises:
        JobLookupError: If a specific job ID does not exist in the scheduler.
        ValueError: If the trigger type is unsupported.
    """
    updated_jobs = []

    if job_id.lower() == "all":
        jobs = scheduler.get_jobs()
        if not jobs:
            raise ValueError("No jobs to update. Scheduler is empty.")
        for job in jobs:
            _update_job(job, trigger_type, trigger_args, scheduler, func, args, kwargs)
            updated_jobs.append(job.id)
    else:
        job = scheduler.get_job(job_id)
        if not job:
            raise JobLookupError(f"Job with ID '{job_id}' not found.")
        _update_job(job, trigger_type, trigger_args, scheduler, func, args, kwargs)
        updated_jobs.append(job_id)

    return updated_jobs

def _update_job(
    job,
    trigger_type: str,
    trigger_args: dict,
    scheduler: AsyncIOScheduler,
    func: Optional[callable] = None,
    args: Optional[List] = None,
    kwargs: Optional[dict] = None
):
    """
    Helper function to update a job's trigger, function, and arguments.

    Args:
        job: The job instance to update.
        trigger_type (str): The type of trigger to apply: 'cron', 'date', or 'interval'.
        trigger_args (dict): Arguments required for the specified trigger type.
        scheduler (AsyncIOScheduler): The scheduler instance.
        func (callable, optional): The new function to be executed by the job.
        args (List, optional): Positional arguments for the function.
        kwargs (dict, optional): Keyword arguments for the function.

    Raises:
        ValueError: If the trigger type is unsupported.
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

    # Update the trigger
    scheduler.reschedule_job(job.id, trigger=trigger)

    # Update function and its arguments if provided
    if func or args or kwargs:
        scheduler.modify_job(job.id, func=func, args=args, kwargs=kwargs)

@app.post(
    "/update-job",
    response_model=UpdateJobResponse,
    status_code=status.HTTP_200_OK,
    summary="Update or Reschedule Job",
    description="Updates or reschedules a job in the scheduler based on its job_id. Optionally updates the function and its arguments."
)
async def update_job(request: UpdateJobRequest):
    """
    Updates or reschedules a job based on its job_id.
    Optionally updates the function and its arguments.
    """
    try:
        updated_jobs = update_job_in_scheduler(
            scheduler=scheduler,
            job_id=request.job_id,
            trigger_type=request.trigger_type,
            trigger_args=request.trigger_args,
            func=request.func,
            args=request.args,
            kwargs=request.kwargs
        )
        return UpdateJobResponse(
            message="Job(s) updated successfully.",
            updated_jobs=updated_jobs
        )
    except JobLookupError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.exception("Unexpected error while updating job.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while updating the job."
        )
