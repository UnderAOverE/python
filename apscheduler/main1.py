from typing import Dict, Any, Union, Optional, List
from pydantic import BaseModel, Field, ConfigDict, RootModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Depends, HTTPException, status, APIRouter, Query, Body
from apscheduler.jobstores.base import JobLookupError
from pymongo import MongoClient
from pymongo import ReturnDocument


# File: apis/models/triggers.py

class CronTrigger(BaseModel):
    type: str = "cron"
    arguments: Dict[str, Any] = Field(
        ...,
        default={},
        description="Cron trigger arguments. Provide only necessary arguments",
        examples=[
            {"year": "2024"},  # run every year
            {"month": "1"},  # run every january
            {"day": "1"},  # run every 1st of the month
            {"week": "1"},  # run every 1st week of month
            {"day_of_week": "mon"},  # run every monday
            {"hour": "10"},  # run every hour 10
            {"minute": "30"},  # run every minute 30
            {"second": "0"},  # run every second 0
            {"start_date": "2024-01-01T10:00:00", "end_date": "2025-01-01T10:00:00"},
            # run between these two dates
            {"timezone": "UTC"},  # run in utc timezone
            {"minute": "*/5"},  # Every 5 minutes
            {"hour": 10, "minute": 30},  # At 10:30 AM
            {"day_of_week": "mon-fri", "hour": 9}  # Every weekday at 9am
        ]
    )

    model_config = ConfigDict(
        json_schema_extra={
            "description": "A cron-based trigger to run the job",
            "properties": {
                "type": {
                    "enum": [
                        "cron"
                    ]
                },
                "arguments": {
                    "description": "Cron arguments",
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "string",
                            "description": "Year to trigger on",
                            "default": "*"
                        },
                        "month": {
                            "type": "string",
                            "description": "Month to trigger on",
                            "default": "*"
                        },
                        "day": {
                            "type": "string",
                            "description": "Day of month to trigger on",
                            "default": "*"
                        },
                        "week": {
                            "type": "string",
                            "description": "Week of the year to trigger on",
                            "default": "*"
                        },
                        "day_of_week": {
                            "type": "string",
                            "description": "Day of week to trigger on",
                            "default": "*"
                        },
                        "hour": {
                            "type": "string",
                            "description": "Hour to trigger on",
                            "default": "*"
                        },
                        "minute": {
                            "type": "string",
                            "description": "Minute to trigger on",
                            "default": "*"
                        },
                        "second": {
                            "type": "string",
                            "description": "Second to trigger on",
                            "default": "*"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Start date to trigger on"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "End date to trigger on"
                        },
                        "timezone": {
                            "type": "string",
                            "description": "Timezone to trigger on",
                            "default": "UTC"
                        }
                    }
                }
            }
        }
    )


# endClass


class DateTrigger(BaseModel):
    type: str = "date"
    arguments: Dict[str, Any] = Field(
        ...,
        default={},
        description="Date trigger arguments, specifically a run_date.",
        examples=[
            {"run_date": "2024-08-27T10:30:00", "timezone": "UTC"}
        ]
    )

    model_config = ConfigDict(
        json_schema_extra={
            "description": "A date-based trigger to run the job once on specified date.",
            "properties": {
                "type": {
                    "enum": [
                        "date"
                    ]
                },
                "arguments": {
                    "type": "object",
                    "properties": {
                        "run_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Date on which the job should be executed."
                        },
                        "timezone": {
                            "type": "string",
                            "description": "Timezone to trigger on",
                            "default": "UTC"
                        }
                    }
                }
            }
        }
    )


# endClass


class IntervalTrigger(BaseModel):
    type: str = "interval"
    arguments: Dict[str, Any] = Field(
        ...,
        default={},
        description="Interval trigger arguments, provide only one of them.",
        examples=[
            {"seconds": 10, "start_date": "2024-01-01T10:00:00", "end_date": "2025-01-01T10:00:00",
             "timezone": "UTC"},  # Every 10 seconds
            {"minutes": 5},  # Every 5 minutes
            {"hours": 1},  # Every hour
            {"days": 1},  # Every day
            {"weeks": 1}  # Every week
        ]
    )

    model_config = ConfigDict(
        json_schema_extra={
            "description": "An interval-based trigger to run the job repeatedly.",
            "properties": {
                "type": {
                    "enum": [
                        "interval"
                    ]
                },
                "arguments": {
                    "type": "object",
                    "properties": {
                        "weeks": {
                            "type": "integer",
                            "description": "Number of weeks to trigger on"
                        },
                        "days": {
                            "type": "integer",
                            "description": "Number of days to trigger on"
                        },
                        "hours": {
                            "type": "integer",
                            "description": "Number of hours to trigger on"
                        },
                        "minutes": {
                            "type": "integer",
                            "description": "Number of minutes to trigger on"
                        },
                        "seconds": {
                            "type": "integer",
                            "description": "Number of seconds to trigger on"
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Start date to trigger on"
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date-time",
                            "description": "End date to trigger on"
                        },
                        "timezone": {
                            "type": "string",
                            "description": "Timezone to trigger on",
                            "default": "UTC"
                        }
                    }
                }
            }
        }
    )


# endClass


class Trigger(RootModel):
    root: Union[CronTrigger, DateTrigger, IntervalTrigger]

    model_config = ConfigDict(
        json_schema_extra={"description": "A trigger to run the job, can be of type cron, date or interval."}
    )


# endClass


# File: apis/models/batch.py


class BatchDetails(BaseModel):
    arguments: Optional[List[Any]] = Field(
        default=None,
        description="List of positional arguments for the batch job main function",
    )

    name: str = Field(
        ...,
        title="Batch Job",
        description="Path to the batch job entry point main function, e.g., `module.function`.",
    )

    keyword_arguments: Optional[Dict[str, Any]] = Field(
        default=None,
        title="Batch Keyword Arguments",
        description="Optional keyword arguments to pass to the batch job main function."
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

    model_config = ConfigDict(
        json_schema_extra={"description": "Details of the batch job."}
    )


# endClass


class BatchIdentifier(BaseModel):
    environment: str = Field(..., title="Batch Job Environment", description="Environment identifier")
    region: str = Field(..., title="Batch Job Region", description="Region identifier")
    sector: str = Field(..., title="Batch Job Sector", description="Sector identifier")

    model_config = ConfigDict(
        json_schema_extra={"description": "Identifiers for the batch job."}
    )


# endClass


# File: apis/models/add.py


class AddRequest(BaseModel):
    batch_identifiers: BatchIdentifier = Field(
        ...,
        title="Batch Identifier",
        description="Batch identifiers: environment, region, sector, etc.",
    )

    batch_details: BatchDetails = Field(
        ...,
        title="Batch Details",
        description="Details of the batch job",
    )

    job_id: str = Field(
        ...,
        title="Job ID",
        description="The ID of the job to add.",
        example="job_1234",
    )

    name: Optional[str] = Field(
        None,
        title="Job Name",
        description="Optional name of the job.",
    )

    tigger: Trigger = Field(..., title="Trigger Type", description="Type of the job trigger.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Request body for adding a new batch job."}
    )


# endClass


class AddResponse(BaseModel):
    job_id: str = Field(..., title="Job ID", description="Unique identifier of the added job.")
    next_run_time: Optional[str] = Field(
        None,
        title="Next Run Time",
        description="The next scheduled run time for the job."
    )
    status: str = Field(..., title="Status", description="Status message indicating success or failure.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Response body after adding a job."}
    )


# endClass


# File: apis/models/update.py


class UpdateRequest(BaseModel):
    batch_details: BatchDetails = Field(
        ...,
        title="Batch Details",
        description="Details of the batch job",
    )

    job_id: str = Field(
        ...,
        title="Job ID",
        description="The ID of the job to update. Use 'all' (case-insensitive) to update all jobs.",
        example="job_1234",
    )

    tigger: Trigger = Field(..., title="Trigger Type", description="Type of the job trigger.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Request body for updating a batch job."}
    )


# endClass


class UpdateResponse(BaseModel):
    status: str = Field(..., title="Status", description="Status message indicating success or failure.")

    updated_jobs: List[str] = Field(
        ...,
        title="Updated Job IDs",
        description="A list of IDs for the jobs that were updated.",
        example=["job_1234", "job_5678"]
    )

    model_config = ConfigDict(
        json_schema_extra={"description": "Response body after updating a job."}
    )


# endClass


# File: apis/models/remove.py


class RemoveRequest(BaseModel):
    job_id: str = Field(
        ...,
        title="Job ID",
        description="The unique identifier of the job to be removed. Use 'all' (case-insensitive) to remove all jobs.",
        example="job-12345"
    )

    model_config = ConfigDict(
        json_schema_extra={"description": "Request body for removing a job."}
    )


# endClass


class RemoveResponse(BaseModel):
    job_id: str = Field(
        ...,
        title="Job ID",
        description="The ID of the job that was removed. If all jobs were removed, this will be 'all'.",
        example="job-12345"
    )

    status: str = Field(..., title="Status", description="Status message indicating success or failure.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Response body after removing a job."}
    )


# endClass


# File: apis/models/list.py


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

    model_config = ConfigDict(
        json_schema_extra={"description": "Details of a scheduled job."}
    )


# endClass


class JobListResponse(BaseModel):
    jobs: List[JobDetails] = Field(..., title="Jobs", description="List of all scheduled jobs.")
    total_jobs: int = Field(..., title="Total Jobs", description="The total number of scheduled jobs.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Response body for listing scheduled jobs."}
    )


# endClass


class JobQuery(BaseModel):
    job_ids: Optional[List[str]] = Field(None, title="Job IDs", description="List of job IDs to fetch.")
    names: Optional[List[str]] = Field(None, title="Job Names", description="List of job names to fetch.")

    model_config = ConfigDict(
        json_schema_extra={"description": "Query parameters for listing scheduled jobs."}
    )


# endClass


# File: apis/utils/api_utils.py


def add_job_to_scheduler(scheduler: AsyncIOScheduler = Depends(get_scheduler), job_data: AddRequest):
    try:
        existing_job = scheduler.get_job(job_data.job_id)
        if existing_job:
            raise ValueError(f"Job with ID '{job_data.job_id}' already exists.")

        # endIf

        module_name, func_name = job_data.batch_details.name.rsplit('.', 1)
        module = __import__(module_name, fromlist=[func_name])
        job_func = getattr(module, func_name)

        job = scheduler.add_job(
            job_func,
            trigger=job_data.trigger.type,
            id=job_data.job_id,
            name=job_data.name,
            replace_existing=job_data.batch_details.replace_existing,
            max_instances=job_data.batch_details.max_instances,
            kwargs=job_data.batch_details.keyword_arguments or {},  # Pass arguments to the job
            **job_data.trigger.arguments
        )

        return job

    except ImportError as e:
        raise ValueError(f"Failed to import the job function: {str(e)}")

    except ValueError as e:
        raise ValueError(str(e))

    except Exception as e:
        raise RuntimeError(f"An error occurred while adding the job: {str(e)}")

    # endTryExcept


# endDef


def update_job_in_scheduler(
    scheduler: AsyncIOScheduler = Depends(get_scheduler),
    job_data: UpdateRequest
):
    try:
        if job_data.job_id.lower() == "all":
            updated_job_ids = []
            jobs = scheduler.get_jobs()

            if not jobs:
                return [], "No jobs to update. Scheduler is empty."

            for job in jobs:
                module_name, func_name = job_data.batch_details.name.rsplit('.', 1)
                module = __import__(module_name, fromlist=[func_name])
                job_func = getattr(module, func_name)

                scheduler.modify_job(
                    job.id,
                    func=job_func,
                    name=job.name,
                    max_instances=job_data.batch_details.max_instances,
                    kwargs=job_data.batch_details.keyword_arguments or {},
                    **job_data.trigger.arguments
                )

                updated_job_ids.append(job.id)

            return updated_job_ids, "All jobs updated successfully."

        else:
            module_name, func_name = job_data.batch_details.name.rsplit('.', 1)
            module = __import__(module_name, fromlist=[func_name])
            job_func = getattr(module, func_name)

            scheduler.modify_job(
                job_data.job_id,
                func=job_func,
                name=job.name,
                max_instances=job_data.batch_details.max_instances,
                kwargs=job_data.batch_details.keyword_arguments or {},
                **job_data.trigger.arguments
            )

            return [job_data.job_id], "Job updated successfully."


    except ImportError as e:
        raise ValueError(f"Failed to import the job function: {str(e)}")

    except JobLookupError:
        raise ValueError(f"Job with ID '{job_data.job_id}' not found")

    except Exception as e:
        raise RuntimeError(f"An error occurred while updating the job: {str(e)}")


# endDef


# File: apis/utils/database.py


def get_database_client():
    try:

        client = MongoClient("mongodb://localhost:27017")  # Replace with your connection string

        # ping the database to check connection
        client.admin.command("ping")

        return client

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while connecting to database: {str(e)}"
        )


# File: apis/utils/scheduler.py


def get_scheduler(app: FastAPI):
    if not hasattr(app.state, 'scheduler'):
        app.state.scheduler = AsyncIOScheduler()
        app.state.scheduler.start()
    return app.state.scheduler


# File: apis/routers/add.py

router = APIRouter()


@router.post("/jobs/add", response_model=AddResponse, status_code=status.HTTP_201_CREATED)
async def add_job(scheduler: AsyncIOScheduler = Depends(get_scheduler), job_data: AddRequest,
                  db_client=Depends(get_database_client)):
    try:
        job = add_job_to_scheduler(scheduler, job_data)
        jobs_db = db_client.get_database("ampscheduler")

        jobs_collection = jobs_db.get_collection("jobs")

        # Extract only fields that are needed for db
        job_data_dict = job_data.model_dump()

        jobs_collection.insert_one(job_data_dict)

        return AddResponse(
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

    # endTryExcept


# endDef


# File: apis/routers/remove.py

router = APIRouter()


@router.post(
    "/jobs/remove",
    response_model=RemoveResponse,
    status_code=status.HTTP_200_OK
)
async def remove_job(
    request: RemoveRequest,
    scheduler: AsyncIOScheduler = Depends(get_scheduler),
    db_client=Depends(get_database_client)
):
    try:
        if request.job_id.lower() == "all":
            jobs = scheduler.get_jobs()
            if not jobs:
                return RemoveResponse(
                    status="No jobs to remove. Scheduler is empty.",
                    job_id="all"
                )

            # endIf

            # Remove all jobs
            for job in jobs:
                scheduler.remove_job(job.id)
                jobs_db = db_client.get_database("ampscheduler")
                jobs_collection = jobs_db.get_collection("jobs")
                jobs_collection.delete_one({"job_id": job.id})

            # endFor

            return RemoveResponse(
                status="All jobs removed successfully.",
                job_id="all"
            )

        else:
            # Remove specific job
            scheduler.remove_job(request.job_id)
            jobs_db = db_client.get_database("ampscheduler")
            jobs_collection = jobs_db.get_collection("jobs")
            jobs_collection.delete_one({"job_id": request.job_id})
            return RemoveResponse(
                status="Job removed successfully.",
                job_id=request.job_id
            )

        # endIfElse

    except JobLookupError:
        # Job with the given ID does not exist
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{request.job_id}' not found"
        )
    except Exception as e:
        # Log uncaught exceptions and return 500

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while removing the job."
        )

    # endTryExcept


# endDef


@router.get(
    "/jobs/remove/{job_id}",
    response_model=RemoveResponse,
    status_code=status.HTTP_200_OK
)
async def remove_job_by_id(
    job_id: str,
    scheduler: AsyncIOScheduler = Depends(get_scheduler),
    db_client=Depends(get_database_client)
):
    try:

        # Remove specific job
        scheduler.remove_job(job_id)
        jobs_db = db_client.get_database("ampscheduler")
        jobs_collection = jobs_db.get_collection("jobs")
        jobs_collection.delete_one({"job_id": job_id})
        return RemoveResponse(
            status="Job removed successfully.",
            job_id=job_id
        )

    except JobLookupError:
        # Job with the given ID does not exist
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{job_id}' not found"
        )
    except Exception as e:
        # Log uncaught exceptions and return 500

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while removing the job."
        )

    # endTryExcept


# endDef


# File: apis/routers/list.py
router = APIRouter()

@router.get("/jobs", response_model=JobListResponse, status_code=status.HTTP_200_OK)
@router.post("/jobs", response_model=JobListResponse, status_code=status.HTTP_200_OK)
async def get_jobs(
    job_ids: Optional[List[str]] = Query(None, title="Job IDs", description="List of job IDs to fetch."),
    names: Optional[List[str]] = Query(None, title="Job Names", description="List of job names to fetch."),
    body: Optional[JobQuery] = Body(None),
    scheduler: AsyncIOScheduler = Depends(get_scheduler),
    db_client=Depends(get_database_client)
):
    try:
        # Merge query params and request body
        if body:
            if body.job_ids:
                job_ids = list(set((job_ids or []) + body.job_ids))

            # endIf

            if body.names:
                names = list(set((names or []) + body.names))

            # endIf

        # endIf

        jobs = scheduler.get_jobs()

        job_details_list = []

        jobs_db = db_client.get_database("ampscheduler")
        jobs_collection = jobs_db.get_collection("jobs")

        # Fetch from db based on filter provided
        if job_ids or names:

            query_filter = {}
            if job_ids:
                query_filter["job_id"] = {"$in": job_ids}

            if names:
                query_filter["name"] = {"$in": names}

            # Fetch jobs from db
            db_jobs = jobs_collection.find(query_filter)

            for db_job in db_jobs:

                scheduler_job = scheduler.get_job(db_job["job_id"])

                if scheduler_job:
                    job_details = JobDetails(
                        job_id=scheduler_job.id,
                        name=scheduler_job.name,
                        next_run_time=str(scheduler_job.next_run_time) if scheduler_job.next_run_time else None,
                        trigger=scheduler_job.trigger.type,
                        trigger_args=scheduler_job.trigger.arguments,
                        func=db_job["batch_details"]["name"],
                        func_args=db_job["batch_details"].get("arguments", []) or [],
                    )
                    job_details_list.append(job_details)

                # endIf

            # endFor
        else:
            # fetch from scheduler if no query param
            for job in jobs:
                db_job = jobs_collection.find_one({"job_id": job.id})

                if db_job:
                    job_details = JobDetails(
                        job_id=job.id,
                        name=job.name,
                        next_run_time=str(job.next_run_time) if job.next_run_time else None,
                        trigger=job.trigger.type,
                        trigger_args=job.trigger.arguments,
                        func=db_job["batch_details"]["name"],
                        func_args=db_job["batch_details"].get("arguments", []) or [],
                    )

                    job_details_list.append(job_details)

                # endIf
            # endFor

        # endIfElse

        return JobListResponse(jobs=job_details_list, total_jobs=len(job_details_list))

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching jobs: {str(e)}",
        )

    # endTryExcept


# endDef
# File: apis/routers/update.py

router = APIRouter()

@router.post("/jobs/update", response_model=UpdateResponse, status_code=status.HTTP_200_OK)
async def update_job(
    job_data: UpdateRequest,
    scheduler: AsyncIOScheduler = Depends(get_scheduler),
    db_client = Depends(get_database_client)
):
    try:
        updated_job_ids, message = update_job_in_scheduler(scheduler, job_data)

        jobs_db = db_client.get_database("ampscheduler")
        jobs_collection = jobs_db.get_collection("jobs")

        # Extract only fields that are needed for db
        job_data_dict = job_data.model_dump()

        if job_data.job_id.lower() == "all":
            # Update all documents in the collection
            jobs_collection.update_many({}, {"$set": job_data_dict})

        else:

            jobs_collection.find_one_and_update({"job_id":job_data.job_id},{"$set":job_data_dict}, return_document=ReturnDocument.AFTER)

        return UpdateResponse(status=message, updated_jobs=updated_job_ids)


    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to import the job function: {str(e)}"
        )
    except ValueError as e:
           raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while updating the job: {str(e)}"
        )

    # endTryExcept
