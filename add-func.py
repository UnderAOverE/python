def add_job_to_scheduler(scheduler: AsyncIOScheduler, job_data: AddJobRequest):
    """
    Add a job to the scheduler programmatically, ensuring no duplicate job IDs exist.

    Args:
        scheduler (AsyncIOScheduler): The scheduler instance.
        job_data (AddJobRequest): The job data model containing all details.

    Returns:
        Job: The scheduled job instance.

    Raises:
        ValueError: If a job with the same ID already exists.
        RuntimeError: For any other errors while adding the job.
    """
    try:
        # Check if the job ID already exists in the scheduler
        existing_job = scheduler.get_job(job_data.job_id)
        if existing_job:
            raise ValueError(f"Job with ID '{job_data.job_id}' already exists.")

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
            kwargs=job_data.job_arguments or {},  # Pass arguments to the job
            **job_data.trigger_args
        )

        return job

    except ImportError as e:
        raise ValueError(f"Failed to import the job function: {str(e)}")
    except ValueError as e:
        raise ValueError(str(e))  # Propagate job existence error
    except Exception as e:
        raise RuntimeError(f"An error occurred while adding the job: {str(e)}")
