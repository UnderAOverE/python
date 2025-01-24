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
