from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()
scheduler.start()

job_data = AddJobRequest(
    job_id="job1",
    name="Daily Backup",
    trigger_type="cron",
    trigger_args={"hour": "12", "minute": "0"},
    job_function="my_module.my_function",
    job_arguments={"arg1": "value1", "arg2": 42},
    max_instances=3,
    replace_existing=True
)

# Add job to the scheduler
try:
    job = add_job_to_scheduler(scheduler, job_data)
    print(f"Job {job.id} added successfully. Next run time: {job.next_run_time}")
except ValueError as e:
    print(f"Job addition failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
