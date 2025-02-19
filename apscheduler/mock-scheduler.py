# src/tests/conftest.py
import pytest
from typing import List, Dict, Optional, Callable

class SimulatedScheduler:
    """A simulated APScheduler for testing."""

    def __init__(self):
        self.jobs: Dict[str, dict] = {}  # Store job details (id, func, trigger, args, kwargs)
        self.started = False
        self.listeners: List[Callable] = []

    async def start(self):
        """Simulates starting the scheduler."""
        self.started = True
        print("SimulatedScheduler: Started")

    async def shutdown(self):
        """Simulates shutting down the scheduler."""
        self.started = False
        print("SimulatedScheduler: Shutdown")

    def add_job(self, func: Callable, trigger: str, *args, id: str = None, **kwargs):
        """Simulates adding a job to the scheduler."""
        job_id = id or f"job_{len(self.jobs) + 1}"
        self.jobs[job_id] = {
            "id": job_id,
            "func": func,
            "trigger": trigger,
            "args": args,
            "kwargs": kwargs,
        }
        print(f"SimulatedScheduler: Added job {job_id} - {func.__name__} ({trigger})")
        return job_id  # Returns the job ID

    def remove_job(self, job_id: str):
        """Simulates removing a job from the scheduler."""
        if job_id in self.jobs:
            del self.jobs[job_id]
            print(f"SimulatedScheduler: Removed job {job_id}")
        else:
            print(f"SimulatedScheduler: Job {job_id} not found")

    def get_job(self, job_id: str):
        """Simulates getting a job by its ID."""
        return self.jobs.get(job_id)

    def get_jobs(self):
        """Simulates getting a list of all jobs."""
        return list(self.jobs.values())

    async def pause_job(self, job_id: str):
        """Simulates pausing a job."""
        if job_id in self.jobs:
            print(f"SimulatedScheduler: Paused job {job_id}")
            # Add logic to track paused state if needed
        else:
            print(f"SimulatedScheduler: Job {job_id} not found")

    async def resume_job(self, job_id: str):
        """Simulates resuming a job."""
        if job_id in self.jobs:
            print(f"SimulatedScheduler: Resumed job {job_id}")
            # Add logic to track paused state if needed
        else:
            print(f"SimulatedScheduler: Job {job_id} not found")

    def add_listener(self, callback: Callable, mask: int = None):
        """Simulates adding an event listener."""
        self.listeners.append(callback)
        print("SimulatedScheduler: Added a Listener")

    async def trigger_job(self, job_id: str):
        """Simulates manually triggering a job (for testing purposes)."""
        job = self.get_job(job_id)
        if job:
            print(f"SimulatedScheduler: Triggering job {job_id} manually")
            await job["func"](*job["args"], **job["kwargs"])
        else:
            print(f"SimulatedScheduler: Job {job_id} not found")


@pytest.fixture
def simulated_scheduler():
    """Pytest fixture providing the simulated scheduler."""
    return SimulatedScheduler()  # Create and return an instance of SimulatedScheduler
