from functools import wraps
import asyncio

def distributed_lock(job_name: str):
    """
    Decorator to acquire and release a distributed lock for a job, 
    supporting both synchronous and asynchronous functions.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Acquire the lock before running the job
            if acquire_lock(job_name):
                try:
                    if asyncio.iscoroutinefunction(func):  # Async function
                        return asyncio.run(func(*args, **kwargs))
                    else:  # Sync function
                        return func(*args, **kwargs)
                finally:
                    release_lock(job_name)
            else:
                print(f"Job '{job_name}' is already running or locked.")
                return None
        return wrapper
    return decorator
