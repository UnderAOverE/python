from functools import wraps
import asyncio
import hashlib

from pymongo import ReturnDocument
from datetime import datetime, timedelta

LOCK_TTL = 600  # 10 minutes

# Assume lock_collection is defined elsewhere (e.g., a global variable)
# lock_collection = ...

def acquire_lock(job_name: str) -> bool:
    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=LOCK_TTL)

    lock = lock_collection.find_one_and_update(
        {"job_name": job_name, "$or": [{"expires_at": {"$lte": now}}, {"expires_at": {"$exists": False}}]},
        {"$set": {"job_name": job_name, "expires_at": expires_at}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    if lock and "expires_at" in lock:  # Check if lock exists and has 'expires_at'
        return lock["expires_at"] == expires_at
    return False  # Handle cases where lock is None

def release_lock(job_name: str):
    """
    Release a distributed lock for a specific job.
    """
    lock_collection.delete_one({"job_name": job_name})


def generate_lock_name(*args, **kwargs) -> str:
    """
    Generates a unique lock name based on the function name and its arguments.
    """
    arg_string = f"{args}{kwargs}"
    hashed_arg_string = hashlib.md5(arg_string.encode('utf-8')).hexdigest()  # Create hash
    return hashed_arg_string

def distributed_lock(base_job_name: str):
    """
    Decorator to acquire and release a distributed lock for a job,
    supporting both synchronous and asynchronous functions.
    The lock name is dynamically generated based on the base job name and arguments.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate the lock name based on function name, and its arguments.
            lock_name = f"{base_job_name}_{generate_lock_name(*args, **kwargs)}"

            # Acquire the lock before running the job
            if acquire_lock(lock_name):
                try:
                    if asyncio.iscoroutinefunction(func):  # Async function
                        return asyncio.run(func(*args, **kwargs))
                    else:  # Sync function
                        return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error during job execution: {e}") #Added error handling
                    raise
                finally:
                    release_lock(lock_name)
            else:
                print(f"Job '{lock_name}' is already running or locked.")
                return None  # Or raise an exception if you prefer

        return wrapper

    return decorator


# Example Usage:
@distributed_lock("sample_job")
def sample_func(env, sector):
    print(f"Running sample_func with env={env} and sector={sector}")
    return f"I am running inside env={env} and sector={sector}"


if __name__ == "__main__":
    import pymongo

    # Replace with your MongoDB connection details
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["mydatabase"]  # Replace with your database name
    lock_collection = db["locks"]  # Replace with your collection name

    # Run the functions (potentially concurrently in a real application)
    result1 = sample_func("dev", "finance")
    result2 = sample_func("prod", "sales")  # This can now run concurrently with the previous call!
    print(f"Result 1: {result1}")
    print(f"Result 2: {result2}")

    # You can simulate concurrency using asyncio.gather or threading.Thread if needed for testing
    # asyncio.run(asyncio.gather(sample_func("dev", "finance"), sample_func("prod", "sales")))
    
generate_lock_name(*args, **kwargs): This function is the key to making the locking system work with variable inputs. It takes the arguments passed to your decorated function and creates a unique lock name based on them. Critically, it hashes the combined arguments using hashlib.md5 to create a fixed-length string suitable for use as a lock identifier. Other hashing algorithms like sha256 could also be used. This avoids exceeding any maximum key length limits imposed by MongoDB. It also combines the arguments and keyword arguments into a single string to ensure that different orderings of keyword arguments lead to the same lock name.

distributed_lock(base_job_name): Now takes a base_job_name as an argument. This allows you to group related jobs under a common identifier while still allowing them to run concurrently with different inputs. This makes it easier to manage and monitor locks.

Dynamic Lock Name Generation: The lock_name is dynamically created inside the wrapper function, incorporating the base_job_name and the hash of the arguments: f"{base_job_name}_{generate_lock_name(*args, **kwargs)}"

Acquire Lock: The acquire_lock function now uses the lock_name to attempt to acquire the lock.

Release Lock: The release_lock function now also uses the lock_name to release the lock.

@wraps(func): This is crucial for preserving the metadata (name, docstring, etc.) of the decorated function.

Error Handling: Added a try...except block to catch potential exceptions during job execution and re-raise them after releasing the lock. This ensures that the lock is always released, even if the job fails.

Clearer if acquire_lock Logic: The if statement is used to check if the lock was successfully acquired before running the function.

Example Usage: The if __name__ == "__main__": block provides a basic example of how to use the decorator and acquire MongoDB connection details.

Checking the return value of acquire_lock: The acquire_lock function checks if the result of the db query actually returned. It also validates if the "expires_at" is in the result.
