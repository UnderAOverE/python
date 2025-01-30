from pymongo import ReturnDocument
from datetime import datetime, timedelta

LOCK_TTL = 600  # 10 minutes

def acquire_lock(job_name: str) -> bool:
    now = datetime.utcnow()
    expires_at = now + timedelta(seconds=LOCK_TTL)

    lock = lock_collection.find_one_and_update(
        {"job_name": job_name, "$or": [{"expires_at": {"$lte": now}}, {"expires_at": {"$exists": False}}]},
        {"$set": {"job_name": job_name, "expires_at": expires_at}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return lock["expires_at"] == expires_at

def release_lock(job_name: str):
    """
    Release a distributed lock for a specific job.
    """
    lock_collection.delete_one({"job_name": job_name})
