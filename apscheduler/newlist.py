from fastapi import APIRouter, Query, Path, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from pymongo import MongoClient
from bson import ObjectId

router = APIRouter()

# Pagination parameters model
class PaginationParams(BaseModel):
    skip: int = Query(0, description="Number of items to skip")
    limit: int = Query(10, description="Number of items to return")

# Job filter model for POST requests
class JobFilter(BaseModel):
    job_ids: Optional[List[str]] = None
    names: Optional[List[str]] = None

@router.get("/jobs")
async def get_jobs(
    job_ids: Optional[List[str]] = Query(None, description="List of job IDs to fetch"),
    names: Optional[List[str]] = Query(None, description="List of job names to fetch"),
    pagination: PaginationParams = Depends(),
    db_client: MongoClient = Depends(get_db_client),
):
    """
    Fetch jobs with pagination using query parameters.
    """
    try:
        jobs_collection = db_client.get_database("your_database").get_collection("your_collection")
        query_filter = {}
        if job_ids:
            query_filter["_id"] = {"$in": [ObjectId(jid) for jid in job_ids]}
        if names:
            query_filter["name"] = {"$in": names}
        
        total_jobs = jobs_collection.count_documents(query_filter)
        jobs = (
            jobs_collection.find(query_filter)
            .skip(pagination.skip)
            .limit(pagination.limit)
        )
        
        results = []
        for job in jobs:
            results.append({
                "id": str(job["_id"]),
                "name": job["name"],
                "status": job.get("status"),
                "next_run_time": job.get("next_run_time"),
                # Add more fields as needed
            })
        
        return {"total": total_jobs, "jobs": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs")
async def post_jobs(
    body: JobFilter,
    pagination: PaginationParams = Depends(),
    db_client: MongoClient = Depends(get_db_client),
):
    """
    Fetch jobs with pagination using a request body.
    """
    try:
        jobs_collection = db_client.get_database("your_database").get_collection("your_collection")
        query_filter = {}
        if body.job_ids:
            query_filter["_id"] = {"$in": [ObjectId(jid) for jid in body.job_ids]}
        if body.names:
            query_filter["name"] = {"$in": body.names}
        
        total_jobs = jobs_collection.count_documents(query_filter)
        jobs = (
            jobs_collection.find(query_filter)
            .skip(pagination.skip)
            .limit(pagination.limit)
        )
        
        results = []
        for job in jobs:
            results.append({
                "id": str(job["_id"]),
                "name": job["name"],
                "status": job.get("status"),
                "next_run_time": job.get("next_run_time"),
                # Add more fields as needed
            })
        
        return {"total": total_jobs, "jobs": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_job_by_id(
    job_id: str = Path(..., description="The ID of the job to fetch"),
    db_client: MongoClient = Depends(get_db_client),
):
    """
    Fetch a specific job by its ID using a path parameter.
    """
    try:
        jobs_collection = db_client.get_database("your_database").get_collection("your_collection")
        job = jobs_collection.find_one({"_id": ObjectId(job_id)})
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        result = {
            "id": str(job["_id"]),
            "name": job["name"],
            "status": job.get("status"),
            "next_run_time": job.get("next_run_time"),
            # Add more fields as needed
        }
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        
----------------

@router.get("/jobs")
async def get_jobs(
    job_ids: Optional[List[str]] = Query(None, description="List of job IDs to fetch"),
    names: Optional[List[str]] = Query(None, description="List of job names to fetch"),
    page: int = Query(1, description="The page number to fetch (1-based index)"),
    limit: int = Query(10, description="Number of items per page"),
    db_client: MongoClient = Depends(get_db_client),
):
    """
    Fetch jobs with pagination using query parameters.
    Returns total jobs, current page, total pages, and remaining pages.
    """
    try:
        if page < 1:
            raise HTTPException(status_code=400, detail="Page number must be 1 or greater")

        jobs_collection = db_client.get_database("your_database").get_collection("your_collection")
        query_filter = {}
        
        # Build the query filter
        if job_ids:
            query_filter["_id"] = {"$in": [ObjectId(jid) for jid in job_ids]}
        if names:
            query_filter["name"] = {"$in": names}
        
        # Get the total count of jobs matching the query
        total_jobs = jobs_collection.count_documents(query_filter)
        
        # Calculate skip based on page and limit
        skip = (page - 1) * limit
        
        # Fetch the jobs based on pagination
        jobs = (
            jobs_collection.find(query_filter)
            .skip(skip)
            .limit(limit)
        )
        
        results = []
        for job in jobs:
            results.append({
                "id": str(job["_id"]),
                "name": job["name"],
                "status": job.get("status"),
                "next_run_time": job.get("next_run_time"),
                # Add more fields as needed
            })
        
        # Calculate pagination metadata
        total_pages = (total_jobs + limit - 1) // limit  # Round up
        remaining_pages = total_pages - page

        return {
            "total_jobs": total_jobs,
            "current_page": page,
            "total_pages": total_pages,
            "remaining_pages": max(0, remaining_pages),
            "jobs": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))