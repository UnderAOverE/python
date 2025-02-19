# src/api/main.py
import uvloop
from fastapi import FastAPI, Depends
from typing import Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
import asyncio
from fastapi import FastAPI

uvloop.install()
app = FastAPI()

# --------------------- Scheduler Setup (Now a Dependency) ---------------------

async def create_scheduler():
    """Creates and configures the APScheduler."""
    # Replace with your actual MongoDB connection details
    # I am removing this and passing a mock object
    #jobstores = {
    #    'default': MongoDBJobStore(database='your_database_name', client=mongo_client)
    #}
    scheduler = AsyncIOScheduler() #jobstores=jobstores)
    return scheduler


# Dependency Injection function
async def get_scheduler():
    """Dependency injection function to get the scheduler."""
    scheduler = await create_scheduler()
    return scheduler


@app.on_event("startup")
async def startup_event(scheduler: AsyncIOScheduler = Depends(get_scheduler)):
    """Starts the scheduler on application startup."""
    # Now you can call method like:
    #scheduler.add_job
    print ("FastAPI: Starting Schedular")
    await scheduler.start()
    app.state.scheduler = scheduler # IMPORTANT store the scheduler
    #await app.state.scheduler.start() # use a mock or non-mock scheduler

@app.on_event("shutdown")
async def shutdown_event():
    """Shuts down the scheduler on application shutdown."""
    await app.state.scheduler.shutdown()

@app.get("/")
async def root():
    """Simple root endpoint."""
    return {"message": "Hello World"}
