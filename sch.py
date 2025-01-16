# main.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
from common.logger import setup_logger
from common.database import get_mongo_client
from sub_app1.sub_app1_main import app as sub_app1_app
from sub_app2.sub_app2_main import app as sub_app2_app
from sub_app1.utils.scheduler import scheduler

# Logger Setup
setup_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    mongo_client = get_mongo_client()
    print("MongoDB client connected.")
    scheduler.start()
    print("Scheduler started.")
    try:
        yield {"mongo_client": mongo_client, "scheduler": scheduler}
    finally:
        mongo_client.close()
        print("MongoDB client closed.")
        scheduler.stop()
        print("Scheduler stopped.")

# Main FastAPI app
app = FastAPI(lifespan=lifespan)

# Register sub-apps
app.mount("/sub_app1", sub_app1_app)
app.mount("/sub_app2", sub_app2_app)
-------------------------------
# common/logger.py
import logging

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.info("Logger initialized.")
-------------------------------
# common/database.py
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"

def get_mongo_client():
    """
    Create and return a MongoDB client instance.
    The client will be shared across the application.
    """
    return MongoClient(MONGO_URI)
-------------------------------
# sub_app1/sub_app1_main.py
from fastapi import FastAPI
from sub_app1.routes.start import router as start_router
from sub_app1.routes.stop import router as stop_router

# Sub-app for sub_app1
app = FastAPI()

# Register routes
app.include_router(start_router, prefix="/start", tags=["Start"])
app.include_router(stop_router, prefix="/stop", tags=["Stop"])
-------------------------------
# sub_app1/utils/database.py
from common.database import get_mongo_client

def get_database():
    """
    Returns a specific database instance from the MongoDB client.
    """
    client = get_mongo_client()
    return client["your_database_name"]
-------------------------------
# sub_app1/utils/scheduler.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from common.database import get_mongo_client

# MongoDB connection for the job store
mongo_client = get_mongo_client()
scheduler = AsyncIOScheduler(
    jobstores={"default": MongoDBJobStore(client=mongo_client, database="scheduler_db", collection="jobs")}
)
-------------------------------
# sub_app1/routes/start.py
from fastapi import APIRouter
from sub_app1.utils.scheduler import scheduler

router = APIRouter()

@router.post("/")
async def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        return {"status": "Scheduler started."}
    return {"status": "Scheduler is already running."}
-------------------------------
# sub_app1/routes/stop.py
from fastapi import APIRouter
from sub_app1.utils.scheduler import scheduler

router = APIRouter()

@router.post("/")
async def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        return {"status": "Scheduler stopped."}
    return {"status": "Scheduler is not running."}
-------------------------------
# sub_app2/sub_app2_main.py
from fastapi import FastAPI
from sub_app2.routes.info import router as info_router
from sub_app2.routes.sales import router as sales_router
from sub_app2.routes.inventory import router as inventory_router

# Sub-app for sub_app2
app = FastAPI()

# Register routes
app.include_router(info_router, prefix="/info", tags=["Info"])
app.include_router(sales_router, prefix="/sales", tags=["Sales"])
app.include_router(inventory_router, prefix="/inventory", tags=["Inventory"])
-------------------------------
# sub_app2/routes/info.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_info():
    return {"info": "Information endpoint"}
-------------------------------
# sub_app2/routes/sales.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_sales():
    return {"sales": "Sales endpoint"}
-------------------------------
# sub_app2/routes/inventory.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_inventory():
    return {"inventory": "Inventory endpoint"}
