import asyncio
from datetime import datetime
from pymongo import MongoClient
from fastapi import FastAPI

# Define the Event Notifier
class ErrorEventNotifier:
    def __init__(self):
        self._subscribers = []

    def subscribe(self, subscriber):
        self._subscribers.append(subscriber)

    def notify(self, error_message, metadata):
        for subscriber in self._subscribers:
            subscriber(error_message, metadata)

# MongoDB logging function
async def log_to_mongo(error_message, metadata):
    """
    Log error information to the MongoDB 'errors' collection.
    """
    MONGO_URI = "mongodb://localhost:27017/"
    client = MongoClient(MONGO_URI)
    db = client["your_database_name"]
    errors_collection = db["errors"]

    error_document = {
        "error_message": error_message,
        "module": metadata.get("module"),
        "function": metadata.get("function"),
        "timestamp": datetime.utcnow()
    }
    errors_collection.insert_one(error_document)
    print(f"Logged error to MongoDB: {error_document}")

# Global error notifier instance
error_notifier = ErrorEventNotifier()

# Subscribe the MongoDB logger to the error notifier
error_notifier.subscribe(log_to_mongo)

# Starting the asyncio event loop
loop = asyncio.get_event_loop()

async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager for startup and shutdown of the event loop.
    """
    print("Starting the event loop...")
    loop.run_forever()

# example.py
from fastapi import FastAPI
from error_handler import error_notifier

# Example function that triggers errors (sync)
def example():
    try:
        # Simulate an issue
        raise Exception("Service is not available")
    except Exception as e:
        # Notify subscribers (log error to MongoDB)
        metadata = {
            "module": __name__,
            "function": "example"
        }
        error_notifier.notify(str(e), metadata)

# FastAPI app
app = FastAPI()

@app.get("/")
def read_root():
    # Call the example function
    example()
    return {"message": "Error handled and logged"}


    # Handle graceful shutdown on app stop
    yield

    print("Shutting down event loop...")
    loop.stop()

# FastAPI app setup
app = FastAPI(lifespan=lifespan)
