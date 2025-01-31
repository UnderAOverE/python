# error_notifier.py
import asyncio
import motor.motor_asyncio
from datetime import datetime

# Define the Event Notifier
class ErrorEventNotifier:
    def __init__(self):
        self._subscribers = []

    def subscribe(self, subscriber):
        self._subscribers.append(subscriber)

    def notify(self, error_message, metadata):
        # Run the notification process asynchronously in the background
        asyncio.create_task(self._notify_subscribers(error_message, metadata))

    async def _notify_subscribers(self, error_message, metadata):
        # Notify all subscribers asynchronously
        tasks = [subscriber(error_message, metadata) for subscriber in self._subscribers]
        await asyncio.gather(*tasks)

# Async MongoDB logging function using motor
async def log_to_mongo(error_message, metadata):
    """
    Log error information to the MongoDB 'errors' collection asynchronously.
    """
    MONGO_URI = "mongodb://localhost:27017/"
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = client["your_database_name"]
    errors_collection = db["errors"]

    error_document = {
        "error_message": error_message,
        "module": metadata.get("module"),
        "function": metadata.get("function"),
        "timestamp": datetime.utcnow()
    }
    
    await errors_collection.insert_one(error_document)
    print(f"Logged error to MongoDB: {error_document}")


# error_handler.py
import asyncio
import signal
from error_notifier import ErrorEventNotifier, log_to_mongo

class ErrorHandler:
    def __init__(self):
        # Set up the observer pattern
        self.error_notifier = ErrorEventNotifier()
        self.error_notifier.subscribe(log_to_mongo)
        self.loop = asyncio.get_event_loop()

    def start(self):
        """
        Start the asyncio event loop and handle graceful/ungraceful exits.
        """
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        print("Starting the error handler...")
        self.loop.run_forever()

    def shutdown(self, signal_num=None, frame=None):
        """
        Handle graceful shutdown.
        """
        print("Gracefully shutting down...")
        self.loop.stop()

# Initialize the error handler
error_handler = ErrorHandler()


# example.py
from error_handler import error_handler

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
        error_handler.error_notifier.notify(str(e), metadata)

# Run the example function (this will be sync, but logging happens in the background)
example()

# Start the error handler (this will keep the event loop running)
error_handler.start()
