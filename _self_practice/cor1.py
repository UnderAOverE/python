# --- Filename: my_project/app_config.py ---
# (Could be environment variables or a more complex config setup)
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "my_async_app_db"

# --- Filename: my_project/common/py_object_id.py ---
from bson import ObjectId
from pydantic import GetJsonSchemaHandler
from pydantic_core import core_schema

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, _field):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema_obj: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> dict:
        json_schema = handler(core_schema_obj)
        json_schema.update(type="string")
        return json_schema

# --- Filename: my_project/models/audit_model.py ---
from typing import Any, Optional, Dict
from datetime import datetime
from pydantic import BaseModel, Field
from bson import ObjectId
from ..common.py_object_id import PyObjectId # Adjusted relative import

class AuditLogModel(BaseModel):
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
    user_id: str
    action: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True
        json_encoders = {ObjectId: str, PyObjectId: str}
        arbitrary_types_allowed = True

# --- Filename: my_project/repositories/base_motor_repository.py ---
import pymongo
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Dict, Any, List, Optional, Tuple, AsyncGenerator
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.results import InsertOneResult # ... and others as needed

T = TypeVar("T")
MongoDoc = Dict[str, Any]

class BaseMotorRepository(ABC, Generic[T]):
    _database_name: str
    _collection_name: str

    def __init__(self, db_client: AsyncIOMotorClient, database_name: str, collection_name: str) -> None:
        # Allow overriding DB/Collection name if provided, else use class attributes
        self._database_name = database_name or self.__class__._database_name
        self._collection_name = collection_name or self.__class__._collection_name

        if not self._collection_name:
            raise NotImplementedError(f"{self.__class__.__name__} must define or be provided _collection_name")
        if not self._database_name:
            raise NotImplementedError(f"{self.__class__.__name__} must define or be provided _database_name")

        self._db_client: AsyncIOMotorClient = db_client
        self._collection: AsyncIOMotorCollection = self._db_client[self._database_name][self._collection_name]

    @abstractmethod
    def _map_to_model(self, doc: MongoDoc) -> T: pass

    @abstractmethod
    def _map_to_document(self, model: T) -> MongoDoc: pass

    async def _execute_insert_one(self, document: MongoDoc) -> InsertOneResult:
        try:
            return await self._collection.insert_one(document)
        except Exception as e:
            print(f"DB ERROR (insert_one) in {self._collection_name}: {e}") # Replace with proper logging
            raise

    async def _execute_find_one(self, filter_query: MongoDoc) -> Optional[MongoDoc]:
        try:
            return await self._collection.find_one(filter_query)
        except Exception as e:
            print(f"DB ERROR (find_one) in {self._collection_name}: {e}")
            raise
    # ... other _execute methods (find_many, update, delete, count) ...

    # --- Example Public Method ---
    async def create(self, entity: T) -> T:
        doc_to_insert = self._map_to_document(entity)
        if "_id" in doc_to_insert and doc_to_insert["_id"] is None:
            del doc_to_insert["_id"] # Let MongoDB generate if ID is None
        elif "_id" not in doc_to_insert and hasattr(entity, 'id') and entity.id is None:
             pass # Let MongoDB generate if ID is not part of the document at all

        result = await self._execute_insert_one(doc_to_insert)
        # To ensure the model has the ID (especially if DB generated)
        # you might fetch it back or assign if client-generated
        if hasattr(entity, 'id'):
            setattr(entity, 'id', result.inserted_id) # Simplistic assignment
        return entity


# --- Filename: my_project/repositories/audit_repository.py ---
from motor.motor_asyncio import AsyncIOMotorClient
from .base_motor_repository import BaseMotorRepository, MongoDoc
from ..models.audit_model import AuditLogModel # Adjusted relative import
from ..app_config import DATABASE_NAME # Get DB name from config


class AuditLogMotorRepository(BaseMotorRepository[AuditLogModel]):
    _database_name: str = DATABASE_NAME # Set default from config
    _collection_name: str = "audit_logs"

    # __init__ can be inherited if db_name and coll_name are passed or set as class vars

    def _map_to_model(self, doc: MongoDoc) -> AuditLogModel:
        return AuditLogModel(**doc)

    def _map_to_document(self, model: AuditLogModel) -> MongoDoc:
        return model.model_dump(by_alias=True, exclude_none=True)

    # Create is inherited from BaseMotorRepository and will use these mapping methods
    # Add other specific query methods if needed


# --- Filename: my_project/services/audit_service.py ---
from typing import Optional, Any
from ..repositories.audit_repository import AuditLogMotorRepository # Adjusted relative import
from ..models.audit_model import AuditLogModel # Adjusted relative import

class AuditService:
    def __init__(self, audit_repo: AuditLogMotorRepository):
        self.audit_repo = audit_repo

    async def record_event(self, user_id: str, action: str, details: Optional[Dict[str, Any]] = None) -> AuditLogModel:
        print(f"SERVICE: Recording event for user '{user_id}', action '{action}'")
        audit_entry = AuditLogModel(user_id=user_id, action=action, details=details)
        created_log = await self.audit_repo.create(audit_entry) # Await the async repo call
        print(f"SERVICE: Event recorded with ID: {created_log.id}")
        return created_log

    async def get_log_by_id(self, log_id: Any) -> Optional[AuditLogModel]:
        """Illustrative method; Base repo would need get_by_id implementation"""
        print(f"SERVICE: Fetching log with ID: {log_id}")
        # Assuming BaseMotorRepository has a get_by_id or find_one implementation
        # For this example, let's assume find_one on base can be used if it exists
        # and the concrete repo implements the public abstract method
        # This part needs the concrete repo to implement a public get_by_id or similar
        # For now, let's imagine a basic find_one for illustration (needs implementation in base/concrete)
        if hasattr(self.audit_repo, 'find_one'): # Check if find_one is implemented
            obj_id = log_id if isinstance(log_id, ObjectId) else ObjectId(str(log_id))
            doc = await self.audit_repo._execute_find_one({"_id": obj_id}) # Using internal for example
            return self.audit_repo._map_to_model(doc) if doc else None
        print(f"SERVICE: get_by_id not fully implemented in this example for {log_id}")
        return None


# --- Filename: my_project/handlers/base_handler.py ---
from abc import ABC, abstractmethod
from typing import Any, Optional

class AsyncRequestContext: # Simple context/request object
    def __init__(self, data: Any):
        self.data = data
        self.is_handled = False
        self.result: Any = None
        self.current_user_id: Optional[str] = None # Example field for auth

class AsyncBaseHandler(ABC):
    def __init__(self, next_handler: Optional['AsyncBaseHandler'] = None):
        self._next_handler = next_handler

    @abstractmethod
    async def handle(self, context: AsyncRequestContext) -> None:
        """Handle the request or pass it to the next handler."""
        pass

    async def _pass_to_next(self, context: AsyncRequestContext) -> None:
        if self._next_handler:
            await self._next_handler.handle(context)
        else:
            print("HANDLER_CHAIN: End of chain, request not fully handled or no default.")


# --- Filename: my_project/handlers/audit_handlers.py ---
from .base_handler import AsyncBaseHandler, AsyncRequestContext # Adjusted relative import
from ..services.audit_service import AuditService # Adjusted relative import
from typing import Any, Optional

class AuditLoggingHandler(AsyncBaseHandler):
    def __init__(self, audit_service: AuditService, next_handler: Optional[AsyncBaseHandler] = None):
        super().__init__(next_handler)
        self.audit_service = audit_service

    async def handle(self, context: AsyncRequestContext) -> None:
        action_description = "unknown_action"
        if isinstance(context.data, dict) and "action_type" in context.data:
            action_description = context.data["action_type"]

        user_id = context.current_user_id or "anonymous" # Assuming auth might set current_user_id

        print(f"AUDIT_HANDLER: Logging action '{action_description}' for user '{user_id}' before passing.")
        await self.audit_service.record_event( # Async call to service
            user_id=user_id,
            action=f"PRE_HANDLE: {action_description}",
            details={"request_data_summary": str(context.data)[:100]} # Example detail
        )

        await self._pass_to_next(context) # Await the rest of the chain

        if context.is_handled:
            print(f"AUDIT_HANDLER: Logging action '{action_description}' for user '{user_id}' after handling.")
            await self.audit_service.record_event( # Async call to service
                user_id=user_id,
                action=f"POST_HANDLE: {action_description}",
                details={"result_summary": str(context.result)[:100]} # Example detail
            )

class BusinessLogicHandler(AsyncBaseHandler):
    async def handle(self, context: AsyncRequestContext) -> None:
        if isinstance(context.data, dict) and context.data.get("action_type") == "process_data":
            print("BUSINESS_LOGIC_HANDLER: Processing data...")
            # Simulate some business logic
            await asyncio.sleep(0.2) # Simulate async work
            context.result = {"message": "Data processed successfully", "processed_data": context.data}
            context.is_handled = True
            print("BUSINESS_LOGIC_HANDLER: Data processing complete.")
            # No explicit call to _pass_to_next here means this handler concludes this path
            # or you could call it if further processing is always needed.
        else:
            await self._pass_to_next(context) # If this handler can't handle it


# --- Filename: my_project/main.py ---
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from .app_config import MONGO_URI, DATABASE_NAME # Adjusted relative import
from .repositories.audit_repository import AuditLogMotorRepository # Adjusted relative import
from .services.audit_service import AuditService # Adjusted relative import
from .handlers.audit_handlers import AuditLoggingHandler, BusinessLogicHandler # Adjusted relative import
from .handlers.base_handler import AsyncRequestContext # Adjusted relative import

async def run_application():
    print("MAIN: Application starting...")
    motor_client = None
    try:
        # 1. Initialize MongoDB Client (Motor)
        motor_client = AsyncIOMotorClient(MONGO_URI)
        # You might want to ping the server to ensure connection
        await motor_client.admin.command('ping')
        print("MAIN: Connected to MongoDB.")

        # 2. Setup Repositories
        # Pass explicit db/collection names to base repo constructor if not relying on class attributes
        audit_repo = AuditLogMotorRepository(
            db_client=motor_client,
            database_name=DATABASE_NAME, # from app_config
            collection_name="audit_logs" # specific to this repo
        )
        print("MAIN: Repositories initialized.")

        # 3. Setup Services
        audit_service = AuditService(audit_repo=audit_repo)
        print("MAIN: Services initialized.")

        # 4. Setup Chain of Responsibility Handlers
        # Handlers are chained in reverse order of execution for setup
        business_handler = BusinessLogicHandler() # This is the last "real" processing handler
        audit_logger_handler = AuditLoggingHandler(audit_service=audit_service, next_handler=business_handler)
        # You could have an AuthenticationHandler before the audit_logger_handler, etc.
        first_handler_in_chain = audit_logger_handler
        print("MAIN: Handler chain configured.")

        # 5. Simulate processing some requests
        print("\nMAIN: --- Simulating Request 1 (Process Data) ---")
        request_data_1 = {"action_type": "process_data", "payload": {"value": 123}}
        context1 = AsyncRequestContext(data=request_data_1)
        context1.current_user_id = "test_user_001" # Simulate an authenticated user
        await first_handler_in_chain.handle(context1) # Await the chain execution
        if context1.is_handled:
            print(f"MAIN: Request 1 handled. Result: {context1.result}")
        else:
            print("MAIN: Request 1 was not handled by the chain.")

        print("\nMAIN: --- Simulating Request 2 (Unknown Action) ---")
        request_data_2 = {"action_type": "unknown_action", "info": "some info"}
        context2 = AsyncRequestContext(data=request_data_2)
        await first_handler_in_chain.handle(context2)
        if context2.is_handled:
            print(f"MAIN: Request 2 handled. Result: {context2.result}")
        else:
            print("MAIN: Request 2 was not handled by the chain (as expected for this example).")

    except Exception as e:
        print(f"MAIN: An error occurred: {e}")
    finally:
        if motor_client:
            motor_client.close()
            print("MAIN: MongoDB connection closed.")
        print("MAIN: Application finished.")

if __name__ == "__main__":
    # This structure assumes you run main.py from the 'my_project' directory
    # or that 'my_project' is in PYTHONPATH for the relative imports to work.
    # To run: python -m my_project.main (if my_project is a package with __init__.py)
    # or adjust imports if running main.py directly.
    # For simplicity of running this single file, if you save all this as one .py file,
    # you'd comment out the relative imports like `from ..common...` and ensure classes
    # are defined before use.

    # To make it runnable as a single script (by removing relative imports and putting all in one file):
    # You would need to define PyObjectId, AuditLogModel, BaseMotorRepository etc. *before* they are used
    # by AuditLogMotorRepository, AuditService, handlers, and main. The order above is mostly correct for this.
    # And change imports like `from ..common.py_object_id import PyObjectId`
    # to `from py_object_id import PyObjectId` if they were in the same directory,
    # or just ensure the classes are defined in the same file before use.

    # For this stitched version, let's assume all classes are in this single file.
    # Therefore, we would remove the relative package imports.
    # The class definitions above already respect dependency order.
    asyncio.run(run_application())
