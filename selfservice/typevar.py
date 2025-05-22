# common/repositories/base_mongo_repository.py (or similar location)
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Any, Dict
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from ..db.mongo_client import get_client # Assuming mongo_client.py is in common.db

T = TypeVar('T') # Generic type for documents, if you have Pydantic models

class BaseMongoRepository(ABC):
    _database_name: str = "aws" # Default database, can be overridden
    _collection_name: str # Must be defined by subclasses

    def __init__(self, db_client: AsyncIOMotorClient):
        if not hasattr(self, '_collection_name') or not self._collection_name:
            raise NotImplementedError("Subclasses must define _collection_name")
        self.db_client = db_client
        self.collection: AsyncIOMotorCollection = db_client[self._database_name][self._collection_name]

    async def _get_document_by_criteria(self, criteria: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Helper for find operations."""
        return await self.collection.find_one(criteria)

    async def _delete_documents_by_criteria(self, criteria: Dict[str, Any]) -> int:
        """Helper for delete operations."""
        result = await self.collection.delete_many(criteria)
        return result.deleted_count

    # You can define common methods here if their implementation is truly generic
    # Or leave them abstract if only the signature is common

    @abstractmethod
    async def find(self, cluster: str, role: str) -> Optional[Dict[str, Any]]: # Or Optional[T]
        pass

    @abstractmethod
    async def delete(self, cluster: str, role: str) -> bool: # Or return count, etc.
        pass

# --- hashicorp_config_repository.py ---
# from ..common.repositories.base_mongo_repository import BaseMongoRepository
# from ..db.mongo_client import get_client # Or directly use get_client from base if preferred

class HashiCorpConfigRepository(BaseMongoRepository):
    _collection_name = "hashi"
    # _database_name = "aws" # Inherited, or override if different

    # No __init__ needed if it's the same as the base

    async def find(self, cluster: str, role: str) -> Optional[Dict[str, Any]]:
        # Specific implementation for HashiCorp find
        # Example:
        criteria = {"cluster_name": cluster, "iam_role": role}
        return await self._get_document_by_criteria(criteria)

    async def insert(self, cluster: str, role: str, data: Dict[str, Any]) -> Any: # Replace Any with actual return type
        # Specific implementation for HashiCorp insert
        # Example:
        document = {"cluster_name": cluster, "iam_role": role, **data}
        result = await self.collection.insert_one(document)
        return result.inserted_id # Or the document itself

    async def delete(self, cluster: str, role: str) -> bool:
        # Specific implementation for HashiCorp delete
        # Example:
        criteria = {"cluster_name": cluster, "iam_role": role}
        deleted_count = await self._delete_documents_by_criteria(criteria)
        return deleted_count > 0

async def get_hashicorp_config_repo() -> HashiCorpConfigRepository:
   current_client = await get_client()
   return HashiCorpConfigRepository(db_client=current_client)


# --- vault_secrets_repository.py ---
# from ..common.repositories.base_mongo_repository import BaseMongoRepository
# from ..db.mongo_client import get_client

class VaultSecretsRepository(BaseMongoRepository):
    _collection_name = "vault"

    async def find(self, cluster: str, path: str) -> Optional[Dict[str, Any]]: # Note: parameters might differ
        # Specific implementation for Vault find
        # Example:
        criteria = {"cluster_name": cluster, "secret_path": path}
        return await self._get_document_by_criteria(criteria)

    async def update(self, cluster: str, path: str, data: Dict[str, Any]) -> bool:
        # Specific implementation for Vault update
        # Example:
        criteria = {"cluster_name": cluster, "secret_path": path}
        update_doc = {"$set": data}
        result = await self.collection.update_one(criteria, update_doc, upsert=False) # Or upsert=True
        return result.modified_count > 0

    async def delete(self, cluster: str, path: str) -> bool:
        # Specific implementation for Vault delete
        # Example:
        criteria = {"cluster_name": cluster, "secret_path": path}
        deleted_count = await self._delete_documents_by_criteria(criteria)
        return deleted_count > 0

async def get_vault_secrets_repo() -> VaultSecretsRepository:
   current_client = await get_client()
   return VaultSecretsRepository(db_client=current_client)







# common/services/base_service.py
from typing import TypeVar, Generic
# from ..common.repositories.base_mongo_repository import BaseMongoRepository # If using base repo
# Or: from ..common.repositories.protocols import ConfigRepositoryProtocol # If using protocol

RepoType = TypeVar('RepoType') # Could be BaseMongoRepository or a protocol

class BaseService(Generic[RepoType]):
    def __init__(self, repository: RepoType):
        self.repository = repository

    # The get_service method is harder to make fully generic
    # without knowing the specific repository getter function.
    # This is where dependency injection frameworks shine.

# --- hashicorp_service.py ---
# from ..common.services.base_service import BaseService
# from ..repositories.hashicorp_config_repository import HashiCorpConfigRepository, get_hashicorp_config_repo

class HashiCorpService(BaseService[HashiCorpConfigRepository]):
    # __init__ is inherited

    # Specific HashiCorp service methods would go here, using self.repository
    async def get_config_details(self, cluster: str, role: str) -> Optional[Dict[str, Any]]:
        return await self.repository.find(cluster, role)

    async def create_new_config(self, cluster: str, role: str, data: Dict[str, Any]) -> Any:
        return await self.repository.insert(cluster, role, data)

    @classmethod
    async def get_service(cls) -> "HashiCorpService": # Renamed for clarity
      repo = await get_hashicorp_config_repo()
      return cls(repository=repo)


# --- vault_service.py ---
# from ..common.services.base_service import BaseService
# from ..repositories.vault_secrets_repository import VaultSecretsRepository, get_vault_secrets_repo

class VaultService(BaseService[VaultSecretsRepository]):
    # __init__ is inherited

    async def get_secret_value(self, cluster: str, path: str) -> Optional[Dict[str, Any]]:
        return await self.repository.find(cluster, path) # Assuming find method maps to this

    async def store_secret(self, cluster: str, path: str, data: Dict[str, Any]) -> bool:
        return await self.repository.update(cluster, path, data) # Or an insert/upsert method

    @classmethod
    async def get_service(cls) -> "VaultService": # Renamed for clarity
      repo = await get_vault_secrets_repo()


from typing import Generic # Make sure Generic is imported

class BaseMongoRepository(ABC, Generic[T]): # <--- This is the key change
    # ... rest of the class ...

    # Methods would then use T in their return types
    async def _get_document_by_criteria(self, criteria: Dict[str, Any]) -> Optional[T]:
        # Here you would also need logic to deserialize the Dict[str, Any] from MongoDB
        # into an instance of type T (e.g., using Pydantic's model_validate)
        raw_doc = await self.collection.find_one(criteria)
        if raw_doc:
            # return T.model_validate(raw_doc) # Example if T is a Pydantic model
            # For now, let's assume T is Dict[str, Any] for simplicity if not using models yet
            return raw_doc # Or some transformation
        return None

    @abstractmethod
    async def find(self, cluster: str, role: str) -> Optional[T]: # <--- Using T
        pass


  # Assume you have Pydantic models (or any other class)
# class HashiCorpConfigModel(BaseModel): ...
# class VaultSecretModel(BaseModel): ...

# --- hashicorp_config_repository.py ---
# class HashiCorpConfigRepository(BaseMongoRepository[HashiCorpConfigModel]): # T is now HashiCorpConfigModel
class HashiCorpConfigRepository(BaseMongoRepository[Dict[str, Any]]): # Or, if you're still using dicts
    _collection_name = "hashi"

    async def find(self, cluster: str, role: str) -> Optional[Dict[str, Any]]: # Or Optional[HashiCorpConfigModel]
        criteria = {"cluster_name": cluster, "iam_role": role}
        # If BaseMongoRepository._get_document_by_criteria returned Optional[T],
        # this would automatically be typed correctly.
        return await self._get_document_by_criteria(criteria)

    # ... insert, delete ...

# --- vault_secrets_repository.py ---
# class VaultSecretsRepository(BaseMongoRepository[VaultSecretModel]): # T is now VaultSecretModel
class VaultSecretsRepository(BaseMongoRepository[Dict[str, Any]]):
    _collection_name = "vault"

    async def find(self, cluster: str, path: str) -> Optional[Dict[str, Any]]: # Or Optional[VaultSecretModel]
        criteria = {"cluster_name": cluster, "secret_path": path}
        return await self._get_document_by_criteria(criteria)

    # ... update, delete ...
      return cls(repository=repo)
