# Example in a FastAPI endpoint (main.py or similar)
from fastapi import FastAPI, Depends, HTTPException
# Assuming you have Pydantic models defined in, say, src/eks_backend/models.py
from src.eks_backend.models import HashiCorpConfigCreateModel, HashiCorpConfigViewModel
from src.eks_backend.services.hashicorp_service import HashiCorpService, get_hashi_service_dependency # (DI version)

app = FastAPI()

@app.post("/hashicorp-configs/", response_model=HashiCorpConfigViewModel)
async def create_hashicorp_config_endpoint(
    config_data: HashiCorpConfigCreateModel, # FastAPI automatically validates request body against this model
    hashi_service: HashiCorpService = Depends(get_hashi_service_dependency)
):
    try:
        # config_data is now a validated Pydantic model instance
        created_config = await hashi_service.create_new_config(config_data)
        return created_config
    except Exception as e: # More specific exception handling is better
        raise HTTPException(status_code=500, detail=str(e))

# If not using FastAPI's DI, it might look like:
# async def create_config_manually():
#     raw_data = {"cluster": "test", "role": "admin", "some_setting": "value"} # From some source
#     try:
#         config_data_model = HashiCorpConfigCreateModel(**raw_data)
#     except ValidationError as e:
#         print(f"Validation Error: {e}")
#         return
#
#     hashi_service = await HashiCorpService.get_service() # Using your classmethod factory
#     created_config = await hashi_service.create_new_config(config_data_model)
#     print(f"Created: {created_config}")


# src/eks_backend/services/hashicorp_service.py
from ..models import HashiCorpConfigCreateModel, HashiCorpConfigViewModel # Assuming ViewModel is for output
from ..repositories.hashicorp_config_repository import HashiCorpConfigRepository
# from ..common.services.base_service import BaseService # If using base service

class HashiCorpService: # (Simplified version without BaseService for clarity here)
    def __init__(self, hashi_repo: HashiCorpConfigRepository):
       self.hashi_repo = hashi_repo

    async def create_new_config(self, config_to_create: HashiCorpConfigCreateModel) -> HashiCorpConfigViewModel:
        # Business logic here, e.g.:
        # if await self.hashi_repo.find(config_to_create.cluster, config_to_create.role):
        #     raise ValueError("Config already exists for this cluster and role")

        # Pass the model (or parts of it) to the repository
        # The repository's 'insert' method now needs to accept the model or its relevant fields.
        inserted_id_or_doc = await self.hashi_repo.insert(
            cluster=config_to_create.cluster_name,
            role=config_to_create.iam_role,
            data_model=config_to_create # Pass the whole model
            # Or: data=config_to_create.model_dump(exclude={'cluster_name', 'iam_role'}) # If repo expects dict
        )

        # Construct and return a ViewModel, perhaps including the ID or full created document
        # This depends on what your repository's insert method returns.
        # For simplicity, let's assume insert returns the created document as a dict/model
        if isinstance(inserted_id_or_doc, dict): # If repo returns a dict
             return HashiCorpConfigViewModel(**inserted_id_or_doc)
        elif hasattr(inserted_id_or_doc, 'model_dump'): # If repo returns a model (e.g. using Generic[T])
             return HashiCorpConfigViewModel(**inserted_id_or_doc.model_dump())
        else: # If it's just an ID, you might need to fetch the full doc or construct ViewModel differently
             # This part needs careful design based on repo return values
             # For now, let's assume a simple pass-through if the repo returns the ViewModel directly
             if isinstance(inserted_id_or_doc, HashiCorpConfigViewModel):
                 return inserted_id_or_doc
             raise TypeError("Unexpected return type from repository insert")


    # ... other service methods ...

    @classmethod
    async def get_service(cls) -> "HashiCorpService": # Your factory
      from ..repositories.hashicorp_config_repository import get_hashicorp_config_repo
      repo = await get_hashicorp_config_repo()
      return cls(hashi_repo=repo)



# src/eks_backend/repositories/hashicorp_config_repository.py
from typing import Optional, Any, Dict
from ..common.repositories.base_mongo_repository import BaseMongoRepository # Assuming T is Dict[str, Any] for now
from ..models import HashiCorpConfigCreateModel # Import the model for type hinting

# class HashiCorpConfigRepository(BaseMongoRepository[HashiCorpConfigViewModel]): # If T is ViewModel
class HashiCorpConfigRepository(BaseMongoRepository[Dict[str, Any]]): # Current state
    _collection_name = "hashi"

    async def find(self, cluster: str, role: str) -> Optional[Dict[str, Any]]:
        criteria = {"cluster_name": cluster, "iam_role": role}
        # If using Pydantic models with Generic[T]:
        # doc = await self._get_document_by_criteria(criteria)
        # return HashiCorpConfigViewModel.model_validate(doc) if doc else None
        return await self._get_document_by_criteria(criteria)

    # Modified insert method to accept the model
    async def insert(self, cluster: str, role: str, data_model: HashiCorpConfigCreateModel) -> Dict[str, Any]:
        # Prepare document for MongoDB.
        # `cluster` and `role` are already part of `data_model` if defined in Pydantic model,
        # or you might explicitly set them if they are top-level keys in your DB schema.
        document_to_insert = data_model.model_dump() # Converts Pydantic model to dict

        # If your DB schema has cluster_name and iam_role as top-level fields and
        # data_model contains them:
        # document_to_insert = {
        #     "cluster_name": data_model.cluster_name,
        #     "iam_role": data_model.iam_role,
        #     **data_model.model_dump(exclude={'cluster_name', 'iam_role'}) # other fields
        # }

        result = await self.collection.insert_one(document_to_insert)
        # Return the full document after insertion, including the _id
        created_document = await self.collection.find_one({"_id": result.inserted_id})
        if not created_document:
            raise Exception("Failed to retrieve document after insert") # Should not happen normally
        return created_document # This is a Dict[str, Any]

    async def delete(self, cluster: str, role: str) -> bool:
        criteria = {"cluster_name": cluster, "iam_role": role}
        deleted_count = await self._delete_documents_by_criteria(criteria)
        return deleted_count > 0

# ... get_hashicorp_config_repo ...
