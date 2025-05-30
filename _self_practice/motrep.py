from typing import TypeVar, Generic, Dict, Any, List, Optional, Union
from abc import ABC
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

T = TypeVar("T")


class MotorRepository(ABC, Generic[T]):
    database_name: str
    collection_name: str

    def __init__(self, db_client: AsyncIOMotorClient) -> None:
        """
        MotorRepository constructor. Base repository for MongoDB (Motor) collections.
        Make sure the inheriting class defines _collection_name and _database_name.
        :param db_client: MongoDB client
        :type db_client: AsyncIOMotorClient
        :raises NotImplementedError: If the subclass does not define _collection_name or _database_name
        """
        if not hasattr(self, "_collection_name") or not self._collection_name:
            raise NotImplementedError("Subclasses must define _collection_name")

        if not hasattr(self, "_database_name") or not self._database_name:
            raise NotImplementedError("Subclasses must define _database_name")

        self._db_client = db_client
        self._collection: AsyncIOMotorCollection = db_client[self._database_name][self._collection_name]

    async def _get_documents_by_criteria(self, criteria: Dict[str, Any], many=False) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
        """
        Helper for find operations.

        :param criteria: The criteria to filter the documents.
        :type criteria: Dict[str, Any]
        :param many: Whether to return multiple documents or a single document.
        :type many: bool
        :return: A list of documents or a single document based on the criteria.
        :rtype: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]
        """
        if many:
            return await self._collection.find(
                filter=criteria.get("filter", {}),
                projection=criteria.get("projection", None),
                sort=criteria.get("sort", None),
                skip=criteria.get("skip", 0),
                limit=criteria.get("limit", 0),
            ).to_list(length=criteria.get("limit", 100))
        else:
            return await self._collection.find_one(
                filter=criteria.get("filter", {}),
                projection=criteria.get("projection", None),
                sort=criteria.get("sort", None),
            )

    async def _delete_documents_by_criteria(self, criteria: Dict[str, Any], many=False) -> None:
        """
        Helper for delete operations.

        :param criteria: The criteria to filter the documents.
        :type criteria: Dict[str, Any]
        :param many: Whether to delete multiple documents or a single document.
        :type many: bool
        :return: None
        """
        if many:
            await self._collection.delete_many(criteria.get("filter", {}))
        else:
            await self._collection.delete_one(criteria.get("filter", {}))

    async def _insert_documents(self, documents: Union[Dict[str, Any], List[Dict[str, Any]]], many=False) -> None:
        """
        Helper for insert operations.

        :param documents: The documents to insert.
        :type documents: Union[Dict[str, Any], List[Dict[str, Any]]]
        :param many: Whether to insert multiple documents or a single document.
        :type many: bool
        :return: None
        """
        if many:
            await self._collection.insert_many(documents)
        else:
            await self._collection.insert_one(documents)