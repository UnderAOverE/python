import motor.motor_asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from typing import Any, Dict, List, Optional, Tuple

def init_connection() -> motor.motor_asyncio.AsyncIOMotorCollection:
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client['mydatabase']
    return db['mycollection']

async def run_findall_query(
    batch_size: int = 1000,
    last_id: Optional[Any] = None,
    filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    sort: Optional[List[Tuple[str, int]]] = None
) -> List[Dict[str, Any]]:
    connection = init_connection()
    
    # Create the query with an optional filter
    query = {"_id": {"$gt": last_id}} if last_id else {}
    if filter:
        query.update(filter)
    
    # Apply projection if provided
    cursor = connection.find(query, projection)
    
    # Apply sort if provided, otherwise default to sorting by _id
    if sort:
        cursor = cursor.sort(sort)
    else:
        cursor = cursor.sort("_id")
        
    # Limit the result set to the specified batch size
    cursor = cursor.limit(batch_size)
    
    # Convert the cursor to a list with the specified batch size
    results = await cursor.to_list(length=batch_size)
    
    return results

async def findall(
    batch_size: int = 1000,
    filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    sort: Optional[List[Tuple[str, int]]] = None
) -> List[Dict[str, Any]]:
    last_id: Optional[Any] = None
    
    while True:
        results = await run_findall_query(batch_size, last_id, filter, projection, sort)
        if not results:
            break
        
        last_id = results[-1]["_id"]
        yield results  # Using yield to handle large datasets efficiently

# Example usage of the client with async context manager
async def main():
    filter = {"status": "active"}
    projection = {"_id": 1, "name": 1}
    sort = [("name", 1)]  # Sort by name in ascending order

    async for results in findall(filter=filter, projection=projection, sort=sort):
        for doc in results:
            print(doc)

asyncio.run(main())
