import pymongo
from typing import Any, Dict, List, Optional, Tuple

def init_connection() -> pymongo.collection.Collection:
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client['mydatabase']
    return db['mycollection']

def run_findall_query(
    skip: int,
    batch_size: int = 1000,
    filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    sort: Optional[List[Tuple[str, int]]] = None
) -> List[Dict[str, Any]]:
    connection = init_connection()
    
    # Create the query with an optional filter
    query = filter if filter else {}
    
    # Apply projection if provided
    cursor = connection.find(query, projection)
    
    # Apply sort if provided, otherwise default to sorting by _id
    if sort:
        cursor = cursor.sort(sort)
    else:
        cursor = cursor.sort("_id")
        
    # Skip the specified number of documents
    cursor = cursor.skip(skip).limit(batch_size)
    
    # Convert the cursor to a list with the specified batch size
    results = list(cursor)
    
    return results

def findall(
    batch_size: int = 1000,
    filter: Optional[Dict[str, Any]] = None,
    projection: Optional[Dict[str, int]] = None,
    sort: Optional[List[Tuple[str, int]]] = None
) -> List[Dict[str, Any]]:
    skip: int = 0
    
    while True:
        results = run_findall_query(skip, batch_size, filter, projection, sort)
        if not results:
            break
        
        yield results  # Using yield to handle large datasets efficiently
        
        skip += batch_size

# Example usage of the client
def main():
    filter = {"status": "active"}
    projection = {"_id": 1, "name": 1}
    sort = [("name", 1)]  # Sort by name in ascending order

    for results in findall(filter=filter, projection=projection, sort=sort):
        for doc in results:
            print(doc)

if __name__ == "__main__":
    main()
