import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from typing import List, Dict, Optional
from pydantic import BaseModel
from pymongo import MongoClient # Importing MongoClient even though we don't directly use it

# ------------------ Simulated MongoDB ------------------
# (Replace with actual MongoDB connection if you want to connect to a real database)
class SimulatedMongoDB:
    def __init__(self):
        self.collections: Dict[str, List[Dict]] = {}

    def get_collection(self, collection_name: str) -> List[Dict]:
        if collection_name not in self.collections:
            self.collections[collection_name] = []
        return self.collections[collection_name]

    # Simulate insert_one
    def insert_one(self, collection_name: str, document: Dict) -> None:
        collection = self.get_collection(collection_name)
        collection.append(document)

    # Simulate find_one
    def find_one(self, collection_name: str, query: Dict) -> Optional[Dict]:
        collection = self.get_collection(collection_name)
        for doc in collection:
            if all(doc.get(key) == value for key, value in query.items()):
                return doc
        return None  # Return None if no matching document is found.

    # Simulate find
    def find(self, collection_name: str, query: Dict = None) -> List[Dict]:
      """Simulates the find method with an optional query."""
      collection = self.get_collection(collection_name)
      if query is None:
          return collection  # Return all documents if no query is provided.

      results = []
      for doc in collection:
          if all(doc.get(key) == value for key, value in query.items()):
              results.append(doc)
      return results


    # Simulate delete_one
    def delete_one(self, collection_name: str, query: Dict) -> bool:  # Returns True if deleted, False otherwise.
        collection = self.get_collection(collection_name)
        for i, doc in enumerate(collection):
            if all(doc.get(key) == value for key, value in query.items()):
                del collection[i]
                return True  # Indicate deletion
        return False # Indicate not found

    # Simulate update_one
    def update_one(self, collection_name: str, query: Dict, update: Dict) -> bool:  # Returns True if updated, False otherwise.
      collection = self.get_collection(collection_name)
      for doc in collection:
          if all(doc.get(key) == value for key, value in query.items()):
              for key, value in update.get("$set", {}).items(): # Only handles $set operator
                  doc[key] = value
              return True  # Indicate update
      return False # Indicate not found


# ------------------ FastAPI App ------------------
app = FastAPI()
db = SimulatedMongoDB()  # Initialize the simulated database

class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float

@app.post("/items/")
async def create_item(item: Item):
    item_dict = item.dict()
    db.insert_one("items", item_dict)
    return item_dict

@app.get("/items/{item_name}")
async def read_item(item_name: str):
    item = db.find_one("items", {"name": item_name})
    if item is None:
        return {"error": "Item not found"}
    return item

@app.get("/items/")
async def list_items():
    items = db.find("items") # return all
    return items

@app.delete("/items/{item_name}")
async def delete_item(item_name: str):
    deleted = db.delete_one("items", {"name": item_name})
    if not deleted:
        return {"error": "Item not found"}
    return {"message": "Item deleted"}

@app.put("/items/{item_name}")
async def update_item(item_name: str, item: Item):
  updated = db.update_one("items", {"name": item_name}, {"$set": item.dict()}) #simplified update
  if not updated:
      return {"error": "Item not found"}
  return {"message": "Item updated"}

# ------------------ Pytest Tests ------------------
client = TestClient(app)  # Create a TestClient instance

def test_create_item():
    response = client.post(
        "/items/",
        json={"name": "Test Item", "description": "A test item", "price": 10.0},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "Test Item", "description": "A test item", "price": 10.0}

def test_read_item():
    # First, create an item (using the create_item endpoint)
    client.post(
        "/items/",
        json={"name": "Read Item", "description": "Item to read", "price": 20.0},
    )

    response = client.get("/items/Read Item")
    assert response.status_code == 200
    assert response.json() == {"name": "Read Item", "description": "Item to read", "price": 20.0}

def test_read_item_not_found():
    response = client.get("/items/NonExistentItem")
    assert response.status_code == 200  # or 404 if you change your endpoint
    assert response.json() == {"error": "Item not found"}

def test_list_items():
    # Create some items first
    client.post("/items/", json={"name": "List Item 1", "price": 5.0})
    client.post("/items/", json={"name": "List Item 2", "price": 7.5})

    response = client.get("/items/")
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list) #it should be a list
    assert any(item["name"] == "List Item 1" for item in items)
    assert any(item["name"] == "List Item 2" for item in items)

def test_delete_item():
    # First, create an item to delete
    client.post("/items/", json={"name": "Delete Item", "price": 15.0})

    response = client.delete("/items/Delete Item")
    assert response.status_code == 200
    assert response.json() == {"message": "Item deleted"}

    # Verify it's deleted
    response = client.get("/items/Delete Item")
    assert response.status_code == 200
    assert response.json() == {"error": "Item not found"}

def test_update_item():
  # Create item first
  client.post("/items/", json={"name": "Update Item", "description":"Old Description", "price": 1.0})

  response = client.put("/items/Update Item", json={"name": "Update Item", "description": "New Description", "price": 2.0})
  assert response.status_code == 200
  assert response.json() == {"message": "Item updated"}

  #Check the value actually updated
  response = client.get("/items/Update Item")
  assert response.json() == {"name": "Update Item", "description": "New Description", "price": 2.0}
