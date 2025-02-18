# conftest.py
import pytest
from typing import List, Dict, Optional
from fastapi import FastAPI  # Import FastAPI
from src.api.main import app  # Import your actual app instance (NEW)

# ------------------ Simulated MongoDB ------------------
class SimulatedMongoDB:
    def __init__(self):
        self.collections: Dict[str, List[Dict]] = {}

    def get_collection(self, collection_name: str) -> List[Dict]:
        if collection_name not in self.collections:
            self.collections[collection_name] = []
        return self.collections[collection_name]

    # Simulate insert_one
    async def insert_one(self, collection_name: str, document: Dict) -> None:
        collection = self.get_collection(collection_name)
        collection.append(document)

    # Simulate find_one
    async def find_one(self, collection_name: str, query: Dict) -> Optional[Dict]:
        collection = self.get_collection(collection_name)
        for doc in collection:
            if all(doc.get(key) == value for key, value in query.items()):
                return doc
        return None

    # Simulate find
    async def find(self, collection_name: str, query: Dict = None) -> List[Dict]:
        collection = self.get_collection(collection_name)
        if query is None:
            return collection
        results = []
        for doc in collection:
            if all(doc.get(key) == value for key, value in query.items()):
                results.append(doc)
        return results

    # Simulate delete_one
    async def delete_one(self, collection_name: str, query: Dict) -> bool:
        collection = self.get_collection(collection_name)
        for i, doc in enumerate(collection):
            if all(doc.get(key) == value for key, value in query.items()):
                del collection[i]
                return True
        return False

    # Simulate update_one
    async def update_one(self, collection_name: str, query: Dict, update: Dict) -> bool:
        collection = self.get_collection(collection_name)
        for doc in collection:
            if all(doc.get(key) == value for key, value in query.items()):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                return True
        return False

# Modify simulated DB:
@pytest.fixture
def simulated_mongo_client():
    return SimulatedMongoDB()

@pytest.fixture(scope="module")
def app_with_db(simulated_mongo_client):
    app.state.mongo_client = simulated_mongo_client
    yield app
    app.state.mongo_client = None  # Reset after tests






# src/tests/test_app.py
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from typing import List, Dict, Optional
from pydantic import BaseModel
from src.api.main import app  # Import the FastAPI app

# ------------------ FastAPI App ------------------
#  Note: Do not add fastAPI app, it is going to conftest for test
class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float

@pytest.mark.asyncio
async def test_create_item(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    response = test_client.post(
        "/items/",
        json={"name": "Test Item", "description": "A test item", "price": 10.0},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "Test Item", "description": "A test item", "price": 10.0}

@pytest.mark.asyncio
async def test_read_item(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    # First, create an item
    response = test_client.post(
        "/items/",
        json={"name": "Read Item", "description": "Item to read", "price": 20.0},
    )
    response = test_client.get("/items/Read Item")
    assert response.status_code == 200
    assert response.json() == {"name": "Read Item", "description": "Item to read", "price": 20.0}

@pytest.mark.asyncio
async def test_read_item_not_found(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    response = test_client.get("/items/NonExistentItem")
    assert response.status_code == 200  # or 404
    assert response.json() == {"error": "Item not found"}

@pytest.mark.asyncio
async def test_list_items(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    response = test_client.post("/items/", json={"name": "List Item 1", "price": 5.0})
    response = test_client.post("/items/", json={"name": "List Item 2", "price": 7.5})

    response = test_client.get("/items/")
    assert response.status_code == 200
    items = response.json()
    assert isinstance(items, list)
    assert any(item["name"] == "List Item 1" for item in items)
    assert any(item["name"] == "List Item 2" for item in items)

@pytest.mark.asyncio
async def test_delete_item(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    response = test_client.post("/items/", json={"name": "Delete Item", "price": 15.0})
    response = test_client.delete("/items/Delete Item")
    assert response.status_code == 200
    assert response.json() == {"message": "Item deleted"}

    response = test_client.get("/items/Delete Item")
    assert response.status_code == 200
    assert response.json() == {"error": "Item not found"}

@pytest.mark.asyncio
async def test_update_item(app_with_db):
    test_client = TestClient(app) # need to reinitalize test client
    # Create item first
    test_client.post("/items/", json={"name": "Update Item", "description":"Old Description", "price": 1.0})

    response = test_client.put("/items/Update Item", json={"name": "Update Item", "description": "New Description", "price": 2.0})
    assert response.status_code == 200
    assert response.json() == {"message": "Item updated"}

    response = test_client.get("/items/Update Item")
    assert response.json() == {"name": "Update Item", "description": "New Description", "price": 2.0}
