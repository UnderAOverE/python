# my_project/app/models.py
from pydantic import BaseModel, Field
from typing import Optional
from bson import ObjectId  # Import ObjectId

class Item(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")  # Optional ID
    name: str
    description: Optional[str] = None
    price: float
    is_offer: Optional[bool] = None

    class Config:
        allow_population_by_field_name = True # Allow alias for _id
        json_encoders = {
            ObjectId: str  # Properly serialize ObjectId to string
        }

# my_project/app/database.py
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from bson import ObjectId  # Import ObjectId

# Load environment variables (if you're using them)
from dotenv import load_dotenv
import os

load_dotenv()  # Load .env file

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")  # Default URI

client = MongoClient(MONGODB_URI)
database: Database = client.get_database("mydatabase")  # Change "mydatabase"
items_collection: Collection = database.get_collection("items")

def get_item(item_id: str):
    item = items_collection.find_one({"_id": ObjectId(item_id)})
    return item
def list_items():
    items = list(items_collection.find())
    return items

# my_project/app/routes.py
from fastapi import FastAPI, Depends, HTTPException
from typing import List
from . import database
from .models import Item

app = FastAPI()

@app.get("/items", response_model=List[Item])
async def read_items():
    items = database.list_items()
    return items

@app.get("/items/{item_id}", response_model=Item)
async def read_item(item_id: str):
    item = database.get_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return Item(**item)

# my_project/tests/conftest.py
import pytest
from mongomock import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from fastapi.testclient import TestClient
from app.main import app  # Import your FastAPI app
from app import database  # Import your database module
from bson import ObjectId
from typing import Generator

@pytest.fixture
def mock_mongodb_client() -> Generator:
    """A fixture that provides a mocked MongoDB client."""
    mock_client = MongoClient()
    database.client = mock_client
    yield mock_client
    database.client = MongoClient()

@pytest.fixture
def mock_mongodb(mock_mongodb_client) -> Generator:
    """A fixture that provides a mocked MongoDB database."""
    mock_db: Database = mock_mongodb_client.get_database("test_db")
    database.database = mock_db
    yield mock_db
    database.database = mock_mongodb_client.get_database("mydatabase")

@pytest.fixture
def mock_items_collection(mock_mongodb) -> Generator:
    """A fixture that provides a mocked MongoDB collection for items."""
    mock_collection: Collection = mock_mongodb.get_collection("items")
    database.items_collection = mock_collection
    yield mock_collection
    database.items_collection = mock_mongodb.get_collection("items")

@pytest.fixture
def test_client() -> TestClient:
    """A fixture that provides a TestClient instance for making requests."""
    return TestClient(app)

# my_project/tests/test_routes.py
from fastapi.testclient import TestClient
from typing import List, Dict
import pytest
from bson import ObjectId
from app.models import Item
import json

def test_read_items_empty(test_client: TestClient, mock_items_collection):
    """Test when the database is empty."""
    response = test_client.get("/items")
    assert response.status_code == 200
    assert response.json() == []

def test_read_items_populated(test_client: TestClient, mock_items_collection):
    """Test when the database has items."""
    item1 = {"name": "Item 1", "price": 10.0}
    item2 = {"name": "Item 2", "price": 20.0}
    mock_items_collection.insert_many([item1, item2])

    response = test_client.get("/items")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 2
    assert items[0]["name"] == "Item 1"
    assert items[0]["price"] == 10.0
    assert items[1]["name"] == "Item 2"
    assert items[1]["price"] == 20.0

def test_read_item_exists(test_client: TestClient, mock_items_collection):
    """Test reading a specific item when it exists."""
    item_data = {"name": "Specific Item", "price": 15.0}
    inserted = mock_items_collection.insert_one(item_data)
    item_id = str(inserted.inserted_id)

    response = test_client.get(f"/items/{item_id}")
    assert response.status_code == 200
    item = response.json()
    assert item["name"] == "Specific Item"
    assert item["price"] == 15.0
    assert item["_id"] == item_id  # Verify ID is correct

def test_read_item_not_found(test_client: TestClient):
    """Test reading a specific item when it doesn't exist."""
    response = test_client.get("/items/64d8b53129b21425992b75f0")  # Non-existent ID
    assert response.status_code == 404
    assert response.json() == {"detail": "Item not found"}

# .env (example)
# MONGODB_URI=mongodb://localhost:27017
