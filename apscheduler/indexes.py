from pymongo import MongoClient
from pymongo.errors import OperationFailure, ConfigurationError

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)

# Updated environment configuration with sort order and index properties
config = {
    "_comment": "this is comment",
    "environment": {
        "global": {
            "dbs": {
                "scheduler_db": {
                    "collections": {
                        "locks": {
                            "name": "locks",
                            "indexes": [
                                {"field": "job_name", "unique": True, "order": "ascending"},
                                {"field": "expires_at", "ttl": 3600, "order": "descending"}
                            ]
                        },
                        "jobs": {
                            "name": "jobs",
                            "indexes": [
                                {"field": "job_name", "unique": True, "sparse": True, "order": "ascending"},
                                {"field": "status", "order": "ascending"}
                            ]
                        }
                    }
                },
                "user_db": {
                    "collections": {
                        "users": {
                            "name": "users",
                            "indexes": [
                                {"field": "username", "unique": True, "order": "ascending"},
                                {"field": "email", "sparse": True, "order": "descending"}
                            ]
                        },
                        "roles": {
                            "name": "roles",
                            "indexes": [
                                {"field": "role_name", "unique": True, "sparse": False, "order": "ascending"}
                            ]
                        }
                    }
                }
            }
        }
    }
}

SORT_ORDER_MAPPING = {"ascending": 1, "descending": -1}

def create_indexes():
    for db_name, db_info in config["environment"]["global"]["dbs"].items():
        db = client[db_name]
        for collection_name, collection_info in db_info["collections"].items():
            collection = db[collection_name]
            for index_info in collection_info["indexes"]:
                field_name = index_info["field"]
                sort_order = SORT_ORDER_MAPPING.get(index_info.get("order", "ascending"), 1)

                index_properties = {
                    "unique": index_info.get("unique", False),
                    "sparse": index_info.get("sparse", False),
                    "expireAfterSeconds": index_info.get("ttl")  # MongoDB uses `expireAfterSeconds` for TTL indexes
                }

                # Filter out None properties to avoid setting invalid parameters
                index_options = {k: v for k, v in index_properties.items() if v is not None}

                # Check if an index already exists on the field with matching properties
                existing_indexes = collection.index_information()
                index_exists = any(
                    idx["key"] == [(field_name, sort_order)] for idx in existing_indexes.values()
                )

                if not index_exists:
                    print(f"Creating index on {collection_name} for {field_name} with options {index_options} and sort order {sort_order}")
                    try:
                        collection.create_index([(field_name, sort_order)], **index_options)
                    except (OperationFailure, ConfigurationError) as e:
                        print(f"Error creating index on {collection_name}: {e}")
                else:
                    print(f"Index on {collection_name} for {field_name} already exists with the same sort order.")


# Call the function to create indexes
create_indexes()
