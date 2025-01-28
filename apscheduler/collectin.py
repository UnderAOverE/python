from pymongo import MongoClient

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)

# Your environment configuration
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
                                "job_name",
                                "expires_at"
                            ]
                        },
                        "jobs": {
                            "name": "jobs",
                            "indexes": [
                                "job_name",
                                "status"
                            ]
                        }
                    }
                },
                "user_db": {
                    "collections": {
                        "users": {
                            "name": "users",
                            "indexes": [
                                "username",
                                "email"
                            ]
                        },
                        "roles": {
                            "name": "roles",
                            "indexes": [
                                "role_name"
                            ]
                        }
                    }
                }
            }
        }
    }
}

# Function to create indexes if not already created
def create_indexes():
    for db_name, db_info in config["environment"]["global"]["dbs"].items():
        db = client[db_name]
        for collection_name, collection_info in db_info["collections"].items():
            collection = db[collection_name]
            for index_field in collection_info["indexes"]:
                # Check if the index exists
                index_exists = any(
                    idx["name"] == index_field for idx in collection.index_information().values()
                )
                if not index_exists:
                    # Create the index if it doesn't exist
                    print(f"Creating index on {collection_name} for {index_field}")
                    collection.create_index([(index_field, 1)])  # 1 for ascending order
                else:
                    print(f"Index on {collection_name} for {index_field} already exists.")

# Call the function to create indexes
create_indexes()
