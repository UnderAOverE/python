import json
from pymongo import MongoClient

class MongoConfig:
    def __init__(self, mongo_filename="mongo.json", mongo_environment="global", mongo_uri="mongodb://localhost:27017/"):
        self.mongo_filename = mongo_filename
        self.mongo_environment = mongo_environment
        self.mongo_environment_data = {}
        self.mongo_uri = mongo_uri
        self.client = MongoClient(self.mongo_uri)
        
        try:
            # Open the JSON file
            with open(self.mongo_filename, "r") as fHandler:
                mongo_data = json.load(fHandler)

            # Access environment data
            self.mongo_environment_data = mongo_data["environment"][self.mongo_environment]
        
        except FileNotFoundError:
            print(f"Error: File '{self.mongo_filename}' not found.")
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON from the file '{self.mongo_filename}'.")
        except KeyError as e:
            print(f"Error: Missing key {e} in the JSON structure.")
        except AttributeError:
            print("Error: Invalid attribute or key accessed.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def persistence_store(self):
        """
        Returns the database and collection information from the parsed environment data.
        """
        try:
            # Extracting database names
            dbs = self.mongo_environment_data["dbs"]
            return dbs
        except KeyError:
            print("Error: 'dbs' not found in environment data.")
            return {}

    def get_all_dbs(self):
        """
        Returns a list of all database names.
        """
        try:
            dbs = self.mongo_environment_data["dbs"]
            return list(dbs.keys())  # Returns the list of database names
        except KeyError:
            print("Error: 'dbs' not found in environment data.")
            return []

    def get_all_collections(self):
        """
        Returns a dictionary with database names as keys and their collections as values.
        """
        try:
            dbs = self.mongo_environment_data["dbs"]
            collections = {db_name: list(db_info["collections"].keys()) for db_name, db_info in dbs.items()}
            return collections
        except KeyError:
            print("Error: 'dbs' or 'collections' not found in environment data.")
            return {}

    def create_indexes(self):
        """
        Creates indexes for all collections as specified in the JSON config.
        Checks if the index already exists before creating a new one.
        """
        collections = self.get_all_collections()

        for db_name, collection_names in collections.items():
            db = self.client[db_name]  # Access the database
            for collection_name in collection_names:
                collection_info = self.mongo_environment_data["dbs"][db_name]["collections"][collection_name]
                indexes = collection_info.get("indexes", [])
                
                if indexes:
                    collection = db[collection_name]
                    for index_field in indexes:
                        # Check if the index already exists
                        existing_indexes = collection.index_information()  # Get current indexes
                        if index_field not in existing_indexes:
                            print(f"Creating index on '{index_field}' for collection '{collection_name}' in database '{db_name}'.")
                            collection.create_index([(index_field, 1)])  # Create ascending index on the field
                        else:
                            print(f"Index on '{index_field}' already exists for collection '{collection_name}' in database '{db_name}'.")

    def jobs_store(self):
        """
        Returns the specific database and collection for jobs.
        """
        try:
            dbs = self.mongo_environment_data["dbs"]
            scheduler_db = dbs.get("scheduler_db")
            if scheduler_db:
                collections = scheduler_db.get("collections", {})
                jobs_collection = collections.get("jobs", {}).get("name")
                if jobs_collection:
                    return scheduler_db["name"], jobs_collection
                else:
                    print("Error: 'jobs' collection not found in 'scheduler_db'.")
            else:
                print("Error: 'scheduler_db' not found in environment data.")
            return None
        except KeyError:
            print("Error: 'dbs' or 'collections' not found in environment data.")
            return None

# Example usage:
mongo_config = MongoConfig()

# Create indexes for all collections, checking if they already exist
mongo_config.create_indexes()

# Fetch all databases
dbs = mongo_config.get_all_dbs()
print(f"Databases: {dbs}")

# Fetch all collections for each database
collections = mongo_config.get_all_collections()
print(f"Collections: {collections}")

# Fetch the specific db and collection for jobs
jobs_data = mongo_config.jobs_store()
print(f"Jobs Data: {jobs_data}")
