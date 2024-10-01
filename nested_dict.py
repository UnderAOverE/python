from pymongo import MongoClient
from collections import defaultdict

# Function to replace dots in field names
def replace_dots_in_keys(d):
    if isinstance(d, dict):
        new_dict = {}
        for k, v in d.items():
            new_key = k.replace('.', '_')  # Replace dot with underscore
            new_dict[new_key] = replace_dots_in_keys(v)  # Recursively replace dots in nested dictionaries
        return new_dict
    elif isinstance(d, list):
        return [replace_dots_in_keys(item) for item in d]
    else:
        return d

# Sample data with both 'status' and 'spec'
def combine_dicts(data):
    combined_dict = defaultdict(dict)
    
    for item in data:
        cluster_project_key = (item["cluster"], item["project"])
        
        # Combine the 'name', 'status', and 'spec' fields
        combined_dict[cluster_project_key][item["name"]] = {
            "status": replace_dots_in_keys(item["status"]),  # Replace dots in status field
            "spec": replace_dots_in_keys(item.get("spec", {}))  # Replace dots in spec field
        }
    
    return dict(combined_dict)

# Example data
data = [
    {
        "cluster": "namgcbswd20d",
        "project": "gcg-xdo-cust-ppccustomer360-d3-153308",
        "name": "compute-resources",
        "status": {
            "hard": {
                "limits.cpu": "12500m",
                "limits.memory": "80Gi",
                "pods": "10",
                "requests.cpu": "12500m",
                "requests.memory": "80Gi"
            },
            "used": {
                "limits.cpu": "0",
                "limits.memory": "0",
                "pods": "0",
                "requests.cpu": "0",
                "requests.memory": "0"
            }
        },
        "spec": {
            "scopes": ["NotTerminating"]
        }
    },
    {
        "cluster": "namgcbswd20d",
        "project": "gcg-xdo-cust-ppccustomer360-d3-153308",
        "name": "storage-quota",
        "status": {
            "hard": {
                "sc-ontap-nas.storageclass.storage.k8s.io/requests.storage": "50Gi",
                "thin.storageclass.storage.k8s.io/requests.storage": "20Gi"
            },
            "used": {
                "sc-ontap-nas.storageclass.storage.k8s.io/requests.storage": "10Gi",
                "thin.storageclass.storage.k8s.io/requests.storage": "5Gi"
            }
        }
    }
]

# Combine the dictionaries
combined_data = combine_dicts(data)

# MongoDB connection setup
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string

# Select the database and collection
db = client["my_database"]  # Replace "my_database" with your actual database name
collection = db["my_collection"]  # Replace "my_collection" with your actual collection name

# Insert the combined data into MongoDB
for (cluster, project), details in combined_data.items():
    document = {
        "cluster": cluster,
        "project": project,
        "details": details  # This will now contain both 'status' and 'spec' info with dots replaced
    }
    collection.insert_one(document)

print("Data inserted successfully!")