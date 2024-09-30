from pymongo import MongoClient
from collections import defaultdict

# Sample data, assume this is the combined dictionary from the previous step
def combine_dicts(data):
    combined_dict = defaultdict(dict)
    
    for item in data:
        cluster_project_key = (item["cluster"], item["project"])
        combined_dict[cluster_project_key][item["name"]] = item["status"]
    
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
        "details": details  # This will contain all the 'name' and 'status' info
    }
    collection.insert_one(document)

print("Data inserted successfully!")