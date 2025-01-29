import uuid
import time
import threading

worker_id = str(uuid.uuid4())

def register_worker_in_mongo():
    """
    Periodically register or update the worker's metadata in MongoDB.
    """
    while True:
        db = client["monitoring_db"]
        workers_collection = db["workers"]

        # Upsert worker status
        workers_collection.update_one(
            {"worker_id": worker_id},
            {
                "$set": {
                    "worker_id": worker_id,
                    "host": socket.gethostname(),
                    "status": "running",
                    "process_id": os.getpid(),
                    "last_heartbeat": datetime.utcnow()
                }
            },
            upsert=True
        )
        time.sleep(10)  # Update every 10 seconds


# Start the background registration thread
threading.Thread(target=register_worker_in_mongo, daemon=True).start()

@app.get("/all-workers")
def get_all_workers():
    """
    Get the status of all registered workers from MongoDB.
    """
    workers_collection = client["monitoring_db"]["workers"]
    workers = list(workers_collection.find({}, {"_id": 0}))
    return {"workers": workers}


---------------------

from fastapi import FastAPI, Response
from pymongo import MongoClient
import socket
import os
import psutil

app = FastAPI()

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017"
client = MongoClient(MONGO_URI)


def get_mongo_connections():
    """
    Retrieve the number of active MongoDB connections by checking socket connections.
    """
    mongo_port = 27017
    connections = 0

    # Check all active connections for MongoDB port
    for conn in psutil.net_connections(kind="inet"):
        if conn.laddr.port == mongo_port:
            connections += 1

    return connections


@app.get("/health")
def health_check():
    # Process and system details
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    cpu_usage = process.cpu_percent(interval=0.1)

    # MongoDB connections
    active_connections = get_mongo_connections()

    # Generate HTML response
    html_content = f"""
    <html>
        <head><title>Health Check</title></head>
        <body>
            <h1>Worker Health Check</h1>
            <p><strong>Worker ID:</strong> {os.getpid()}</p>
            <p><strong>Host:</strong> {socket.gethostname()}</p>
            <p><strong>Status:</strong> Running</p>
            <h2>MongoDB</h2>
            <p><strong>Active Connections:</strong> {active_connections}</p>
            <h2>System Metrics</h2>
            <p><strong>Memory Usage:</strong> {memory_info:.2f} MB</p>
            <p><strong>CPU Usage:</strong> {cpu_usage}%</p>
        </body>
    </html>
    """
    return Response(content=html_content, media_type="text/html")



from fastapi import FastAPI
import socket
import os
from pymongo import MongoClient
import psutil

app = FastAPI()
mongo_client = MongoClient("mongodb://localhost:27017/")


def get_mongo_connections():
    try:
        server_status = mongo_client.admin.command("serverStatus")
        connections = server_status["connections"]
        return {
            "current": connections["current"],
            "available": connections["available"],
            "total_created": connections["totalCreated"]
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/health")
def health_check():
    memory_info = psutil.virtual_memory()
    cpu_percent = psutil.cpu_percent(interval=1)

    return {
        "worker_id": os.getpid(),
        "host": socket.gethostname(),
        "status": "running",
        "mongo_connections": get_mongo_connections(),
        "cpu_percent": cpu_percent,
        "memory_usage": {
            "total": memory_info.total,
            "used": memory_info.used,
            "free": memory_info.free,
            "percent": memory_info.percent
        }
    }



from pymongo import MongoClient

def get_active_connections():
    client = MongoClient("mongodb://localhost:27017/?appname=my-app")
    try:
        # Run currentOp to list all current operations
        active_ops = client.admin.command("currentOp")
        
        # Filter operations with your app name
        connections = [
            op for op in active_ops["inprog"]
            if op.get("clientMetadata", {}).get("application", {}).get("name") == "my-app"
        ]
        
        return len(connections)
    except Exception as e:
        return f"Error fetching active connections: {str(e)}"

print(get_active_connections())



from fastapi import FastAPI, Response
import psutil
import os
import socket
from pymongo import MongoClient

app = FastAPI()

def get_mongo_connections():
    """Get the current number of active MongoDB connections."""
    client = MongoClient("mongodb://localhost:27017/")
    try:
        server_status = client.admin.command("serverStatus")
        return server_status["connections"]["current"]
    except Exception as e:
        return f"Error fetching connection status: {str(e)}"


@app.get("/health")
def health_check():
    # Process and system details
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    cpu_usage = process.cpu_percent(interval=0.1)

    # MongoDB connections
    active_connections = get_mongo_connections()

    # Generate HTML response in table format
    html_content = f"""
    <html>
        <head>
            <title>Health Check</title>
            <style>
                table {{
                    width: 50%;
                    border-collapse: collapse;
                    margin: 20px auto;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
            </style>
        </head>
        <body>
            <h1 style="text-align: center;">Worker Health Check</h1>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td>Worker ID</td>
                    <td>{os.getpid()}</td>
                </tr>
                <tr>
                    <td>Host</td>
                    <td>{socket.gethostname()}</td>
                </tr>
                <tr>
                    <td>Status</td>
                    <td>Running</td>
                </tr>
                <tr>
                    <td>Active MongoDB Connections</td>
                    <td>{active_connections}</td>
                </tr>
                <tr>
                    <td>Memory Usage</td>
                    <td>{memory_info:.2f} MB</td>
                </tr>
                <tr>
                    <td>CPU Usage</td>
                    <td>{cpu_usage}%</td>
                </tr>
            </table>
        </body>
    </html>
    """
    return Response(content=html_content, media_type="text/html")
