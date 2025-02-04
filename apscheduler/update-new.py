from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Any
from pymongo import MongoClient

app = FastAPI()

# MongoDB client setup
client = MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]
collection = db["mycollection"]

class Trigger(BaseModel):
    type: str
    arguments: Dict[str, Any]

class UpdateRequest(BaseModel):
    job_id: str
    trigger: Trigger

@app.put("/update-trigger")
async def update_trigger(payload: UpdateRequest, user: str = Header(...)):
    # Fetch document by job_id
    document = collection.find_one({"job_id": payload.job_id})
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    # Prepare update details
    utc_time = datetime.utcnow()
    before_trigger = document.get("trigger", {})
    after_trigger = payload.trigger.dict()

    update_detail = {
        "user": user,
        "update_time": utc_time.isoformat(),
        "details": {
            "before": before_trigger,
            "after": after_trigger
        }
    }

    # Update the document in MongoDB
    collection.update_one(
        {"job_id": payload.job_id},
        {
            "$set": {"trigger": after_trigger},
            "$push": {"update_details": update_detail}
        }
    )

    return {"message": "Trigger updated successfully", "update_detail": update_detail}