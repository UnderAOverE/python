update_document = [
    {
        "$set": {
            "update_details": {
                "$cond": [
                    {"$eq": ["$update_details", None]},  # Check if update_details is None
                    [],  # If it's None, initialize it as an empty array
                    "$update_details"  # If it's not None, keep its existing value
                ]
            }
        }
    },
    {
        "$push": {
            "update_details": {
                "message": "New detail"
            }
        }
    }
]

result = collection.update_one(
    {"_id": 123},
    update_document
)
