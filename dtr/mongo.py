from pymongo import MongoClient
from datetime import datetime, timedelta
import argparse

def calculate_time_delta(time_string):
    """Calculates timedelta from input string like '1h', '2d', etc."""
    value = int(time_string[:-1])
    unit = time_string[-1]

    if unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    elif unit == 'm':
        return timedelta(minutes=value)
    else:
        raise ValueError("Invalid time unit. Use 'h', 'd', or 'm'.")

def build_aggregation_pipeline(time_delta, show_only_features=None):
    """Builds the aggregation pipeline based on time delta and feature filtering."""

    cutoff_date = datetime.utcnow() - time_delta

    pipeline = [
        {
            "$match": {
                "LogDate": { "$lte": cutoff_date }
            }
        },
        {
            "$addFields": {
                "allZero": {
                    "$and": [
                        { "$eq": ["$Total", 0] },
                        { "$eq": ["$Success", 0] },
                        { "$eq": ["$Errors", 0] },
                        { "$eq": ["$AverageResponseTime", 0] },
                        { "$eq": ["$FailurePercentage", 0] },
                        { "$eq": ["$SlowCallsCount", 0] }
                    ]
                }
            }
        },
        {
            "$match": {
                "allZero": True
            }
        },
        {
            "$group": {
                "_id": {
                    "Sector": "$Sector",
                    "Region": "$Region",
                    "Environment": "$Environment",
                    "LOB": "$LOB",
                    "Application": "$Application",
                    "SearchHead": "$SearchHead",
                    "LogDate": "$LogDate",  # Group by LogDate to avoid duplicates
                    "Feature": "$Feature"
                },
                count: { $sum: 1 } # Count the documents
            }
        },
        {
            "$group": {
                "_id": {
                    "Sector": "$_id.Sector",
                    "Region": "$_id.Region",
                    "Environment": "$_id.Environment",
                    "LOB": "$_id.LOB",
                    "Application": "$_id.Application",
                    "SearchHead": "$_id.SearchHead",
                },
                "Features": {
                    "$push": {
                        "name": "$_id.Feature",
                        "NullCount": "$count"
                    }
                },
                "Total_Nulls": { "$sum": 1 }  # Counting distinct LogDate's (null occurrences)
            }
        },
        {
            "$project": {
                "_id": 0,
                "Sector": "$_id.Sector",
                "Region": "$_id.Region",
                "Environment": "$_id.Environment",
                "LOB": "$_id.LOB",
                "Application": "$_id.Application",
                "SearchHead": "$_id.SearchHead",
                "Total_Nulls": "$Total_Nulls",
                "Features": 1
            }
        }
    ]


    # Apply feature filtering if specified
    if show_only_features:
        pipeline[4]["$group"]["Features"] = { # Modify group stage
                "$push": {
                    "$cond": [
                        {"$in": ["$_id.Feature", show_only_features]},
                        {"name": "$_id.Feature", "NullCount": "$count"},
                        "$$REMOVE"
                    ]
                }
            }

        #Remove features that are empty
        pipeline.append({
                "$unwind": {
                    "path": "$Features",
                    "preserveNullAndEmptyArrays": False  # This will remove if empty
                }
            })

        pipeline.append({
            "$group": {
                "_id": {
                    "Sector": "$_id.Sector",
                    "Region": "$_id.Region",
                    "Environment": "$_id.Environment",
                    "LOB": "$_id.LOB",
                    "Application": "$_id.Application",
                    "SearchHead": "$_id.SearchHead",
                },
                "Features": {
                    "$push": "$Features"
                },
                "Total_Nulls": { "$first": "$Total_Nulls" }
            }
        })



    return pipeline


def main():
    parser = argparse.ArgumentParser(description="Process MongoDB data for null occurrences.")
    parser.add_argument("time_argument", help="Time range to consider (e.g., 1h, 2d).")
    parser.add_argument("--no-bt-names", action="store_true", help="Do not show individual feature names in the output.")
    parser.add_argument("--show-only", help="Comma-separated list of features to show (e.g., Feature1,Feature2).")

    args = parser.parse_args()

    try:
        time_delta = calculate_time_delta(args.time_argument)
    except ValueError as e:
        print(f"Error: {e}")
        return

    show_only_features = None
    if args.show_only:
        show_only_features = [feature.strip() for feature in args.show_only.split(",")]


    # MongoDB connection details (replace with your actual credentials)
    mongo_uri = "mongodb://localhost:27017/"
    database_name = "your_database"
    collection_name = "your_collection"

    client = MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Build and execute the aggregation pipeline
    pipeline = build_aggregation_pipeline(time_delta, show_only_features)
    results = list(collection.aggregate(pipeline))  # Convert cursor to list

    # Prepare the final output
    for result in results:
        if args.no_bt_names:
            del result["Features"]  # Remove features array if --no-bt-names is set

    import json
    print(json.dumps(results, indent=2, default=str))  # Output results as JSON


if __name__ == "__main__":
    main()
