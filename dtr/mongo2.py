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

def process_data(data, show_only_features=None, no_bt_names=False):
    """Processes the data after fetching from MongoDB using Python."""
    results = {}

    for doc in data:
        key = (
            doc["Sector"],
            doc["Region"],
            doc["Environment"],
            doc["LOB"],
            doc["Application"],
            doc["SearchHead"],
        )

        if key not in results:
            results[key] = {
                "Sector": doc["Sector"],
                "Region": doc["Region"],
                "Environment": doc["Environment"],
                "LOB": doc["LOB"],
                "Application": doc["Application"],
                "SearchHead": doc["SearchHead"],
                "Total_Nulls": 0,
                "Features": {},  # Use a dictionary to store feature counts
            }

        # Check for all zero values
        all_zero = all(
            [
                doc["Total"] == 0,
                doc["Success"] == 0,
                doc["Errors"] == 0,
                doc["AverageResponseTime"] == 0,
                doc["FailurePercentage"] == 0,
                doc["SlowCallsCount"] == 0,
            ]
        )

        if all_zero:
            logdate_key = doc["LogDate"].isoformat()  # Use ISO format for LogDate
            feature = doc["Feature"]

            if feature not in results[key]["Features"]:
                results[key]["Features"][feature] = set()  # Set to store LogDates

            # Add LogDate to the set only once per feature
            if logdate_key not in results[key]["Features"][feature]:
                results[key]["Features"][feature].add(logdate_key)
                results[key]["Total_Nulls"] += 1  # Increment Total_Nulls per unique LogDate

    # Format output and apply filtering
    output = []
    for key, value in results.items():
        # Format features list
        features_list = []
        for feature, logdates in value["Features"].items():
            feature_data = {"name": feature, "NullCount": len(logdates)}

            if (show_only_features is None) or (feature in show_only_features):
                features_list.append(feature_data)

        if no_bt_names:
            value["Features"] = []  # Reset it.
        else:
            value["Features"] = features_list # re-set features array

        output.append(value)

    return output


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

    # Calculate cutoff date
    cutoff_date = datetime.utcnow() - time_delta

    # Fetch data using only the LogDate filter
    data = list(collection.find({"LogDate": {"$lte": cutoff_date}}))

    # Process data using Python
    results = process_data(data, show_only_features, args.no_bt_names)

    import json
    print(json.dumps(results, indent=2, default=str))  # Output results as JSON


if __name__ == "__main__":
    main()
