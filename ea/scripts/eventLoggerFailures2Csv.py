import csv
import json
from pymongo import MongoClient
from datetime import datetime, timezone

# Step 1: Load configuration from the config file
def load_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

# Step 2: Convert epoch time to timezone-aware UTC datetime
def convert_epoch_to_datetime(epoch_time):
    return datetime.fromtimestamp(int(epoch_time) / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# Step 3: Automatically determine CSV headers based on document fields, excluding "tenantid" and "ver"
def get_csv_headers(documents):
    headers = set()
    for document in documents:
        if "events" in document and isinstance(document["events"], str):
            events = json.loads(document["events"])
            for event in events:
                headers.update(event.keys())  # Collect keys from event objects

    # Exclude "tenantid" and "ver" from headers
    headers.discard("tenantid")
    headers.discard("ver")

    return sorted(headers)  # Return headers in a sorted order for consistency

# Step 4: Write MongoDB data to a CSV file
def write_to_csv(documents, csv_file_path):
    first_batch = list(documents)
    if first_batch:
        event_headers = get_csv_headers(first_batch)
        base_headers = ["_id", "_tm"]  # Include base headers without "tenantid" or "ver"
        headers = base_headers + event_headers  # Combine base headers with event field headers

        with open(csv_file_path, mode="w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()  # Write the header row

            for doc in first_batch:
                if "events" in doc and isinstance(doc["events"], str):
                    events = json.loads(doc["events"])
                    for event in events:
                        # Apply both conditions: origin == "DISPOSITION" AND tenantid == "bnymnamuat"
                        if event.get("origin") == "DISPOSITION" and event.get("tenantid") == "bnymnamuat":
                            readable_time = convert_epoch_to_datetime(event.get("_tm", 0))

                            row = {key: event.get(key, "") for key in event_headers if key not in ["tenantid", "ver"]}
                            row["_id"] = str(doc["_id"])  # Add document _id
                            row["_tm"] = readable_time   # Add converted _tm

                            # Write the row without special processing for any fields, including withheld_policies
                            writer.writerow(row)
        print(f"Data written to {csv_file_path}")
    else:
        print("No documents found.")

# Main function to fetch data and process it
if __name__ == "__main__":
    # Load MongoDB configuration
    config = load_config('./config/eventLogger2Csv.json')

    # Extract MongoDB connection details
    mongo_config = config['mongodb']
    uri = mongo_config['uri']
    database_name = mongo_config['database']
    collection_name = mongo_config['collection']

    # Connect to MongoDB
    client = MongoClient(uri)
    db = client[database_name]
    collection = db[collection_name]

    # Fetch all documents (you can apply a broader query here if needed)
    documents = collection.find()

    # Output CSV file path
    csv_file_path = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\t2_bnym_nam_uat\\output_new.csv"

    # Write the filtered documents to a CSV file
    write_to_csv(documents, csv_file_path)

    # Close the MongoDB connection
    client.close()
