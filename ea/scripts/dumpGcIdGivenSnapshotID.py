"""

    This script is written while working on SINC-16365 to get gcid from archive_metrics when snapshot_id is provided

    DO NOT USE THIS SCRIPT SINCE THERE ARE NO INDICES CREATED ON THE FIELDS BEING SEARCHED

    Sample json
    {
        "MongoDB": {
        "uri": "mongodb://",
        "database": "mydb",
        "collection": "archive_metrics",
        "input_file": "snapshot_ids.txt",
        "output_file": "gc_ids_output.txt"
        }
    }

"""

import pymongo
from pymongo import MongoClient
import time
import json

# Read configuration from file
with open('mongoConfig/gcid2SnapshotId.json', 'r') as config_file:
    config = json.load(config_file)

# MongoDB connection setup
mongo_uri = config['MongoDB']['uri']
database_name = config['MongoDB']['database']
collection_name = config['MongoDB']['collection']
input_file = config['MongoDB']['input_file']
output_file = config['MongoDB']['output_file']

# Initialize MongoDB client
mongo_client = MongoClient(mongo_uri)
database = mongo_client[database_name]
collection = database[collection_name]

# Read snapshot_ids from file
try:
    with open(input_file, 'r') as file:
        snapshot_ids = [line.strip() for line in file]
except IOError as e:
    print(f"Failed to read snapshot IDs from file: {e}")
    raise

# Function to fetch gcId with retry mechanism
def fetchGcId(snapshot_id, retries=3, delay=2):
    for attempt in range(retries):
        try:
            query = {"snapshot_id": snapshot_id}
            document = collection.find_one(query)
            if document and "gcId" in document:
                return document["gcId"]
            else:
                return None
        except pymongo.errors.PyMongoError as e:
            print(f"Attempt {attempt + 1} failed for snapshot_id {snapshot_id}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return None

# Query MongoDB for each snapshot_id to fetch the corresponding gcId
gcIds = []
try:
    for snapshot_id in snapshot_ids:
        gcId = fetchGcId(snapshot_id)
        if gcId is not None:
            gcIds.append(gcId)
finally:
    mongo_client.close()

# Write gcIds to an output file, one per line
try:
    with open(output_file, 'w') as outfile:
        for gcId in gcIds:
            outfile.write(f"{gcId}\n")
    print(f"gcIds have been written to {output_file}")
except IOError as e:
    print(f"Failed to write gcIds to file: {e}")
