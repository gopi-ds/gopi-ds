import argparse
import logging
import signal
import time
from pymongo import MongoClient, errors
from datetime import datetime, timezone
from functools import wraps
from config_loader import load_and_configure

# Global flag to control graceful shutdown
shutdown_flag = False

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Producer: MongoDB unique gcId batch file creator")
parser.add_argument('--config', required=True, help="Path to the JSON5 configuration file")
args = parser.parse_args()

# Load configuration
config = load_and_configure(args.config)
mongo_config = config["MongoDB"]
app_config = config["AppSettings"]
runtime_config = config["AppRuntimeCache"]

# Retry Decorator
def retry_on_failure(delay=300):
    """Decorator to retry a function indefinitely with a specified delay on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            while not shutdown_flag:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error("Error in %s: %s. Retrying in %d seconds...", func.__name__, e, delay)
                    if shutdown_flag:
                        break
                    time.sleep(delay)
        return wrapper
    return decorator

# Signal handler for graceful shutdown
def handle_signal(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    logging.info("Received termination signal. Initiating graceful shutdown...")

# Register signal handlers
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# MongoDB Connection with Retry
@retry_on_failure()
def connect_to_mongo():
    """Attempts to connect to MongoDB with retries on failure."""
    uri = mongo_config["site_uri"]
    client = MongoClient(uri, serverSelectionTimeoutMS=mongo_config["connection_timeout"], socketTimeoutMS=mongo_config["socket_timeout"])
    client.admin.command('ping')  # Test connection
    logging.info("Connected to MongoDB successfully.")
    return client

client = connect_to_mongo()

tenant_db = client[mongo_config["tenant"]]
rpm_collection = tenant_db["retention_policy_map"]

# Determine or create the temporary and tracking collections
temp_collection_name = runtime_config.get("temp_collection") or f"temp_unique_gcid_{int(time.time())}"
temp_collection = tenant_db[temp_collection_name]
tracking_collection = tenant_db[f"{temp_collection_name}_tracking"]

@retry_on_failure()
def ensure_index_on_gcId():
    """Ensures an index on 'gcId' exists in the temporary collection."""
    indexes = temp_collection.list_indexes()
    index_exists = any(index['key'] == {'gcId': 1} for index in indexes)

    if not index_exists:
        temp_collection.create_index("gcId", unique=True)
        logging.info("Created unique index on 'gcId' for collection: %s", temp_collection_name)
    else:
        logging.info("Index on 'gcId' already exists in collection: %s", temp_collection_name)

# Call to ensure index on 'gcId' after defining `temp_collection`
ensure_index_on_gcId()

# Functions to manage last_id and script status in the tracking collection
def get_last_processed_id():
    """Fetches the last processed ID, processed_count, and status from the tracking collection."""
    record = tracking_collection.find_one({"script_name": "unique_gcid"})
    if record:
        return record.get("last_id"), record.get("processed_count", 0), record.get("status", "not_started")
    return None, 0, "not_started"

@retry_on_failure()
def update_tracking_status(last_id, processed_count, status="in_progress"):
    """Updates tracking collection only after a successful batch process."""
    try:
        tracking_collection.update_one(
            {"script_name": "unique_gcid"},
            {"$set": {
                "last_id": last_id,
                "processed_count": processed_count,
                "status": status,
                "timestamp": datetime.now(tz=timezone.utc)
            }},
            upsert=True
        )
        logging.info("Tracking updated: last_id=%s, processed_count=%d, status=%s", last_id, processed_count, status)
    except Exception as e:
        logging.error("Failed to update tracking status: %s", e)
        raise

@retry_on_failure()
def process_batch_in_mongo(last_id):
    """Processes a batch directly in MongoDB using an aggregation pipeline."""
    query_filter = {"_id": {"$gt": last_id}} if last_id else {}

    pipeline = [
        {"$match": query_filter},  # Filter documents by last processed ID
        {"$sort": {"_id": 1}},     # Ensure sequential order
        {"$limit": app_config.get("gcid_fetch_batch_size", 100000)},  # Limit batch size
        {"$project": {"_id": 1, "gcId": 1}},  # Extract required fields
        {"$merge": {
            "into": temp_collection_name,
            "on": "gcId",
            "whenMatched": "keepExisting",
            "whenNotMatched": "insert"
        }}
    ]

    rpm_collection.aggregate(pipeline)
    logging.info("Batch processed directly in MongoDB.")

    # Get the last `_id` in the processed batch
    last_processed_id = None
    for document in rpm_collection.find(query_filter).sort("_id", 1).limit(1):
        last_processed_id = document["_id"]

    return last_processed_id

def fetch_and_process_direct(last_id, processed_count):
    """Processes documents directly in MongoDB in sequential batches."""
    while not shutdown_flag:
        # Process a batch directly in MongoDB
        last_processed_id = process_batch_in_mongo(last_id)
        if not last_processed_id:
            logging.info("No more documents to process.")
            break

        last_id = last_processed_id
        processed_count += app_config.get("gcid_fetch_batch_size", 100000)
        logging.info(f"Processed batch; Total processed: {processed_count}")

        # Update tracking after each batch
        update_tracking_status(last_id, processed_count)

    if shutdown_flag:
        logging.info("Process interrupted by shutdown signal. Exiting gracefully.")
        update_tracking_status(last_id, processed_count, status="interrupted")

    return last_id, processed_count

# Main Execution
try:
    # Retrieve last processed state
    last_id, processed_count, status = get_last_processed_id()

    # Process from the last checkpoint
    if status != "complete":
        last_id, processed_count = fetch_and_process_direct(last_id, processed_count)
        update_tracking_status(last_id, processed_count, status="complete")

except Exception as e:
    logging.error(f"Unexpected error during processing: {e}")

finally:
    # Ensure MongoDB connection is closed
    try:
        client.close()
        logging.info("MongoDB connection closed.")
    except Exception as e:
        logging.error("Failed to close MongoDB connection: %s", e)
