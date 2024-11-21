import argparse
import logging
import signal
import time
from pymongo import MongoClient, errors, UpdateOne, WriteConcern
from datetime import datetime, timezone
from functools import wraps
from config_loader import load_and_configure

# Global flag to control graceful shutdown
shutdown_flag = False

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Case holds evaluation with aggregation pipeline")
parser.add_argument('--config', required=True, help="Path to the JSON5 configuration file")
args = parser.parse_args()

# Load configuration
try:
    config = load_and_configure(args.config)
    mongo_config = config["MongoDB"]
    app_settings = config["AppSettings"]
    runtime_config = config["AppRuntimeCache"]
except Exception as e:
    logging.error("Failed to load configuration: %s", e)
    raise

# Retry Decorator
def retry_on_failure(delay=300):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            while not shutdown_flag:
                try:
                    return func(*args, **kwargs)
                except errors.PyMongoError as e:
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
def connect_to_mongo():
    while not shutdown_flag:
        try:
            uri = mongo_config["site_uri"]
            client = MongoClient(
                uri,
                readPreference="secondary",  # Read from secondary
                serverSelectionTimeoutMS=mongo_config["connection_timeout"],
                socketTimeoutMS=mongo_config["socket_timeout"]
            )
            client.admin.command('ping')  # Test connection
            logging.info("Connected to MongoDB successfully.")
            return client
        except errors.ConnectionFailure as e:
            if shutdown_flag:
                break
            logging.error("MongoDB connection failed: %s. Retrying in 5 minutes...", e)
            time.sleep(300)
    raise SystemExit("Exiting due to shutdown signal.")
logging.info("Setting up MongoDB connection...")
client = connect_to_mongo()

# Access Collections
tenant_db = client[mongo_config["tenant"]]
case_collection = tenant_db["case_items"]
temp_collection_name = runtime_config.get("temp_collection")
temp_collection = tenant_db.get_collection(
    temp_collection_name,
    write_concern=WriteConcern(w=1)
)
tracking_collection = tenant_db[f"{temp_collection_name}_tracking"]

case_fetch_batch_size = app_settings.get("case_fetch_batch_size", 10000)

@retry_on_failure()
def ensure_index_on_gcid():
    indexes = temp_collection.list_indexes()
    index_exists = any(index['key'] == {'gcId': 1} for index in indexes)
    if not index_exists:
        temp_collection.create_index("gcId", unique=True)
        logging.info("Created unique index on 'gcId' for collection: %s", temp_collection_name)
    else:
        logging.info("Index on 'gcId' already exists in collection: %s", temp_collection_name)
ensure_index_on_gcid()

# Functions to manage last_id and script status in the tracking collection
def get_tracking_status():
    record = tracking_collection.find_one({"script_name": "case_hold_evaluation"})
    if record:
        return record.get("last_id"), record.get("processed_count", 0), record.get("status", "not_started")
    return None, 0, "not_started"

@retry_on_failure()
def update_tracking_status(last_id, processed_count, status="in_progress"):
    tracking_collection.update_one(
        {"script_name": "case_hold_evaluation"},
        {"$set": {
            "last_id": last_id,
            "processed_count": processed_count,
            "status": status,
            "timestamp": datetime.now(tz=timezone.utc)
        }},
        upsert=True
    )
    logging.info("Tracking updated: last_id=%s, processed_count=%d, status=%s", last_id, processed_count, status)

@retry_on_failure()
def aggregate_hold_flags():
    pipeline = [
        # Stage 1: Group by gcid and check if any holdFlag is True
        {
            "$group": {
                "_id": "$documentKey.gcid",
                "hasHoldFlagTrue": {
                    "$max": { "$cond": [{ "$eq": ["$holdFlag", True] }, 1, 0] }
                }
            }
        },
        # Stage 2: Determine case_hold based on presence of holdFlagTrue
        {
            "$project": {
                "gcid": "$_id",
                "case_hold": {
                    "$cond": {
                        "if": { "$eq": ["$hasHoldFlagTrue", 1] },
                        "then": "Y",
                        "else": "N"
                    }
                },
                "_id": 0
            }
        }
    ]
    return case_collection.aggregate(pipeline, allowDiskUse=True)


@retry_on_failure()
def process_aggregated_results(aggregated_results, processed_count):
    bulk_updates = []
    for result in aggregated_results:
        gc_id = result["gcid"]
        case_hold = result["case_hold"]

        # Add bulk update operation
        bulk_updates.append(
            UpdateOne(
                {"gcId": gc_id},
                {"$set": {"gcId": gc_id, "case_hold": case_hold}},
                upsert=True
            )
        )

        # Execute batch if limit is reached
        if len(bulk_updates) >= case_fetch_batch_size:
            execute_bulk_update(bulk_updates)
            processed_count += len(bulk_updates)
            bulk_updates = []  # Reset after execution
            update_tracking_status(None, processed_count)  # Update tracking after each bulk execution

        if shutdown_flag:
            logging.info("Shutdown flag detected. Exiting batch processing.")
            break

    # Final batch execution regardless of shutdown_flag
    if bulk_updates:
        execute_bulk_update(bulk_updates)
        processed_count += len(bulk_updates)
        bulk_updates = []
        update_tracking_status(None, processed_count)  # Ensure tracking is updated for the final batch


@retry_on_failure()
def execute_bulk_update(bulk_updates):
    try:
        # Execute the bulk operation with all UpdateOne operations in bulk_updates
        if bulk_updates:  # Only perform write if there are updates
            result = temp_collection.bulk_write(bulk_updates)
            logging.info(
                f"Bulk upsert completed: {result.matched_count} matched, "
                f"{result.modified_count} modified, {result.upserted_count} upserted."
            )
    except errors.BulkWriteError as e:
        logging.error("Bulk write error: %s", e.details)
        raise


# Main execution function
def aggregate_and_process():
    last_id, processed_count, status = get_tracking_status()
    if status != "complete":
        logging.info("Starting aggregation and processing of case holds.")
        # Using 'with' to ensure the cursor is closed after processing
        with aggregate_hold_flags() as aggregated_results:
            process_aggregated_results(aggregated_results, processed_count)

        # Mark final status as complete if not interrupted
        if not shutdown_flag:
            _, final_processed_count, _ = get_tracking_status()
            update_tracking_status(last_id, final_processed_count, status="complete")
        else:
            logging.info("Processing interrupted by shutdown. Tracking status set to 'interrupted'.")

try:
    # Reset tracking status at the start
    update_tracking_status(None, 0)

    aggregate_and_process()
except Exception as e:
    logging.error(f"Unexpected error during processing: {e}")
finally:
    try:
        client.close()
        logging.info("MongoDB connection closed.")
    except Exception as e:
        logging.error("Failed to close MongoDB connection: %s", e)
