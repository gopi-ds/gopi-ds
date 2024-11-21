import argparse
import logging
import signal
import time
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, UpdateOne, errors, WriteConcern
from concurrent.futures import ThreadPoolExecutor, as_completed
from config_loader import load_and_configure
import dateutil.parser

# Global flag to control graceful shutdown
shutdown_flag = False

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Supervision policy expiry evaluator")
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

def handle_signal(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    logging.info("Received termination signal. Initiating graceful shutdown...")

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def connect_to_mongo():
    """Attempts to connect to MongoDB with indefinite retries on failure."""
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

tenant_db = client[mongo_config["tenant"]]
rpm_collection = tenant_db["retention_policy_map"]
temp_collection_name = runtime_config.get("temp_collection")
temp_collection = tenant_db.get_collection(
    temp_collection_name,
    write_concern=WriteConcern(w=1)
)
tracking_collection = tenant_db[f"{temp_collection_name}_tracking"]
rpoll_coll = tenant_db['retention_policies']
effective_instant_str = app_settings.get("EFFECTIVE_INSTANT")
gcid_fetch_batch_size = app_settings.get("gcid_fetch_batch_size", 1000000)
gcid_eval_batch_size = app_settings.get("gcid_eval_batch_size", 1000)
gcid_eval_max_threads = app_settings.get("gcid_eval_max_threads", 4)

def parse_effective_instant(effective_instant_str):
    try:
        effective_instant = dateutil.parser.isoparse(effective_instant_str)
        if effective_instant.tzinfo != timezone.utc:
            effective_instant = effective_instant.astimezone(timezone.utc)
        return effective_instant
    except (ValueError, TypeError) as e:
        logging.error(f"Failed to parse effective_instant_str: {effective_instant_str}. Error: {e}")
        raise ValueError(f"Invalid date string format: {effective_instant_str}") from e
EFFECTIVE_INSTANT = parse_effective_instant(effective_instant_str)

def retry_on_failure(func):
    def wrapper(*args, **kwargs):
        while not shutdown_flag:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error("Error in %s: %s. Retrying in 5 minutes...", func.__name__, e)
                if shutdown_flag:
                    break
                time.sleep(300)
    return wrapper

@retry_on_failure
def get_tracking_status():
    try:
        # Attempt to find the tracking record
        record = tracking_collection.find_one({"script_name": "supervision_policy_evaluation"})
        if record is None:
            # Insert a default record if none exists
            default_record = {
                "script_name": "supervision_policy_evaluation",
                "last_id": None,
                "processed_count": 0,
                "expired_count": 0,
                "active_count": 0,
                "timestamp": datetime.now(tz=timezone.utc),
                "status": "not_started"
            }
            tracking_collection.insert_one(default_record)
            logging.info("No tracking record found. Inserted default record: %s", default_record)
            return default_record["status"], default_record["last_id"], default_record["processed_count"], default_record["expired_count"], default_record["active_count"]
        # Return the retrieved record's values
        return (
            record.get("status", "not_started"),
            record.get("last_id"),
            record.get("processed_count", 0),
            record.get("expired_count", 0),
            record.get("active_count", 0)
        )
    except Exception as e:
        logging.error("Error in get_tracking_status: %s", e)
        raise

@retry_on_failure
def fetch_gc_id_batch(last_id):
    try:
        query = {"_id": {"$gt": last_id}} if last_id else {}
        return list(temp_collection.find(query).sort("_id", 1).limit(gcid_fetch_batch_size))
    except Exception as e:
        logging.error("Failed to fetch gcId batch: %s", e)
        raise

@retry_on_failure
def get_duration_in_millis(unit_of_period, no_of_units):
    try:
        effective_date = EFFECTIVE_INSTANT.replace(hour=0, minute=0, second=0, microsecond=0)
        adjusted_date = effective_date
        if unit_of_period.lower() == "month":
            month = effective_date.month - no_of_units
            year = effective_date.year
            while month <= 0:
                month += 12
                year -= 1
            adjusted_date = adjusted_date.replace(year=year, month=month)
        elif unit_of_period.lower() == "year":
            adjusted_date = adjusted_date.replace(year=effective_date.year - no_of_units)
        else:
            raise ValueError(f"Unsupported unit_of_period: {unit_of_period}")
        return int((effective_date - adjusted_date).total_seconds() * 1000)
    except Exception as e:
        logging.error("Failed to calculate duration in millis for %s %s(s): %s", no_of_units, unit_of_period, e)
        raise

@retry_on_failure
def get_filter(policy):
    try:
        no_of_units = policy['retentionPeriod']
        unit_of_period = policy['unitOfPeriod']
        if unit_of_period.lower() == "day":
            retention_duration = timedelta(days=no_of_units)
        elif unit_of_period.lower() == "month":
            retention_duration = timedelta(milliseconds=get_duration_in_millis("month", no_of_units))
        elif unit_of_period.lower() == "year":
            retention_duration = timedelta(milliseconds=get_duration_in_millis("year", no_of_units))
        else:
            raise ValueError("Unsupported unit of period")
        return EFFECTIVE_INSTANT - retention_duration
    except Exception as e:
        logging.error("Error calculating expiration date for policy: %s", e)
        raise

@retry_on_failure
def evaluate_all_policies_for_a_gcid(gc_id_policies, policy_expiry_thresholds):
    """
    Check if all policies for a gcId are expired.
    """
    try:
        for policy in gc_id_policies:
            policy_id = policy.get("policyId")
            expiration_date = policy_expiry_thresholds.get(policy_id)
            start_date = policy.get("startDate")

            # Convert to UTC if `start_date` has no timezone info
            start_date = start_date.replace(tzinfo=timezone.utc) if start_date.tzinfo is None else start_date

            # If any policy is still active, return False
            if expiration_date and start_date and start_date >= expiration_date:
                return False
        return True  # All policies are expired
    except Exception as e:
        logging.error("Error evaluating policies for gcId: %s", e)
        raise

@retry_on_failure
def evaluate_policies_for_gcids(gc_id_batch, policy_expiry_thresholds):
    """
    Evaluate policies for a batch of gcIds, identifying expired and active gcIds.
    Treat gcIds with no policies as expired.
    """
    try:
        expired_gc_ids = []
        active_gc_ids = []
        # Fetch policies for the given gcIds
        policies = list(rpm_collection.find(
            {"gcId": {"$in": gc_id_batch}},
            {"gcId": 1, "policyId": 1, "startDate": 1}
        ))
        # Group policies by gcId
        policies_by_gc_id = {}
        for policy in policies:
            gc_id = policy["gcId"]
            policies_by_gc_id.setdefault(gc_id, []).append(policy)
        # Process each gcId in the batch
        for gc_id in gc_id_batch:
            gc_id_policies = policies_by_gc_id.get(gc_id, [])
            if not gc_id_policies:
                # Treat gcIds with no policies as expired
                expired_gc_ids.append(gc_id)
                continue
            if evaluate_all_policies_for_a_gcid(gc_id_policies, policy_expiry_thresholds):
                expired_gc_ids.append(gc_id)
            else:
                active_gc_ids.append(gc_id)
        logging.info(
            "Evaluation completed: %d gcIds processed, %d expired, %d active.",
            len(gc_id_batch),
            len(expired_gc_ids),
            len(active_gc_ids)
        )
        return expired_gc_ids, active_gc_ids
    except Exception as e:
        logging.error("Error during evaluation of policies for gcIds: %s", e)
        raise

@retry_on_failure
def update_temp_collection(expired_gc_ids, active_gc_ids):
    """
    Update `ret_policies_expired` to 'Y' for expired and 'N' for active gcIds.
    """
    if not expired_gc_ids and not active_gc_ids:
        return
    try:
        updates = []
        # Update 'Y' for expired gcIds
        if expired_gc_ids:
            updates.extend([
                UpdateOne({"gcId": gc_id}, {"$set": {"sup_policies_expired": "Y"}}, upsert=True)
                for gc_id in expired_gc_ids
            ])
        # Update 'N' for active gcIds
        if active_gc_ids:
            updates.extend([
                UpdateOne({"gcId": gc_id}, {"$set": {"ret_policies_expired": "N"}}, upsert=True)
                for gc_id in active_gc_ids
            ])
        # Perform bulk write
        result = temp_collection.bulk_write(updates)
        logging.info("Bulk upsert completed: %d matched, %d modified, %d upserted.",
                     result.matched_count, result.modified_count, result.upserted_count)
    except Exception as e:
        logging.error("Failed to perform bulk upsert on temp collection: %s", e)
        raise

@retry_on_failure
def update_tracking_status(last_id, processed_count, expired_count, active_count, status="in_progress"):
    try:
        tracking_collection.update_one(
            {"script_name": "supervision_policy_evaluation"},
            {"$set": {
                "last_id": last_id,
                "processed_count": processed_count,
                "expired_count": expired_count,
                "active_count": active_count,
                "timestamp": datetime.now(tz=timezone.utc),
                "status": status
            }},
            upsert=True
        )
        logging.info(
            "Tracking status updated: last_id: %s, evaluated: %d, expired: %d, active: %d, status: %s",
            last_id, processed_count, expired_count, active_count, status
        )
    except Exception as e:
        logging.error("Failed to update tracking status: %s", e)
        raise

def evaluate_and_update_batch(gc_id_list, policy_expiry_thresholds):
    expired_gc_ids, active_gc_ids = evaluate_policies_for_gcids(gc_id_list, policy_expiry_thresholds)
    update_temp_collection(expired_gc_ids, active_gc_ids)
    return len(gc_id_list), len(expired_gc_ids), len(active_gc_ids)

def process_gcids_in_batches(policy_expiry_thresholds):

    def get_parent_script_status():
        # Function to check the status of the parent script "unique_gcid"
        parent_status_record = tracking_collection.find_one({"script_name": "unique_gcid"})
        return parent_status_record.get("status", "not_started") if parent_status_record else "not_started"

    # Retrieve tracking status
    processing_status, last_id, total_evaluated, total_expired, total_active = get_tracking_status()
    previous_last_id = last_id
    previous_total_evaluated = total_evaluated
    previous_total_expired = total_expired
    previous_total_active = total_active
    while not shutdown_flag:
        previous_last_id = last_id
        previous_total_evaluated = total_evaluated
        previous_total_expired = total_expired
        previous_total_active = total_active
        # Fetch a large batch of gcIds
        large_gc_id_batch = fetch_gc_id_batch(last_id)
        if not large_gc_id_batch and get_parent_script_status() == "complete":
            logging.info("All gcIds processed, and tracking status is 'complete'. Exiting.")
            break
        if large_gc_id_batch:
            last_id = large_gc_id_batch[-1]["_id"]
            gc_id_list = [doc["gcId"] for doc in large_gc_id_batch]
            logging.info("Fetched a large batch of %d gcIds for processing.", len(gc_id_list))
            # Batch-specific metrics
            batch_evaluated, batch_expired, batch_active = 0, 0, 0
            # Process batches using threads
            with ThreadPoolExecutor(max_workers=gcid_eval_max_threads) as executor:
                futures = []
                for i in range(0, len(gc_id_list), gcid_eval_batch_size):
                    if shutdown_flag:
                        logging.info("Terminating batch submission due to shutdown signal.")
                        break
                    gcid_eval_batch = gc_id_list[i:i + gcid_eval_batch_size]
                    futures.append(executor.submit(evaluate_and_update_batch, gcid_eval_batch, policy_expiry_thresholds))
                for completed_future in as_completed(futures):
                    try:
                        if shutdown_flag:
                            logging.info("Terminating batch processing due to shutdown signal.")
                            break
                        evaluated_count, expired_count, active_count = completed_future.result()
                        batch_evaluated += evaluated_count
                        batch_expired += expired_count
                        batch_active += active_count
                    except Exception as e:
                        logging.error("An error occurred while processing a batch: %s", e)
            # Update totals
            total_evaluated += batch_evaluated
            total_expired += batch_expired
            total_active += batch_active
            # Update tracking collection
            update_tracking_status(last_id, total_evaluated, total_expired, total_active)
            logging.info("Batch processed: Evaluated: %d, Expired: %d, Active: %d", batch_evaluated, batch_expired, batch_active)
            logging.info("Total so far: Evaluated: %d, Expired: %d, Active: %d", total_evaluated, total_expired, total_active)
        else:
            time.sleep(60)
    # Final tracking update
    if shutdown_flag:
        logging.info("Process interrupted by shutdown signal. Exiting gracefully.")
        update_tracking_status(previous_last_id, previous_total_evaluated, previous_total_expired, previous_total_active, status="interrupted")
    else:
        logging.info("Batch processing completed successfully.")
        update_tracking_status(last_id, total_evaluated, total_expired, total_active, status="complete")

try:
    logging.info("Retrieving supervision policies from MongoDB...")
    supervision_policies = list(rpoll_coll.find(
        {"sourceType": {"$eq": "supervision"}},
        {"policyId": 1, "retentionPeriod": 1, "unitOfPeriod": 1, "_id": 0}
    ))
    logging.info("Total supervision policies retrieved: %d", len(supervision_policies))
    policy_expiry_thresholds = {policy["policyId"]: get_filter(policy) for policy in supervision_policies}
    logging.info("Computed supervision policy expiration thresholds.")

    process_gcids_in_batches(policy_expiry_thresholds)

except Exception as e:
    logging.error("An error occurred during processing: %s", e)
finally:
    try:
        client.close()
        logging.info("MongoDB connection closed.")
    except Exception as e:
        logging.error("Failed to close MongoDB connection: %s", e)
