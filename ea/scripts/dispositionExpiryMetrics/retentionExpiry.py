import argparse
import json5
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
from pymongo import MongoClient

# Parse command-line arguments
parser = argparse.ArgumentParser(description="MongoDB expired documents counter")
parser.add_argument('--config', required=True, help="Path to the JSON5 configuration file")
args = parser.parse_args()

# Load the configuration from the specified config file path
try:
    with open(args.config, 'r') as config_file:
        config = json5.load(config_file)
except Exception as e:
    print("Failed to load configuration file:", e)
    raise

# Extract MongoDB and Logging configuration details
mongo_config = config["MongoDB"]
logging_config = config["Logging"]
db_settings = config.get("DatabaseSettings", {})

shared_uri = mongo_config["shared_uri"]
site_uri = mongo_config["site_uri"]
tenant = mongo_config["tenant"]
connection_timeout = mongo_config["connection_timeout"]
socket_timeout = mongo_config["socket_timeout"]
execution_mode = db_settings.get("execution_mode", "batch").lower()
MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000

# Setup logging based on configuration
log_file = logging_config["log_file"]
log_level = logging_config["log_level"].upper()

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logging.info("Logging configured successfully with level: %s and file: %s", log_level, log_file)

# Create a MongoDB client with specified timeouts
client = MongoClient(
    site_uri,
    serverSelectionTimeoutMS=connection_timeout,
    socketTimeoutMS=socket_timeout
)

def format_time(seconds):
    """Formats seconds into hours, minutes, and seconds."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"


def get_filter(cluster_id, retention_policy, effective_instant):
    """Constructs a MongoDB filter document based on the retention policy and effective instant."""
    no_of_units = retention_policy['retentionPeriod']
    unit_of_period = retention_policy['unitOfPeriod']
    retention_duration = MILLISECONDS_PER_DAY * no_of_units if unit_of_period.lower() == "day" else get_duration_in_millis(unit_of_period, no_of_units)

    start_date_mark = effective_instant.timestamp() * 1000 - retention_duration
    start_date = datetime.fromtimestamp(start_date_mark / 1000, tz=timezone.utc)

    return {
        "clusterId": cluster_id,
        "policyId": retention_policy['policyId'],
        "startDate": {"$lt": start_date}
    }


def get_duration_in_millis(unit_of_period, no_of_units):
    """Calculates retention duration in milliseconds based on unit of period and number of units."""
    now = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    if unit_of_period.lower() == "month":
        month = (now.month - no_of_units) % 12 or 12
        year = now.year - ((now.month - no_of_units - 1) // 12 + 1)
        now = now.replace(year=year, month=month)
    elif unit_of_period.lower() == "year":
        now = now.replace(year=now.year - no_of_units)

    return (datetime.now(tz=timezone.utc).timestamp() * 1000) - (now.timestamp() * 1000)


def count_expired_documents_batch(cluster_id, retention_policy, batch_size=100000):
    """Efficiently counts expired documents for a specific cluster and retention policy using range-based pagination."""
    filter_query = get_filter(cluster_id, retention_policy, datetime.now(tz=timezone.utc))
    expired_count = 0
    last_id = None  # Starting point for range-based pagination

    logging.info("Starting batch processing for Cluster %s, Policy %s",
                 cluster_id, retention_policy.get("policyId"))

    projection = {"_id": 1}  # Only fetch `_id` field for pagination
    start_time = time.time()  # Start timer for the total processing of this cluster-policy combination

    while True:
        # Add `_id` range condition if `last_id` is set
        if last_id:
            filter_query["_id"] = {"$gt": last_id}

        # Fetch batch using the modified filter query
        batch = list(tenant_rpm_coll.find(filter_query, projection).sort("_id", 1).limit(batch_size))
        batch_count = len(batch)

        # Break if there are no more documents
        if batch_count == 0:
            break

        # Update the count and set `last_id` for the next batch
        expired_count += batch_count
        last_id = batch[-1]["_id"]

        # Log batch processing count
        logging.info("Processed batch with %d documents for Cluster %s, Policy %s",
                     batch_count, cluster_id, retention_policy.get("policyId"))

    total_time_elapsed = time.time() - start_time
    logging.info("Total expired documents for Cluster %s and Retention Policy %s: %d in %s",
                 cluster_id, retention_policy.get("policyId"), expired_count, format_time(total_time_elapsed))
    return expired_count


def process_documents_in_batches_for_policies(cluster_id, policy_list, batch_size=10000):
    """Processes documents in batches for multiple policy IDs within each batch."""
    filter_query = {"clusterId": cluster_id}  # Basic filter for the cluster ID
    projection = {"_id": 1, "policyId": 1, "startDate": 1}  # Retrieve only relevant fields
    last_id = None  # For pagination

    # Precompute expiration thresholds for each policy to avoid recalculating in each document
    policy_expiry_filters = {
        policy["policyId"]: get_filter(cluster_id, policy, datetime.now(tz=timezone.utc))["startDate"]["$lt"]
        for policy in policy_list
    }

    # Create dictionary to store counts per policyId
    policy_counts = {policy['policyId']: 0 for policy in policy_list}
    total_start_time = time.time()

    while True:
        # If last_id is set, add it to filter for pagination
        if last_id:
            filter_query["_id"] = {"$gt": last_id}

        # Fetch a batch of documents
        batch = list(tenant_rpm_coll.find(filter_query, projection).sort("_id", 1).limit(batch_size))
        if not batch:
            break  # Exit if no more documents in the batch

        # Update last_id for the next batch
        last_id = batch[-1]["_id"]

        # Process each document in the batch for all policyId filters
        for document in batch:
            policy_id = document.get("policyId")
            if policy_id in policy_expiry_filters:
                # Check expiration condition
                if document.get("startDate") < policy_expiry_filters[policy_id]:
                    policy_counts[policy_id] += 1

    # Log the counts for each policy
    for policy_id, count in policy_counts.items():
        logging.info("Expired documents count for Cluster %s and Policy %s: %d",
                     cluster_id, policy_id, count)

    # Log total elapsed time
    total_elapsed_time = time.time() - total_start_time
    logging.info("Total elapsed time for processing Cluster %s: %s",
                 cluster_id, format_time(total_elapsed_time))
    return policy_counts


def count_expired_documents_aggregation(cluster_id, retention_policy, allow_disk_use=False):
    filter_query = get_filter(cluster_id, retention_policy, datetime.now(tz=timezone.utc))
    aggregation_pipeline = [
        {"$match": filter_query},
        {"$group": {"_id": None, "expiredCount": {"$sum": 1}}}
    ]

    # Start timing the aggregation process
    start_time = time.time()

    # Run the aggregation pipeline with optional disk use
    result = tenant_rpm_coll.aggregate(aggregation_pipeline, allowDiskUse=allow_disk_use)
    expired_count = next(result, {}).get("expiredCount", 0)

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    formatted_time = format_time(elapsed_time)  # Convert to hours, minutes, seconds format

    logging.info("Expired documents count for Cluster %s and Policy %s: %d (Elapsed Time: %s)",
                 cluster_id, retention_policy.get("policyId"), expired_count, formatted_time)
    return expired_count


def count_expired_documents(cluster_id, retention_policy):
    """Counts expired documents using the configured execution mode."""
    if execution_mode == "aggregation":
        return count_expired_documents_aggregation(cluster_id, retention_policy)
    else:
        return count_expired_documents_batch(cluster_id, retention_policy)


def count_expired_documents_for_all_policies(cluster_id, policy_list, allow_disk_use=False):
    # Create filter for the cluster
    filter_query = {"clusterId": cluster_id}

    # Build a dictionary to store policy expiration filters
    expiration_filters = {
        policy['policyId']: get_filter(cluster_id, policy, datetime.now(tz=timezone.utc))['startDate']['$lt']
        for policy in policy_list
    }

    # Build a $match filter to include expiration checks for each policyId
    match_conditions = {
        "$or": [
            {"policyId": policy_id, "startDate": {"$lt": expiration_date}}
            for policy_id, expiration_date in expiration_filters.items()
        ]
    }

    # Define aggregation pipeline
    aggregation_pipeline = [
        {"$match": {"$and": [filter_query, match_conditions]}},
        {"$group": {"_id": "$policyId", "expiredCount": {"$sum": 1}}}
    ]

    # Start timing the aggregation process
    start_time = time.time()

    # Run the aggregation pipeline
    result = tenant_rpm_coll.aggregate(aggregation_pipeline, allowDiskUse=allow_disk_use)

    # Collect results
    policy_counts = {doc["_id"]: doc["expiredCount"] for doc in result}

    # Log the counts for each policy
    for policy_id, count in policy_counts.items():
        logging.info("Expired documents count for Cluster %s and Policy %s: %d",
                     cluster_id, policy_id, count)

    # Log total elapsed time
    elapsed_time = time.time() - start_time
    logging.info("Total elapsed time for processing Cluster %s: %s", cluster_id, format_time(elapsed_time))
    return policy_counts


try:
    # Start timer for the entire script execution
    total_start_time = time.time()

    # Access necessary collections
    alcatraz_db = client["alcatraz"]
    tenant_db = client[tenant]
    alcatraz_tenancy_coll = alcatraz_db["tenancy"]
    tenant_cluster_coll = tenant_db['cluster']
    tenant_rpoll_coll = tenant_db['retention_policies']
    tenant_rpm_coll = tenant_db['retention_policy_map']

    logging.info("Connected to MongoDB and accessed the necessary collections.")

    # Query tenancy collection to confirm active status
    alcatraz_tenancy_query = {"_id": tenant, "status": "active"}
    result = alcatraz_tenancy_coll.find_one(alcatraz_tenancy_query, {"tenantuuid": 1, "_id": 0})

    if result and "tenantuuid" in result:
        tenantuuid = result["tenantuuid"]
        logging.info("Fetched tenantuuid: %s", tenantuuid)

        # Retrieve clusters and retention policies
        cluster_ids_list = [doc["_id"] for doc in tenant_cluster_coll.find({}, {"_id": 1})]
        retention_policies = list(tenant_rpoll_coll.find({}, {
            "policyId": 1, "retentionPeriod": 1, "unitOfPeriod": 1, "_id": 0
        }))

        # Process expired documents for all policies in each cluster
        results = []
        for cluster_id in cluster_ids_list:
            # Call count_expired_documents_for_all_policies once per cluster
            policy_counts = count_expired_documents_for_all_policies(cluster_id, retention_policies)

            # Collect results for each policy in the cluster
            for policy_id, expired_count in policy_counts.items():
                policy_name = next((policy["name"] for policy in retention_policies if policy["policyId"] == policy_id), "")
                results.append({
                    "Cluster ID": cluster_id,
                    "Policy ID": policy_id,
                    "Policy Name": policy_name,
                    "Expired Count": expired_count
                })

        # Display the results in a tabular format using pandas
        results_df = pd.DataFrame(results)
        print("\nExpired Documents Count by Cluster and Policy:")
        print(results_df)

    else:
        logging.warning("No matching document found with status 'active' for tenant: %s", tenant)

    # Log total execution time
    total_elapsed_time = time.time() - total_start_time
    logging.info("Total execution time: %s", format_time(total_elapsed_time))

except Exception as e:
    logging.error("An error occurred during database operations: %s", e)

finally:
    client.close()
    logging.info("MongoDB connection closed.")