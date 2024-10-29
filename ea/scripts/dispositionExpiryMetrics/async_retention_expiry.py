import json5
import logging
from datetime import datetime, timezone
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

# Load the configuration from config.json5
try:
    with open("../config/dispositionExpiry.json", "r") as config_file:
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

# Create an async MongoDB client
client = AsyncIOMotorClient(site_uri)


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


async def count_expired_documents(cluster_id, retention_policy, collection):
    """Count expired documents asynchronously for a specific cluster and retention policy."""
    filter_query = get_filter(cluster_id, retention_policy, datetime.now(tz=timezone.utc))
    expired_count = await collection.count_documents(filter_query)
    logging.info("Expired documents count for Cluster %s and Retention Policy %s: %d",
                 cluster_id, retention_policy.get("policyId"), expired_count)
    return expired_count


async def process_clusters_and_policies():
    """Process each cluster-policy combination asynchronously."""
    # Access necessary collections asynchronously
    alcatraz_db = client["alcatraz"]
    tenant_db = client[tenant]
    alcatraz_tenancy_coll = alcatraz_db["tenancy"]
    tenant_cluster_coll = tenant_db['cluster']
    tenant_rpoll_coll = tenant_db['retention_policies']
    tenant_rpm_coll = tenant_db['retention_policy_map']

    # Confirm active status in tenancy collection
    alcatraz_tenancy_query = {"_id": tenant, "status": "active"}
    result = await alcatraz_tenancy_coll.find_one(alcatraz_tenancy_query, {"tenantuuid": 1, "_id": 0})

    if result and "tenantuuid" in result:
        tenantuuid = result["tenantuuid"]
        logging.info("Fetched tenantuuid: %s", tenantuuid)

        # Retrieve clusters and retention policies
        cluster_ids_list = [doc["_id"] async for doc in tenant_cluster_coll.find({}, {"_id": 1})]
        retention_policies = [policy async for policy in tenant_rpoll_coll.find({}, {
            "policyId": 1, "name": 1, "retentionPeriod": 1, "unitOfPeriod": 1, "_id": 0
        })]

        # Execute count_expired_documents concurrently for each cluster-policy combination
        tasks = [
            count_expired_documents(cluster_id, policy, tenant_rpm_coll)
            for cluster_id in cluster_ids_list
            for policy in retention_policies
        ]
        await asyncio.gather(*tasks)
    else:
        logging.warning("No matching document found with status 'active' for tenant: %s", tenant)


async def main():
    try:
        await process_clusters_and_policies()
    finally:
        client.close()
        logging.info("MongoDB connection closed.")


# Run the asynchronous main function
asyncio.run(main())
