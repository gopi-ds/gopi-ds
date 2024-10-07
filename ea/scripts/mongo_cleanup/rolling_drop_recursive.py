# Summary
#       - Configuration: The script reads from a JSON configuration file and
#                       initializes MongoDB connections, log settings, and other options.
#       - Sharded and Unsharded Collections: Collections are categorized as either sharded
#                       or unsharded, and different drop logic is applied accordingly.
#       - Dry Run: If the dry_run option is enabled,
#                   operations are simulated, and no actual data is modified.
#       - Concurrent and Sequential Deletions: For sharded collections,
#                   deletions can occur either concurrently or sequentially,
#                   depending on the configuration.
#       - Write Concern: MongoDB operations are performed with a specified writeConcern,
#                   ensuring proper replication and journaling behavior.
#       - Summary Logging: A summary of the operations (success or failure) is logged at the end.

# Sample config format
#   {
#       "mongo_uri": "mongodb://admin@localhost:37018",
#       "db_name": "testDatabase",
#       "config_db_name": "config",
#       "log_file": "C:\\rolling_drop.log",
#       "log_level": "INFO",
#       "dry_run": true,
#       "deletes_per_batch": 10000,
#       "concurrent_deletion_enabled": true,
#       "writeConcern": {"w": "majority", "j": true, "wtimeout": 60000}
#   }


import datetime
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tabulate import tabulate
from setup import initialize_logging, initialize_mongo_connection


# Function to read configuration from a JSON file
# This function loads the configuration file containing MongoDB settings,
# log file paths, and other options.
def read_config(config_file):
    try:
        with open(config_file, 'r') as file:
            config_data = json.load(file)
        return config_data
    except Exception as e:
        print(f"Error loading config file: {e}")
        raise

# Read configuration from the config file (provide the correct path)
config = read_config('../config/rollingDrop.json')  # Specify the path to your config file

# MongoDB connection details from the config file
mongo_uri = config['mongo_uri']
db_name = config['db_name']
config_db_name = config['config_db_name']
log_file = config['log_file']  # Read log file location from config
log_level_str = config['log_level']  # Read log level from config
DRY_RUN = config.get('dry_run', True)  # Read dry_run from config
deletes_per_batch= config.get('deletes_per_batch', 10000)  # Read batch size in number of documents
concurrent_deletion_enabled = config.get('concurrent_deletion_enabled', False)  # Toggle for concurrent execution
write_concern = config.get("writeConcern", {"w": "majority", "j": True})

# Initialize logging and MongoDB connection using setup.py functions
initialize_logging(log_file, log_level_str)
tenantdb = initialize_mongo_connection(mongo_uri, db_name)
configdb = initialize_mongo_connection(mongo_uri, config_db_name)

# Track the status of each collection (name, type, status)
collection_status = []

# Banner text to indicate the start of the program execution
def log_banner():
    logging.info("="*50)
    logging.info(f"Starting Rolling Drop Program at {datetime.datetime.now()}")
    logging.info("="*50)

# Call the banner logging function
log_banner()

# Function to get sharded and unsharded collections
# Fetches the collections from the MongoDB instance and categorizes them into sharded or unsharded collections.
def get_sharded_unsharded_collections():
    sharded_collections = []
    unsharded_collections = []
    all_collections = tenantdb.list_collection_names()

    for collection_name in all_collections:
        stats = tenantdb.command('collstats', collection_name)
        if stats.get('sharded', False):
            sharded_collections.append(collection_name)
        else:
            unsharded_collections.append(collection_name)

    return sharded_collections, unsharded_collections

# Function to log the summary of success or failure at the end
# Logs a summary table of the results for each collection processed during the rolling drop.
def log_summary():
    summary_table = tabulate(collection_status, headers=["Collection Name", "Shard", "Status"], tablefmt="grid")
    logging.info("Processing summary\n" + summary_table)

# Function to retrieve chunk information for sharded collections
# Gathers chunk details from the config server for a given collection and logs the data in a tabular format.
def all_chunk_info(ns, estimate=True):
    """
    This function gathers chunk information for a given namespace (ns)
    before the rolling drop process and logs the data in a tabular format.
    Chunk size is converted to MB for readability.
    It also logs consolidated totals for the chunk size and the number of objects.

    :param ns: Namespace in the format "database.collection"
    :param estimate: Boolean, if True the datasize command uses estimates
    :return: A dictionary mapping each shard to its chunk data
    """
    # Fetch all chunks for the specified namespace and sort by 'min'
    chunks = configdb.chunks.find({"ns": ns}).sort("min", 1)

    # Fetch the shard key only once for the namespace
    collection_info = configdb.collections.find_one({"_id": ns})
    if not collection_info:
        logging.warning(f"Could not find collection info for namespace {ns}")
        return {}

    shard_key = collection_info["key"]

    # Initialize the shard map to hold the data for each shard
    shard_map = {}

    # Initialize totals for size and object count
    total_size_mb = 0
    total_objects = 0

    # Prepare a list to store chunk information for tabulate
    chunk_info_list = []

    # Iterate over each chunk
    for chunk in chunks:
        shard = chunk['shard']
        min_key = chunk['min']
        max_key = chunk['max']

        # Run the datasize command to estimate the chunk size and number of objects
        data_size_cmd = {
            "datasize": chunk["ns"],
            "keyPattern": shard_key,
            "min": chunk["min"],
            "max": chunk["max"],
            "estimate": estimate
        }

        # Use the correct database reference here (assuming it's `db`)
        data_size_result = tenantdb.command(data_size_cmd)

        # Convert chunk size from bytes to MB
        chunk_size_mb = data_size_result['size'] / (1024 * 1024)
        document_count = data_size_result['numObjects']

        # Add chunk data to the shard map
        if shard not in shard_map:
            shard_map[shard] = []

        shard_map[shard].append({
            "chunk_id": chunk["_id"],
            "minKey": min_key,
            "maxKey": max_key,
            "size_mb": f"{chunk_size_mb:.2f} MB",
            "document_count": document_count
        })

        # Append chunk data to list for tabular display, with Shard as the first column
        chunk_info_list.append([
            shard, chunk['_id'], min_key, max_key, f"{chunk_size_mb:.2f} MB", document_count
        ])

        # Update the totals
        total_size_mb += chunk_size_mb
        total_objects += document_count

    # Log chunk information in a tabular format
    headers = ["Shard", "Chunk ID", "Min Key", "Max Key", "Chunk Size (MB)", "Objects in Chunk"]
    logging.info(f"Summary for collection {ns} \n"
                 f"\t\t\t Consolidated size: {total_size_mb:.2f} MB\n"
                 f"\t\t\t Consolidated object count: {total_objects}\n"
                 + tabulate(chunk_info_list, headers=headers, tablefmt="grid"))

    return shard_map

# Function to delete documents from a shard
# Deletes documents from chunks of a sharded collection, either concurrently or sequentially.
def delete_from_shard(shard, chunks, collection_name):
    try:
        for chunk in chunks:
            min_key = chunk['minKey']
            max_key = chunk['maxKey']
            shard_key = list(min_key.keys())[0]

            remaining_docs = chunk['document_count']

            while remaining_docs > 0:
                # Calculate the actual batch size for this iteration
                batch_size = min(deletes_per_batch, remaining_docs)

                query = {shard_key: {"$gte": min_key[shard_key], "$lt": max_key[shard_key]}}
                logging.info(f"Shard '{shard}': Executing delete query, batch size: {batch_size} documents")

                if not DRY_RUN:
                    try:
                        result = tenantdb[collection_name].delete_many(query, limit=batch_size, writeConcern=write_concern)
                        deleted_docs_count = result.deleted_count
                        logging.info(f"Shard '{shard}': Deleted {deleted_docs_count} documents")
                        # Update the remaining docs after processing this batch
                        remaining_docs -= deleted_docs_count
                    except Exception as e:
                        logging.error(f"Shard '{shard}': Error deleting documents for collection {collection_name}: {e}")
                        return False  # Return failure status
                else:
                    logging.info(f"DRY RUN: Shard '{shard}' would have executed delete query for {batch_size} documents with query: {query}")
                    # Simulate batch progress
                    remaining_docs -= batch_size

        return True  # Return success status
    except Exception as e:
        logging.error(f"Shard '{shard}': Unexpected error during deletion: {e}")
        return False  # Return failure status

# Function to drop a sharded collection either concurrently or sequentially
# Drops the sharded collection after deleting documents, with optional concurrent deletion.
def drop_sharded_collection(collection_name):
    """
    Deletes documents from a sharded collection either concurrently across shards
    or sequentially, based on the configuration.
    """
    logging.info(f"Starting rolling drop for sharded collection: {collection_name} (Dry run: {DRY_RUN})")

    try:
        # Step 1: Fetch chunk information for the collection
        shard_map = {}
        try:
            # Call all_chunk_info to retrieve the shard map
            shard_map = all_chunk_info(f"{db_name}.{collection_name}", estimate=True)
            if not shard_map:
                logging.warning(f"No chunks found for collection {collection_name}, treating as unsharded.")
        except Exception as e:
            logging.error(f"Error while fetching chunk information for collection {collection_name}: {e}")

        # Step 2: Handle collections with no chunks (treat as unsharded)
        if not shard_map:
            try:
                if not DRY_RUN:
                    tenantdb[collection_name].drop(writeConcern=write_concern)
                    logging.info(f"Unsharded collection {collection_name} dropped successfully")
                    collection_status.append((collection_name, "Unsharded", "Success"))
                else:
                    logging.info(f"DRY RUN: Would have dropped unsharded collection: {collection_name}")
                    collection_status.append((collection_name, "Unsharded", "Dry Run Success"))
            except Exception as e:
                logging.error(f"Error dropping unsharded collection {collection_name}: {e}")
                collection_status.append((collection_name, "Unsharded", "Drop Error"))
            return  # Exit after handling unsharded collection

        # Step 3: Check if concurrent deletion is enabled
        if concurrent_deletion_enabled:
            logging.info("Concurrent deletion enabled. Deleting from shards concurrently...")
            with ThreadPoolExecutor() as executor:  # No need for `concurrent_shards` value, use default workers
                futures = {
                    executor.submit(delete_from_shard, shard, chunks, collection_name): shard
                    for shard, chunks in shard_map.items()
                }

                for future in as_completed(futures):
                    shard = futures[future]
                    try:
                        success = future.result()
                        if success:
                            logging.info(f"Shard '{shard}': Successfully processed")
                            collection_status.append((collection_name, shard, "Success"))
                        else:
                            logging.error(f"Shard '{shard}': Failed to process")
                            collection_status.append((collection_name, shard, "Failed"))
                    except Exception as e:
                        logging.error(f"Shard '{shard}': Unexpected error: {e}")
                        collection_status.append((collection_name, shard, "Unexpected Error"))
        else:
            logging.info("Concurrent deletion disabled. Deleting from shards sequentially...")
            # Step 4: Sequentially delete data from shards
            for shard, chunks in shard_map.items():
                success = delete_from_shard(shard, chunks, collection_name)
                if success:
                    logging.info(f"Shard '{shard}': Successfully processed")
                    collection_status.append((collection_name, shard, "Success"))
                else:
                    logging.error(f"Shard '{shard}': Failed to process")
                    collection_status.append((collection_name, shard, "Failed"))

        # Step 5: Drop the collection after processing all shards
        try:
            if not DRY_RUN:
                logging.info(f"Dropping sharded collection: {collection_name}")
                tenantdb[collection_name].drop(writeConcern=write_concern)
                logging.info(f"Sharded collection {collection_name} dropped successfully")
            else:
                logging.info(f"DRY RUN: Would have dropped sharded collection: {collection_name}")
            #collection_status.append((collection_name, "Sharded", "Success"))
        except Exception as e:
            logging.error(f"Error dropping sharded collection {collection_name}: {e}")
            collection_status.append((collection_name, "Sharded", "Drop Error"))

    except Exception as e:
        logging.error(f"Unexpected error while processing collection {collection_name}: {e}")
        collection_status.append((collection_name, "Sharded", "Unexpected Error"))


# Function to drop unsharded collections
# Drops unsharded collections with optional dry run simulation.
def drop_unsharded_collection(collection_name):
    logging.info(f"Starting drop for unsharded collection: {collection_name} (Dry run: {DRY_RUN})")

    try:
        if not DRY_RUN:
            # Fetch and log collection stats before drop
            stats = tenantdb.command("collstats", collection_name)
            logging.info(f"Collection stats before drop: {stats}")

            # Drop the collection with majority write concern to ensure replication safety
            tenantdb.command({"drop": collection_name, "writeConcern": write_concern})
            logging.info(f"Unsharded collection {collection_name} dropped successfully")
        else:
            logging.info(f"DRY RUN: Would have dropped unsharded collection: {collection_name}")
        collection_status.append((collection_name, "Unsharded", "Success"))
    except Exception as e:
        logging.error(f"Error dropping unsharded collection {collection_name}: {e}")
        collection_status.append((collection_name, "Unsharded", "Failure"))

# Main function to process both sharded and unsharded collections
# Iterates over all collections and calls the appropriate drop function based on sharding status.
def perform_rolling_drop_for_all_collections():
    try:
        sharded_collections, unsharded_collections = get_sharded_unsharded_collections()

        for collection_name in sharded_collections:
            try:
                logging.info(f"Processing sharded collection: {collection_name} (Dry run: {DRY_RUN})")
                drop_sharded_collection(collection_name)
            except Exception as e:
                logging.error(f"Error processing sharded collection {collection_name}: {e}")
                #collection_status.append((collection_name, "Sharded", "Failure"))

        for collection_name in unsharded_collections:
            try:
                logging.info(f"Processing unsharded collection: {collection_name} (Dry run: {DRY_RUN})")
                drop_unsharded_collection(collection_name)
            except Exception as e:
                logging.error(f"Error processing unsharded collection {collection_name}: {e}")
                collection_status.append((collection_name, "Unsharded", "Failure"))

    except Exception as e:
        logging.error(f"Error during rolling drop process: {e}")

    log_summary()

if __name__ == "__main__":
    # Record the start time
    start_time = time.time()

    logging.info(f"Starting recursive rolling drop for all collections (Dry run: {DRY_RUN})")
    perform_rolling_drop_for_all_collections()
    logging.info(f"Recursive rolling drop completed for all collections (Dry run: {DRY_RUN})")

    # Record the end time and calculate elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Convert to hours, minutes, seconds
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    # Log the time taken for completion
    logging.info(f"Script completed in {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
    logging.info("#" * 50)
