import argparse
import datetime
import json
import logging
import time
from pymongo.errors import OperationFailure
from setup import initialize_logging, initialize_mongo_connection


def parse_arguments():
    """
    Parses command-line arguments to retrieve the configuration file path.

    Returns:
        Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Rolling Drop Script for MongoDB")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the JSON configuration file."
    )
    return parser.parse_args()


# Function to read configuration from a JSON file
def read_config(config_file):
    """
    Reads configuration from a JSON file.

    :param config_file: Path to the JSON configuration file.
    :return: Dictionary containing configuration data.
    :raises: Exception if the file cannot be read or parsed.
    """
    try:
        with open(config_file, 'r') as file:
            config_data = json.load(file)
        return config_data
    except Exception as e:
        print(f"Error loading config file: {e}")
        raise


def log_banner():
    """
    Logs a banner message to indicate the start of the program.
    """
    logging.info("=" * 50)
    logging.info(f"Starting Rolling Drop Program at {datetime.datetime.now()}")
    logging.info("=" * 50)


def is_hashed_shard_key(db_name, collection_name, config_db):
    """
    Determines if a collection's shard key is hashed.

    This function retrieves the shard key metadata for a collection from the
    config database and checks if any part of the shard key is of type "hashed."
    Logs a warning and returns False if metadata retrieval fails.

    Args:
        db_name (str): Name of the database.
        collection_name (str): Name of the collection.
        config_db (pymongo.database.Database): Connection to the config database.

    Returns:
        bool: True if the shard key is hashed, False otherwise.
    """
    try:
        namespace = f"{db_name}.{collection_name}"
        collection_metadata = config_db["collections"].find_one({"_id": namespace})
        if not collection_metadata:
            logging.warning(f"Metadata for collection {namespace} not found in config.collections. Assuming non-hashed.")
            return False
        shard_key = collection_metadata.get("key", {})
        return any(value == "hashed" for value in shard_key.values())
    except Exception as e:
        logging.warning(f"Error checking shard key type for {collection_name}: {e}. Assuming non-hashed.")
        return False


def retry_on_failure_infinite(delay=5, backoff=1):
    """
    Decorator to retry a function infinitely until it succeeds.

    Includes an exponential backoff for delays between retries.

    :param delay: Initial delay between retries in seconds.
    :param backoff: Multiplier for exponential backoff.
    :return: Decorated function with retry logic.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            current_delay = delay
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.warning(f"Attempt {attempt}: Function {func.__name__} failed with error: {e}")
                    logging.info(f"Retrying after {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator


def is_rebalancing(config_db):
    """
    Checks if any chunk migrations (rebalancing) are currently in progress.

    :param config_db: Config database connection object.
    :return: True if rebalancing is detected, False otherwise.
    :raises: OperationFailure if the currentOp command fails.
    """
    try:
        active_migrations = config_db.command("currentOp", {"active": True, "desc": "chunk migration"})
        in_progress_operations = active_migrations.get("inprog", [])
        if in_progress_operations:
            logging.info(f"Rebalancing detected: {len(in_progress_operations)} operations in progress.")
            return True
        logging.info("No rebalancing operations currently in progress.")
        return False
    except OperationFailure as e:
        logging.error(f"Failed to check rebalancing status: {e}")
        raise


def wait_for_rebalancing_to_complete(config_db, check_interval=300):
    """
    Pauses the script if rebalancing is detected and waits until it completes.

    :param config_db: Config database connection object.
    :param check_interval: Time in seconds to wait before rechecking.
    """
    while is_rebalancing(config_db):
        logging.warning(f"Rebalancing in progress. Pausing for {check_interval} seconds...")
        time.sleep(check_interval)
    logging.info("Rebalancing completed. Resuming operations.")


@retry_on_failure_infinite(delay=60, backoff=1)
def delete_with_range_key(collection, shard_key, min_key, max_key, write_concern, dry_run):
    """
    Deletes documents in a range defined by the shard key.

    :param collection: MongoDB collection object.
    :param shard_key: The shard key for deletion.
    :param min_key: Minimum value of the range.
    :param max_key: Maximum value of the range.
    :param write_concern: Write concern configuration.
    :param dry_run: If True, simulates deletion without actual changes.
    :return: Number of documents deleted (0 if dry_run is True).
    """
    query = {shard_key: {"$gte": min_key[shard_key], "$lt": max_key[shard_key]}}
    if dry_run:
        logging.info(f"DRY RUN: Would have deleted documents with range query: {query}")
        return 0
    else:
        result = collection.delete_many(query, writeConcern=write_concern)
        logging.info(f"Deleted {result.deleted_count} documents using range key query: {query}")
        return result.deleted_count


@retry_on_failure_infinite(delay=60, backoff=1)
def delete_with_hashed_key(collection, shard_key, min_key, max_key, write_concern, dry_run):
    query = {shard_key: {"$gte": min_key[shard_key], "$lt": max_key[shard_key]}}
    if dry_run:
        logging.info(f"DRY RUN: Would have deleted documents with hashed query: {query}")
        return 0
    else:
        result = collection.delete_many(query, writeConcern=write_concern)
        logging.info(f"Deleted {result.deleted_count} documents using hashed key query: {query}")
        return result.deleted_count


@retry_on_failure_infinite(delay=60, backoff=1)
def delete_with_full_scan(collection, batch_size, write_concern, dry_run, delay_between_batches=0):
    """
    Deletes documents from a collection in batches or simulates deletion in DRY_RUN mode.

    :param collection: MongoDB collection object.
    :param batch_size: Number of documents to delete in each batch.
    :param write_concern: Write concern configuration.
    :param dry_run: If True, simulates deletion without actual changes.
    :param delay_between_batches: Optional delay between batch deletions in seconds.
    :return: Total number of documents deleted.
    """
    remaining_docs = collection.count_documents({})
    total_deleted = 0
    logging.info(f"Starting full scan deletion for collection '{collection.name}' with {remaining_docs} documents.")
    while remaining_docs > 0:
        delete_count = min(batch_size, remaining_docs)
        if dry_run:
            logging.info(f"DRY RUN: Would have deleted {delete_count} documents in this batch.")
            total_deleted += delete_count
        else:
            result = collection.delete_many({}, limit=delete_count, writeConcern=write_concern)
            total_deleted += result.deleted_count
            logging.info(f"Deleted {result.deleted_count} documents in this batch.")
        remaining_docs -= delete_count
        logging.info(f"Remaining documents: {remaining_docs}. Total deleted: {total_deleted}")
        if delay_between_batches > 0:
            time.sleep(delay_between_batches)
    logging.info(f"Full scan deletion completed for collection '{collection.name}'. Total deleted: {total_deleted}")
    return total_deleted


@retry_on_failure_infinite(delay=60, backoff=1)
def initialize_collection_status(collection_name, shard, total_chunks, total_documents):
    """
    Initializes or updates the status of a collection or shard in the status tracking collection.

    Args:
        collection_name (str): The name of the collection being processed.
        shard (str or None): The name of the shard (use None for unsharded collections).
        total_chunks (int): Total number of chunks to process.
        total_documents (int): Estimated total documents in the collection/shard.

    Returns:
        None
    """
    status_doc = {
        "collection_name": collection_name,
        "shard": shard,
        "status": "pending",
        "total_documents": total_documents,
        "total_deleted": 0,
        "chunks_processed": 0,
        "total_chunks": total_chunks,
        "last_processed_chunk": None,
        "last_updated": datetime.datetime.now(),
        "error": None
    }
    alcatrazdb[status_tracking_collection_name].update_one(
        {"collection_name": collection_name, "shard": shard},
        {"$set": status_doc},
        upsert=True
    )


@retry_on_failure_infinite(delay=60, backoff=1)
def update_collection_status(collection_name, shard, chunks_processed, total_deleted, last_processed_chunk, status="in_progress", error=None):
    """
    Updates the status of a collection or shard in the status tracking collection.

    Args:
        collection_name (str): The name of the collection being processed.
        shard (str or None): The name of the shard (use None for unsharded collections).
        chunks_processed (int): The number of chunks processed so far.
        total_deleted (int): The total number of documents deleted so far.
        last_processed_chunk (dict or None): Information about the last processed chunk.
        status (str, optional): Current status of the process ("in_progress", "completed", "failed"). Defaults to "in_progress".
        error (str, optional): Error message if the status is "failed". Defaults to None.

    Returns:
        None
    """
    alcatrazdb[status_tracking_collection_name].update_one(
        {"collection_name": collection_name, "shard": shard},
        {
            "$set": {
                "status": status,
                "chunks_processed": chunks_processed,
                "total_deleted": total_deleted,
                "last_processed_chunk": last_processed_chunk,
                "last_updated": datetime.datetime.now(),
                "error": error
            }
        }
    )


def get_collection_status(collection_name, shard):
    """
    Retrieves the current status of a collection or shard from the status tracking collection.

    Args:
        collection_name (str): The name of the collection whose status is being retrieved.
        shard (str or None): The name of the shard (use None for unsharded collections).

    Returns:
        dict or None: The status document if found, or None if no matching document exists.
    """
    return alcatrazdb[status_tracking_collection_name].find_one({"collection_name": collection_name, "shard": shard})


@retry_on_failure_infinite(delay=60, backoff=1)
def delete_from_shard(shard, chunks, collection_name, is_hashed=False):
    """
    Deletes documents from a specific shard by processing its chunks.

    This method processes chunks sequentially, tracking the deletion progress and
    handling resumable state using the status tracking collection.

    Args:
        shard (str): The name of the shard being processed.
        chunks (list): List of chunk metadata for the shard.
        collection_name (str): Name of the collection being processed.
        is_hashed (bool, optional): Whether the shard key is hashed. Defaults to False.

    Returns:
        None
    """
    collection = tenantdb[collection_name]
    total_chunks = len(chunks)
    # Count total documents in the collection (if not already initialized)
    total_documents = collection.estimated_document_count()
    # Initialize status tracking
    initialize_collection_status(collection_name, shard, total_chunks, total_documents)
    # Retrieve last saved state for resumption
    status = get_collection_status(collection_name, shard)
    chunks_processed = status.get("chunks_processed", 0)
    total_deleted = status.get("total_deleted", 0)
    # Resume from the last processed chunk
    for i, chunk in enumerate(chunks[chunks_processed:], start=chunks_processed + 1):
        min_key = chunk['minKey']
        max_key = chunk['maxKey']
        shard_key = list(min_key.keys())[0]
        try:
            # Wait for rebalancing to complete before processing this chunk
            wait_for_rebalancing_to_complete(configdb)
            remaining_docs = chunk['document_count']
            while remaining_docs > 0:
                if is_hashed:
                    deleted = delete_with_hashed_key(collection, shard_key, min_key, max_key, write_concern, DRY_RUN)
                else:
                    deleted = delete_with_range_key(collection, shard_key, min_key, max_key, write_concern, DRY_RUN)
                remaining_docs -= deleted
                total_deleted += deleted
            # Update status after processing the chunk
            update_collection_status(
                collection_name, shard, i, total_deleted, last_processed_chunk=chunk
            )
        except Exception as e:
            logging.error(f"Error processing chunk {i}/{total_chunks} in shard {shard}: {e}")
            update_collection_status(
                collection_name, shard, i, total_deleted, last_processed_chunk=chunk, status="failed", error=str(e)
            )
    # Mark shard as completed
    update_collection_status(collection_name, shard, total_chunks, total_deleted, last_processed_chunk=None, status="completed")


@retry_on_failure_infinite(delay=60, backoff=1)
def all_chunk_info(ns, estimate=True):
    """
    Gathers chunk information for a given namespace (ns) before the rolling drop process.

    This function logs the data in a tabular format for better understanding, converts chunk size
    to MB for readability, and provides consolidated totals for chunk size and object count.

    Args:
        ns (str): Namespace in the format "database.collection".
        estimate (bool): If True, the `datasize` command uses estimates.

    Returns:
        dict: A dictionary mapping each shard to its chunk data. Returns an empty dictionary if an error occurs.
    """
    try:
        # Fetch all chunks for the specified namespace from the config server
        chunks = configdb.chunks.find({"ns": ns}).sort("min", 1)
        # Fetch collection metadata to determine the shard key
        collection_metadata = configdb.collections.find_one({"_id": ns})
        if not collection_metadata:
            logging.warning(f"No metadata found for namespace {ns}")
            return {}
        shard_key = collection_metadata.get("key")
        if not shard_key:
            logging.warning(f"Shard key not found for namespace {ns}")
            return {}
        # Initialize the shard map for organizing chunk data
        shard_map = {}
        # Initialize totals for consolidated size and document count
        total_size_mb = 0
        total_objects = 0
        # Prepare data for tabular output
        chunk_info_list = []
        for chunk in chunks:
            shard = chunk["shard"]
            min_key = chunk["min"]
            max_key = chunk["max"]
            # Command to estimate the chunk size and number of objects
            data_size_cmd = {
                "datasize": ns,
                "keyPattern": shard_key,
                "min": min_key,
                "max": max_key,
                "estimate": estimate
            }
            # Fetch data size details for the chunk
            try:
                data_size_result = tenantdb.command(data_size_cmd)
            except OperationFailure as e:
                logging.warning(f"Failed to fetch datasize for chunk {chunk['_id']}: {e}")
                continue
            # Convert size to MB
            chunk_size_mb = data_size_result["size"] / (1024 * 1024)
            document_count = data_size_result["numObjects"]
            # Update the shard map
            if shard not in shard_map:
                shard_map[shard] = []
            shard_map[shard].append({
                "chunk_id": chunk["_id"],
                "minKey": min_key,
                "maxKey": max_key,
                "size_mb": f"{chunk_size_mb:.2f} MB",
                "document_count": document_count
            })
            # Add chunk information to the table
            chunk_info_list.append([
                shard, chunk["_id"], min_key, max_key, f"{chunk_size_mb:.2f} MB", document_count
            ])
            # Update totals
            total_size_mb += chunk_size_mb
            total_objects += document_count
        # Log the consolidated chunk information
        from tabulate import tabulate
        headers = ["Shard", "Chunk ID", "Min Key", "Max Key", "Chunk Size (MB)", "Objects in Chunk"]
        logging.info(f"Summary for collection {ns}\n"
                     f"Consolidated size: {total_size_mb:.2f} MB\n"
                     f"Consolidated object count: {total_objects}\n"
                     + tabulate(chunk_info_list, headers=headers, tablefmt="grid"))

        return shard_map
    except Exception as e:
        logging.warning(f"Error retrieving chunk information for namespace {ns}: {e}. Returning empty shard map.")
        return {}


@retry_on_failure_infinite(delay=60, backoff=1)
def process_sharded_collection(collection_name):
    """
    Processes a sharded collection by deleting documents chunk by chunk across all its shards.

    This method retrieves shard and chunk metadata, determines if the shard key is hashed,
    and invokes `delete_from_shard` for each shard.

    Args:
        collection_name (str): Name of the sharded collection to process.

    Returns:
        None
    """
    shard_map = all_chunk_info(f"{tenant_db_name}.{collection_name}")
    is_hashed = is_hashed_shard_key(tenant_db_name, collection_name, configdb)
    for shard, chunks in shard_map.items():
        total_deleted = delete_from_shard(shard, chunks, collection_name, is_hashed)
        logging.info(f"Shard '{shard}': Total deleted documents: {total_deleted}")


@retry_on_failure_infinite(delay=60, backoff=1)
def process_unsharded_collection(collection_name):
    """
    Processes an unsharded collection by deleting all documents through a full scan.

    Args:
        collection_name (str): Name of the unsharded collection to process.

    Returns:
        None
    """
    delete_with_full_scan(tenantdb[collection_name], deletes_per_batch, write_concern, DRY_RUN)


def get_sharded_unsharded_collections():
    """
    Categorizes collections in the tenant database into sharded and unsharded.

    This method retrieves all collection names from the tenant database,
    checks their shard status using the `collstats` command, and organizes them
    into separate lists for sharded and unsharded collections.

    Returns:
        tuple: A tuple containing two lists:
            - sharded_collections (list): Names of sharded collections.
            - unsharded_collections (list): Names of unsharded collections.
    """
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


def perform_rolling_drop_for_all_collections():
    """
    Executes a rolling drop for all collections in the tenant database.

    This method categorizes collections into sharded and unsharded, processes
    each collection by deleting documents in batches or chunks, and updates
    status tracking for resumable operations.

    Steps:
        1. Fetch sharded and unsharded collections.
        2. For sharded collections:
           - Retrieve shard and chunk metadata.
           - Skip shards marked as "completed."
           - Process each shard using `delete_from_shard`.
        3. For unsharded collections:
           - Skip collections marked as "completed."
           - Delete documents using `delete_with_full_scan`.
           - Update status tracking.

    Returns:
        None
    """
    sharded_collections, unsharded_collections = get_sharded_unsharded_collections()
    for collection_name in sharded_collections:
        shard_map = all_chunk_info(f"{tenant_db_name}.{collection_name}")
        is_hashed = is_hashed_shard_key(tenant_db_name, collection_name, configdb)
        for shard, chunks in shard_map.items():
            # Retrieve the current status and resume if necessary
            status = get_collection_status(collection_name, shard)
            if status and status["status"] == "completed":
                logging.info(f"Shard {shard} of collection {collection_name} is already completed. Skipping.")
                continue
            try:
                delete_from_shard(shard, chunks, collection_name, is_hashed)
            except Exception as e:
                logging.error(f"Error processing collection {collection_name}, shard {shard}: {e}")
    for collection_name in unsharded_collections:
        try:
            # Get the current status for the unsharded collection
            status = get_collection_status(collection_name, shard=None)
            if status and status["status"] == "completed":
                logging.info(f"Collection {collection_name} is already completed. Skipping.")
                continue
            # Initialize status tracking for unsharded collection
            total_documents = tenantdb[collection_name].estimated_document_count()
            initialize_collection_status(collection_name, shard=None, total_chunks=1, total_documents=total_documents)
            # Perform deletion
            total_deleted = delete_with_full_scan(tenantdb[collection_name], deletes_per_batch, write_concern, DRY_RUN)
            # Mark as completed
            update_collection_status(collection_name, shard=None, chunks_processed=1, total_deleted=total_deleted, last_processed_chunk=None, status="completed")
        except Exception as e:
            logging.error(f"Error processing unsharded collection {collection_name}: {e}")
            update_collection_status(collection_name, shard=None, chunks_processed=0, total_deleted=0, last_processed_chunk=None, status="failed", error=str(e))


if __name__ == "__main__":
    """
    Main entry point for the script.

    Reads the configuration file provided via the command-line parameter, initializes connections and logging,
    and starts the rolling drop process.
    """
    # Parse command-line arguments
    args = parse_arguments()
    config_file = args.config

    # Read the configuration file
    config = read_config(config_file)

    # MongoDB connection details from the config file
    mongo_uri = config['mongo_uri']
    tenant_db_name = config['tenant_db_name']
    config_db_name = config['config_db_name']
    alcatraz_db_name = config['alcatraz_db_name']
    status_tracking_collection_name = f"{tenant_db_name}_decomm_status_tracking"
    log_file = config['log_file']
    log_level_str = config['log_level']
    DRY_RUN = config.get('dry_run', True)
    deletes_per_batch = config.get('deletes_per_batch', 10000)
    concurrent_deletion_enabled = config.get('concurrent_deletion_enabled', False)
    write_concern = config.get("writeConcern", {"w": "majority", "j": True})

    # Initialize logging and MongoDB connection using setup.py functions
    initialize_logging(log_file, log_level_str)
    tenantdb = initialize_mongo_connection(mongo_uri, tenant_db_name)
    configdb = initialize_mongo_connection(mongo_uri, config_db_name)
    alcatrazdb = initialize_mongo_connection(mongo_uri, alcatraz_db_name)

    # Log the start of the process
    start_time = time.time()
    logging.info(f"Starting recursive rolling drop for all collections (Dry run: {DRY_RUN})")

    # Perform the rolling drop
    try:
        perform_rolling_drop_for_all_collections()
    except Exception as e:
        logging.error(f"Error during rolling drop: {e}")

    # Log the end of the process
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)

    logging.info(f"Script completed in {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")
    logging.info("#" * 50)