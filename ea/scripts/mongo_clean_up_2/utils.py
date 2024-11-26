# utils.py
import logging
from tabulate import tabulate

# Function to log the summary of success or failure at the end
# Logs a summary table of the results for each collection processed during the rolling drop.
def log_summary():
    summary_table = tabulate(collection_status, headers=["Collection Name", "Shard", "Status"], tablefmt="grid")
    logging.info("Processing summary\n" + summary_table)


# Function to get sharded and unsharded collections
# Fetches the collections from the MongoDB instance and categorizes them into sharded or unsharded collections.
def get_sharded_unsharded_collections(tenantdb):
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

def get_shard_key_for_collection(collection_name, configdb, db_name):
    """
    Retrieves the shard key for the specified collection.

    :param collection_name: Name of the collection
    :param configdb: The MongoDB config database connection
    :param db_name: The database name
    :return: The shard key as a dictionary
    """
    try:
        namespace = f"{db_name}.{collection_name}"
        collection_metadata = configdb["collections"].find_one({"_id": namespace})

        if not collection_metadata:
            raise ValueError(f"Collection {namespace} not found in config.collections")

        return collection_metadata.get("key")
    except Exception as e:
        logging.error(f"Error retrieving shard key for collection {collection_name}: {e}")
        raise


def get_relevant_index(collection, query_fields):
    """
    Determines the most relevant index for the given query fields.

    :param collection: The MongoDB collection object
    :param query_fields: List of fields in the deletion query
    :return: The most relevant index, or None if no relevant index is found
    """
    try:
        indexes = collection.list_indexes()
        relevant_index = None

        for index in indexes:
            index_fields = set(index["key"].keys())
            if index_fields.intersection(query_fields):  # Check if any query field matches the index
                if not relevant_index or len(index_fields) < len(relevant_index["key"].keys()):
                    relevant_index = index  # Select the smallest matching index

        return relevant_index
    except Exception as e:
        logging.error(f"Error fetching indexes for collection: {e}")
        raise


def is_rebalancing(configdb):
    """
    Checks if any chunk migrations are currently in progress.
    Returns True if rebalancing is detected, False otherwise.
    """
    try:
        active_migrations = configdb.command("currentOp", {"active": True, "desc": "chunk migration"})
        return bool(active_migrations.get("inprog"))
    except Exception as e:
        logging.error(f"Error checking rebalancing status: {e}")
        raise
