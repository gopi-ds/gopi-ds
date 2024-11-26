# evaluate_and_delete.py
import logging
from utils import get_shard_key_for_collection, get_relevant_index, is_rebalancing


def evaluate_and_delete(tenantdb, configdb, db_name, collection_name, query, dry_run=True):
    """
    Evaluates the best deletion strategy for a collection and applies it.

    :param tenantdb: The tenant database connection
    :param configdb: The config database connection
    :param db_name: The database name
    :param collection_name: The collection name
    :param query: The deletion query
    :param dry_run: Boolean flag for dry run mode
    """
    try:
        collection = tenantdb[collection_name]
        shard_key = get_shard_key_for_collection(collection_name, configdb, db_name)
        query_fields = list(query.keys())

        # Determine the best strategy
        if set(shard_key).intersection(query_fields):
            strategy_type = "shard_key"
            logging.info(f"Using shard key strategy for {collection_name}.")
        else:
            index = get_relevant_index(collection, query_fields)
            if index:
                strategy_type = "indexed"
                logging.info(f"Using index {index['key']} for {collection_name}.")
            else:
                strategy_type = "non_indexed"
                logging.info(f"Using collection scan for {collection_name}.")

        # Apply the appropriate deletion strategy
        if dry_run:
            logging.info(f"DRY RUN: Would evaluate strategy '{strategy_type}' for collection {collection_name}.")
        else:
            if strategy_type == "shard_key":
                result = collection.delete_many(query)
                logging.info(f"Deleted {result.deleted_count} documents using shard key strategy.")
            elif strategy_type == "indexed":
                result = collection.delete_many(query)
                logging.info(f"Deleted {result.deleted_count} documents using index.")
            elif strategy_type == "non_indexed":
                result = collection.delete_many(query)
                logging.info(f"Deleted {result.deleted_count} documents using collection scan.")
            else:
                logging.warning(f"Unknown strategy for collection {collection_name}. Skipping deletion.")
    except Exception as e:
        logging.error(f"Error evaluating and deleting from collection {collection_name}: {e}")
