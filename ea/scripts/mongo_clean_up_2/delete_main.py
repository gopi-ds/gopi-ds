# main_script.py
import logging
from utils import get_sharded_unsharded_collections
from evaluate_and_delete import evaluate_and_delete
from setup import initialize_logging, initialize_mongo_connection

# Track the status of each collection (name, type, status)
collection_status = []

def main(tenantdb, configdb, db_name, dry_run=True):
    """
    Main script to enumerate collections and delegate evaluation to another script.

    :param tenantdb: The tenant database connection
    :param configdb: The config database connection
    :param db_name: The database name
    :param dry_run: Boolean flag for dry run mode
    """
    try:
        sharded_collections, unsharded_collections = get_sharded_unsharded_collections(tenantdb)

        for collection_name in sharded_collections + unsharded_collections:
            logging.info(f"Processing collection: {collection_name}")

            # Define query dynamically or via config
            query = {"someField": {"$gte": 100, "$lt": 200}}  # Replace with your query

            # Delegate evaluation and deletion to the external script
            evaluate_and_delete(tenantdb, configdb, db_name, collection_name, query, dry_run)

    except Exception as e:
        logging.error(f"Error processing collections: {e}")


if __name__ == "__main__":

    # Set up MongoDB connections and other configurations
    tenantdb = initialize_mongo_connection("mongodb://admin@localhost:37018", "testDatabase")
    configdb = initialize_mongo_connection("mongodb://admin@localhost:37018", "config")

    logging.info("Starting collection processing...")
    main(tenantdb, configdb, "testDatabase", dry_run=True)
    logging.info("Collection processing completed.")
