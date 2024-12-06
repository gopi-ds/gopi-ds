import argparse
import csv
import json
import logging
import os
import shutil
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from logging_utils import CompressedRotatingFileHandler

import pymongo
from pymongo import MongoClient, WriteConcern
from pymongo.errors import BulkWriteError

shutdown_flag = False

# Logger Setup
def setup_logger(log_folder, log_file, max_bytes, backup_count):
    """
    Set up a rolling logger to log exclusively to a file.
    """
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler with rolling logs
    file_handler = CompressedRotatingFileHandler(
        os.path.join(log_folder, log_file),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(funcName)s - %(message)s")
    file_handler.setFormatter(formatter)

    # Add file handler to logger
    logger.addHandler(file_handler)

    return logger

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

@retry_on_failure()
def connect_to_mongo():
    """Attempts to connect to MongoDB with retries on failure."""
    uri = config["shared_uri"]
    client = MongoClient(
        uri,
        maxPoolSize=50,  # Maximum number of connections in the pool
        minPoolSize=5,   # Optional: Minimum number of connections in the pool
        serverSelectionTimeoutMS=config["connection_timeout"],
        socketTimeoutMS=config["socket_timeout"]
    )
    client.admin.command('ping')  # Test connection
    logging.info("Connected to MongoDB successfully.")
    return client

# MongoDB Utility Functions
@retry_on_failure()
def ensure_index_on_gcid(unique_gcid_collection, collection_name):
    """Ensures an index on 'gcId' exists in the temporary collection."""
    indexes = unique_gcid_collection.list_indexes()
    index_exists = any(index['key'] == {'gcId': 1} for index in indexes)

    if not index_exists:
        unique_gcid_collection.create_index("gcId", unique=True)
        logging.info("Created unique index on 'gcId' for collection: %s", collection_name)
    else:
        logging.info("Index on 'gcId' already exists in collection: %s", collection_name)


@retry_on_failure()
def bulk_upsert(unique_gcid_collection, records, unique_key):
    """
    Perform bulk upserts into a MongoDB collection with infinite retries on errors.
    """
    while not shutdown_flag:
        try:
            bulk_operations = []

            for record in records:
                # Build the upsert operation
                filter_query = {unique_key: record[unique_key]} if unique_key in record else {}
                bulk_operations.append(
                    pymongo.UpdateOne(
                        filter_query, {"$set": record}, upsert=True
                    )
                )

            if bulk_operations:
                result = unique_gcid_collection.bulk_write(bulk_operations)
                logging.info(
                    f"Upserted {result.upserted_count} records, "
                    f"matched {result.matched_count}, modified {result.modified_count}."
                )
            break  # Exit the loop if operation succeeds
        except BulkWriteError as bwe:
            logging.error(f"Bulk write error: {bwe.details}. Retrying...")
            time.sleep(300)  # Wait before retrying
        except Exception as e:
            logging.error(f"Unexpected error during bulk upsert: {e}. Retrying...")
            time.sleep(300)  # Wait before retrying

def normalize_headers(record, mappings):
    """
    Normalize headers in a record to ensure consistent field names.

    Parameters:
        record (dict): A single record (row) from the CSV.
        mappings (dict): A dictionary mapping original headers to normalized headers.

    Returns:
        dict: The normalized record.
    """
    return {mappings.get(k, k): v for k, v in record.items()}


# File Processing
def process_file(file_path, unique_gcid_collection, unique_key, completed_folder, batch_size, max_workers_per_file, delete_after_upsert):
    """
    Process a CSV file: bulk upsert into MongoDB in batches using multithreading for higher throughput.

    Parameters:
        file_path (str): Path to the CSV file.
        unique_gcid_collection (Collection): MongoDB collection object for upserts.
        unique_key (str): Key used for deduplication/upserts.
        batch_size (int): Number of records per batch.
        max_workers_per_file (int): Number of threads for concurrent processing.
        completed_folder (str): Path to the folder where completed files will be moved (if delete_after_upsert is False).
        delete_after_upsert (bool): Whether to delete the file after successful processing.
    """
    logger.info(f"Processing file: {file_path}")

    # Header mappings to normalize field names
    header_mappings = {"gcid": "gcId"}  # Map 'gcid' to 'gcId'

    def process_batch(batch):
        """Process a single batch."""
        bulk_upsert(unique_gcid_collection, batch, unique_key)

    with open(file_path, 'r', buffering=5 * 1024 * 1024) as csvfile:  # Buffered I/O for large files
        reader = csv.DictReader(csvfile)
        records = [normalize_headers(row, header_mappings) for row in reader]

    if records:
        total_batches = -(-len(records) // batch_size)  # Ceiling division for total batches
        logger.info(f"File contains {len(records)} records, divided into {total_batches} batches.")

        # Use ThreadPoolExecutor for concurrent batch processing
        with ThreadPoolExecutor(max_workers=max_workers_per_file) as executor:
            futures = []
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                logger.info(f"Submitting batch {i // batch_size + 1}/{total_batches} for processing.")
                futures.append(executor.submit(process_batch, batch))

            # Wait for all threads to complete and log results
            for future in futures:
                try:
                    future.result()  # Raise any exceptions that occurred in the threads
                except Exception as e:
                    logger.error(f"Error processing a batch: {e}")
    else:
        logger.warning(f"No records found in file: {file_path}")

    if delete_after_upsert:
        # Delete the file
        os.remove(file_path)
        logger.info(f"File {file_path} has been deleted after processing.")
    else:
        # Ensure the completed folder exists
        if not os.path.exists(completed_folder):
            os.makedirs(completed_folder)

        # Rename file to mark it as processed and move to the completed folder
        completed_file_name = os.path.basename(file_path) + ".completed"
        completed_file_path = os.path.join(completed_folder, completed_file_name)
        shutil.move(file_path, completed_file_path)
        logger.info(f"File processed and moved to: {completed_file_path}")


# Polling and Processing
def poll_and_process_files(unique_gcid_collection, unique_key, folder_to_watch, completed_folder, timeout_minutes, poll_interval, batch_size, max_files, max_workers_per_file, delete_after_upsert):
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_files) as executor:
        while True:
            futures = []
            files_processed = False
            for file_name in os.listdir(folder_to_watch):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_to_watch, file_name)
                    futures.append(
                        executor.submit(
                            process_file,
                            file_path,
                            unique_gcid_collection,
                            unique_key,
                            completed_folder,
                            batch_size,
                            max_workers_per_file,
                            delete_after_upsert
                        )
                    )
                    files_processed = True

            for future in futures:
                try:
                    future.result()  # Wait for each file to complete
                except Exception as e:
                    logger.error(f"Error processing file: {e}")

            if files_processed:
                start_time = time.time()
                logger.info("Processed files. Continuing to poll for new files.")
            else:
                logger.info("No files to process. Waiting for new files...")

            elapsed_time = time.time() - start_time
            if elapsed_time >= timeout_minutes * 60:
                logger.info("No new files detected for the timeout period. Terminating script.")
                break

            time.sleep(poll_interval)


# Load Config Utility
def load_config(config_path):
    """
    Load the configuration file.
    """
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse configuration file: {e}")
        exit(1)

def ensure_directories_exist(directories):
    """
    Ensure that all required directories exist.
    Create them if they are missing.

    Parameters:
        directories (list): List of directory paths to verify/create.
    """
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created missing directory: {directory}")
        else:
            logger.info(f"Directory already exists: {directory}")


def initialize_logger(config):
    """
    Initialize the logger using configuration.
    """
    log_folder = config.get("AppConfig", {}).get("log_folder", "./logs")
    log_file = config.get("AppConfig", {}).get("mongo_log_file", "mongo_upsert.log")
    log_max_bytes = config.get("AppConfig", {}).get("log_max_bytes", 100 * 1024 * 1024)
    log_backup_count = config.get("AppConfig", {}).get("log_backup_count", 100)
    return setup_logger(log_folder, log_file, log_max_bytes, log_backup_count)

def main(config):
    global logger  # Use the global logger initialized in setup_logger

    # Extract configuration parameters
    tenantdb_name = config["MongoDB"]["tenantdb"]
    collection_name = config["MongoDB"]["collection"]
    unique_key = config["MongoDB"].get("unique_key", "_id")
    folder_to_watch = config["AppConfig"]["es_complete_folder"]
    completed_folder = config["AppConfig"]["mongo_complete_folder"]
    timeout_minutes = config["AppConfig"]["timeout_minutes"]
    poll_interval = config["AppConfig"]["poll_interval"]
    batch_size = config["AppConfig"]["batch_size"]
    max_files = config["AppConfig"]["max_files"]
    max_workers_per_file = config["AppConfig"]["max_workers_per_file"]
    delete_after_upsert = config["AppConfig"]["delete_after_upsert"]

    # Ensure required directories exist
    ensure_directories_exist([folder_to_watch, completed_folder, config["AppConfig"]["log_folder"]])

    # Connect to MongoDB
    client = connect_to_mongo()
    tenantdb = client[tenantdb_name]
    unique_gcid_collection = tenantdb.get_collection(
        collection_name,
        write_concern=WriteConcern(w=1)
    )

    # Ensure the collection exists
    ensure_index_on_gcid(unique_gcid_collection, collection_name)

    # Start polling for files
    logger.info(f"Starting to poll folder: {folder_to_watch}")
    poll_and_process_files(
        unique_gcid_collection,
        unique_key,
        folder_to_watch,
        completed_folder,
        timeout_minutes,
        poll_interval,
        batch_size,
        max_files,
        max_workers_per_file,
        delete_after_upsert
    )


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="MongoDB Bulk Upsert with Polling")
    parser.add_argument("--config", required=True, help="Path to configuration file (JSON format)")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Initialize logger
    logger = initialize_logger(config)
    logger.info("Logger initialized successfully.")

    # Execute the main script logic
    main(config)