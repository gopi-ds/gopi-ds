import logging
import os

from pymongo import MongoClient


# Function to map log level from config to logging module constants
def get_log_level(level_str):
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    return log_levels.get(level_str.upper(), logging.INFO)  # Default to INFO if not found

# Function to ensure the log directory exists
def ensure_log_directory(log_file_path):
    log_directory = os.path.dirname(log_file_path)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)  # Create the directory if it doesn't exist

# Function to initialize logging
def initialize_logging(log_file, log_level_str):
    ensure_log_directory(log_file)
    logging.basicConfig(
        filename=log_file,  # Use the log file path from the config file
        level=get_log_level(log_level_str),  # Use the log level from the config
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log message format
        filemode='a'  # Append mode
    )
    logging.info("#"*50)
    logging.info("Logging initialized successfully.")

# Function to establish a MongoDB connection
def initialize_mongo_connection(mongo_uri, db_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        return db
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        raise
