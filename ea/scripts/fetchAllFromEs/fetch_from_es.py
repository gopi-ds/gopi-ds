import csv
import time
import json
import re
import os
import argparse
import logging
from threading import Lock

from logging_utils import CompressedRotatingFileHandler

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError, RequestError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random

# Constants
STATE_FILE = "progress_state.json"
state_lock = Lock()

# Logger Setup
def setup_logger(log_folder, log_file, max_bytes=5 * 1024 * 1024, backup_count=5):
    """
    Set up a rolling logger to log exclusively to a file.
    Args:
        log_folder (str): Folder where logs will be saved.
        log_file (str): Name of the log file.
        max_bytes (int): Maximum size of a log file before it rolls over.
        backup_count (int): Number of backup log files to keep.
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


def save_state(state):
    """Persist progress state to a file with a lock."""
    with state_lock:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=4)

def load_state():
    """Load progress state from a file with a lock."""
    with state_lock:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        return {"completed_indices": [], "in_progress": {}}


def mark_index_complete(state, index_name):
    """Mark an index as completed in the state."""
    state["completed_indices"].append(index_name)
    state["in_progress"].pop(index_name, None)
    save_state(state)


def mark_slice_complete(state, index_name, slice_id):
    """Mark a slice as completed."""
    state["in_progress"].setdefault(index_name, {}).setdefault("slices", {})[str(slice_id)] = {"completed": True}
    save_state(state)


# Retry Logic with Indefinite Retries
def retry_indefinitely(func, *args, base_delay=1, max_delay=300, **kwargs):
    """
    Retries a function indefinitely with exponential backoff.
    """
    delay = base_delay
    while True:
        try:
            return func(*args, **kwargs)
        except (ConnectionError, TransportError, RequestError, NotFoundError) as e:
            logger.warning(f"Operation failed with error: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)  # Exponential backoff with a cap
            delay += random.uniform(0, 1)  # Add jitter to avoid thundering herd


# Elasticsearch Operations
def get_indices_by_pattern(es, pattern):
    """Retrieve indices that match a specific pattern."""
    all_indices = retry_indefinitely(es.cat.indices, format='json')
    return [index['index'] for index in all_indices if re.match(pattern.replace("*", ".*"), index['index'])]


def calculate_optimal_slices(es, index_name, max_slices=8):
    """Dynamically calculate the number of slices based on index metadata."""
    index_stats = retry_indefinitely(es.indices.stats, index=index_name)
    shard_count = index_stats['_shards']['total']
    doc_count = retry_indefinitely(es.count, index=index_name)['count']

    base_slices = shard_count
    scale_factor = max(1, doc_count // 10_000_000)
    return min(base_slices + scale_factor, max_slices)


def initialize_scroll(es, slice_id, total_slices, index_name, state, fields_to_export, page_size):
    """Initialize or resume a scroll context."""
    if index_name in state["in_progress"]:
        slice_data = state["in_progress"][index_name]["slices"].get(str(slice_id), {})
        if slice_data.get("completed"):
            logger.info(f"Index {index_name} Slice {slice_id}: Already completed. Skipping.")
            return None, [], 0
        if "scroll_id" in slice_data:
            logger.info(f"Resuming scroll for Index {index_name} Slice {slice_id}")
            return slice_data["scroll_id"], [], 0

    query = {
        "slice": {"id": slice_id, "max": total_slices},
        "track_total_hits": True,
        "query": {"match_all": {}},
        "_source": fields_to_export,
        "sort": ["_doc"]
    }
    response = retry_indefinitely(es.search, index=index_name, body=query, scroll='5m', size=page_size)
    scroll_id = response["_scroll_id"]
    state["in_progress"].setdefault(index_name, {}).setdefault("slices", {})[str(slice_id)] = {"scroll_id": scroll_id}
    save_state(state)
    return scroll_id, response["hits"]["hits"], response["hits"]["total"]["value"]


def scroll_results(es, slice_id, scroll_id, state, index_name):
    """Fetch results for the current scroll context."""
    response = retry_indefinitely(es.scroll, scroll_id=scroll_id, scroll='5m')
    new_scroll_id = response["_scroll_id"]
    state["in_progress"][index_name]["slices"][str(slice_id)]["scroll_id"] = new_scroll_id
    save_state(state)
    return new_scroll_id, response["hits"]["hits"]


def scroll_slice(es, slice_id, total_slices, index_name, state, in_progress_folder, fields_to_export, page_size):
    """Fetch all results for a given slice using the scroll API."""
    scroll_id, hits, _ = initialize_scroll(es, slice_id, total_slices, index_name, state, fields_to_export, page_size)
    if not scroll_id:
        return 0

    fetched_records = 0
    buffer = []
    slice_file = os.path.join(in_progress_folder, f"{index_name}_{slice_id}.csv")

    with open(slice_file, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields_to_export)

        while hits:
            for hit in hits:
                buffer.append([hit['_source'].get(field, "") for field in fields_to_export])
                fetched_records += 1

                if len(buffer) >= 10000:
                    csvwriter.writerows(buffer)
                    buffer.clear()

            scroll_id, hits = scroll_results(es, slice_id, scroll_id, state, index_name)

        if buffer:
            csvwriter.writerows(buffer)

    logger.info(f"Index {index_name} Slice {slice_id}: Fetched {fetched_records} records.")
    mark_slice_complete(state, index_name, slice_id)
    return fetched_records

def process_completed_slices(index_name, total_slices, in_progress_folder, completed_folder, merge_enabled, fields_to_export):
    """
    Process completed slices based on the merge flag.
    If merge is enabled, slices are merged into a single CSV.
    Otherwise, slice files are moved to the completed folder.
    """
    if merge_enabled:
        final_index_file = os.path.join(completed_folder, f"{index_name}.csv")
        with open(final_index_file, 'w', newline='') as final_csvfile:
            csvwriter = csv.writer(final_csvfile)
            csvwriter.writerow(fields_to_export)

            for slice_id in range(total_slices):
                slice_file = os.path.join(in_progress_folder, f"{index_name}_{slice_id}.csv")
                try:
                    with open(slice_file, 'r') as slice_csvfile:
                        reader = csv.reader(slice_csvfile)
                        next(reader)  # Skip header
                        for row in reader:
                            csvwriter.writerow(row)
                    os.remove(slice_file)  # Delete slice file after merging
                except FileNotFoundError:
                    logger.warning(f"Slice file {slice_file} not found. Skipping.")
                except Exception as e:
                    logger.error(f"Error processing slice file {slice_file}: {e}")

        logger.info(f"Index {index_name}: Merged slices into {final_index_file}")
    else:
        for slice_id in range(total_slices):
            slice_file = os.path.join(in_progress_folder, f"{index_name}_{slice_id}.csv")
            completed_slice_file = os.path.join(completed_folder, f"{index_name}_{slice_id}.csv")
            try:
                os.rename(slice_file, completed_slice_file)
                logger.info(f"Moved slice file {slice_file} to {completed_slice_file}")
            except FileNotFoundError:
                logger.warning(f"Slice file {slice_file} not found. Skipping.")
            except Exception as e:
                logger.error(f"Error moving slice file {slice_file}: {e}")

        logger.info(f"Index {index_name}: All slices moved to {completed_folder}")

# Main Script
def main(config):
    global logger  # Use the global logger initialized in setup_logger

    es_host = config['Elasticsearch']['host']
    index_pattern = config['Elasticsearch']['index_pattern']
    page_size = config['Elasticsearch']['page_size']
    in_progress_folder = config['AppConfig']['es_in_progress_folder']
    completed_folder = config['AppConfig']['es_complete_folder']
    fields_to_export = config['Elasticsearch']['fields_to_export']
    merge_enabled = config['Elasticsearch'].get('merge_slices', True)

    # Ensure folders exist
    for folder in [in_progress_folder, completed_folder]:
        os.makedirs(folder, exist_ok=True)

    es = Elasticsearch([es_host])

    # Load state
    state = load_state()

    # Get matching indices
    matching_indices = get_indices_by_pattern(es, index_pattern)
    if not matching_indices:
        logger.error("No indices found matching pattern.")
        exit(0)

    # Process each index
    for index_name in tqdm(matching_indices, desc="Processing Indices"):
        if index_name in state["completed_indices"]:
            logger.info(f"Index {index_name} already completed. Skipping.")
            continue

        total_slices = calculate_optimal_slices(es, index_name)
        with ThreadPoolExecutor(max_workers=min(total_slices, 4)) as executor:
            futures = [
                executor.submit(scroll_slice, es, slice_id, total_slices, index_name, state, in_progress_folder, fields_to_export, page_size)
                for slice_id in range(total_slices)
            ]
            for future in tqdm(as_completed(futures), total=total_slices, desc=f"Index {index_name} Slices"):
                future.result()

        # Process completed slices
        process_completed_slices(index_name, total_slices, in_progress_folder, completed_folder, merge_enabled, fields_to_export)
        mark_index_complete(state, index_name)

    logger.info("All indices processed successfully.")

def load_config(config_path):
    """
    Load the configuration file.
    """
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print(f"Configuration file not found at {config_path}")  # Use print
        exit(1)
    except json.JSONDecodeError as e:
        print(f"Failed to parse configuration file: {e}")  # Use print
        exit(1)

def initialize_logger(config):
    """
    Initialize the logger using configuration.
    """
    log_folder = config.get("AppConfig", {}).get("log_folder", "./logs")
    log_file = config.get("AppConfig", {}).get("es_log_file", "es_fetch.log")
    log_max_bytes = config.get("AppConfig", {}).get("log_max_bytes", 100 * 1024 * 1024)
    log_backup_count = config.get("AppConfig", {}).get("log_backup_count", 100)
    return setup_logger(log_folder, log_file, log_max_bytes, log_backup_count)

# Main Script
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elasticsearch Data Export Script")
    parser.add_argument("--config", required=True, help="Path to configuration file (JSON format)")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Initialize logger
    logger = initialize_logger(config)
    logger.info("Logger initialized successfully.")

    # Execute the main script logic
    main(config)
