"""

    To fetch ic_report collection

"""
import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient, errors

# Read configuration from JSON file
with open('config/dumpFromIC.json', 'r') as config_file:
    config = json.load(config_file)

# Extract MongoDB configuration
mongo_uri = config['MongoDB']['uri']
database_name = config['MongoDB']['database']
collection_name = config['MongoDB']['collection']
connection_timeout = config['MongoDB']['connection_timeout']
socket_timeout = config['MongoDB']['socket_timeout']

output_file = config['Files']['output_file']
max_threads = config['Processing']['max_threads']
retry_attempts = config['Processing']['retry_attempts']
retry_delay = config['Processing']['retry_delay']
mongo_batch_size = config['Processing']['mongo_batch_size']


def process_chunk(dummy_chunk):
    results = []
    client = MongoClient(mongo_uri, maxPoolSize=max_threads, connectTimeoutMS=connection_timeout,
                         socketTimeoutMS=socket_timeout)
    try:
        db = client[database_name]
        collection = db[collection_name]

        retry_count = 0
        while retry_count < retry_attempts:
            try:
                # Fetch all fields by not specifying a projection
                cursor = collection.find({}).batch_size(mongo_batch_size)
                for document in cursor:
                    results.append(document)
                break  # Exit retry loop if successful
            except (errors.AutoReconnect, errors.NetworkTimeout) as e:
                retry_count += 1
                if retry_count < retry_attempts:
                    time.sleep(retry_delay)
                else:
                    print(f"Connection error after {retry_attempts} attempts for full collection dump: {e}")
                    break
    finally:
        client.close()
    return results


if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    total_rows_written = 0
    csv_buffer = []
    all_fieldnames = set()

    # Fetch the data first to determine all possible fields
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(process_chunk, None)]
        for future in as_completed(futures):
            try:
                batch_results = future.result()
                csv_buffer.extend(batch_results)
                # Collect all unique field names
                for document in batch_results:
                    all_fieldnames.update(document.keys())
            except Exception as e:
                print(f"Error processing chunk: {e}")

    # Write the CSV file after determining all fieldnames
    if all_fieldnames:
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(all_fieldnames))
            writer.writeheader()
            writer.writerows(csv_buffer)
            total_rows_written = len(csv_buffer)
            csvfile.flush()

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    print(f"Total rows written: {total_rows_written}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
