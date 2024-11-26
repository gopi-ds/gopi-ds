"""

    This script is written while working on SINC-16012/SINC-16410/SINC-16327
    to dump contents of retention_policy_map collection before and after reingestion
    based on the provided gcid's

    The following columns/fields are fetched and put into CSV

    [gcId] [policyId] [channel] [clusterId] [network]

    Sample json
    {
      "MongoDB": {
        "uri": "mongodb://",
        "database": "prodnam",
        "collection": "retention_policy_map",
        "connection_timeout": 6000000,
        "socket_timeout": 60000000
      },
      "Files": {
        "input_file": "<input file of gcid's to fetch>",
        "output_file": "<output csv to write to>"
      },
      "Processing": {
        "max_threads": 10,
        "mongo_batch_size": 1024,
        "csv_buffer_size": 10240,
        "retry_attempts": 100,
        "retry_delay": 5
      },
      "Fields": {
        "fields_to_export": ["gcId", "policyId", "channel", "clusterId", "network"],
        "field_to_filter": "gcId"
      }
    }

    Approximate completion time ~25mins to fetch ~10million records

"""

import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient, errors

# Read configuration from JSON file
with open('config/eu-west-2-natwest.json', 'r') as config_file:
    config = json.load(config_file)

# Extract MongoDB configuration
mongo_uri = config['MongoDB']['uri']
database_name = config['MongoDB']['database']
collection_name = config['MongoDB']['collection']
connection_timeout = config['MongoDB']['connection_timeout']
socket_timeout = config['MongoDB']['socket_timeout']

# Extract file paths
input_file = config['Files']['input_file']
output_file = config['Files']['output_file']

# Extract processing configuration
max_threads = config['Processing']['max_threads']
mongo_batch_size = config['Processing']['mongo_batch_size']
csv_buffer_size = config['Processing']['csv_buffer_size']
retry_attempts = config['Processing']['retry_attempts']
retry_delay = config['Processing']['retry_delay']

# Extract field configuration
fields_to_export = config['Fields']['fields_to_export']
field_to_filter = config['Fields']['field_to_filter']


def process_chunk(values_chunk):
    results = []
    client = MongoClient(mongo_uri, maxPoolSize=max_threads, connectTimeoutMS=connection_timeout,
                         socketTimeoutMS=socket_timeout)
    try:
        db = client[database_name]
        collection = db[collection_name]
        query_condition = {field_to_filter: {"$in": values_chunk}}

        retry_count = 0
        while retry_count < retry_attempts:
            try:
                cursor = collection.find(query_condition, {field: 1 for field in fields_to_export}).batch_size(
                    mongo_batch_size)
                for document in cursor:
                    results.append({field: document.get(field, "") for field in fields_to_export})
                break  # Exit retry loop if successful
            except (errors.AutoReconnect, errors.NetworkTimeout) as e:
                retry_count += 1
                if retry_count < retry_attempts:
                    time.sleep(retry_delay)
                else:
                    print(
                        f"Connection error after {retry_attempts} attempts for query condition {query_condition}: {e}")
                    break
    finally:
        client.close()
    return results


def process_values_in_chunks(file_path, chunk_size=10240):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            chunk = []
            for line in file:
                value = line.strip()
                if value:
                    chunk.append(value)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None


if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    chunks = list(process_values_in_chunks(input_file))
    if not chunks:
        print(f"No data to process from {input_file}")
        exit(1)

    total_input_lines = sum(len(chunk) for chunk in chunks)

    total_rows_written = 0
    total_rows_processed = 0
    csv_buffer = []

    with open(output_file, "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields_to_export)
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(process_chunk, chunk): chunk for chunk in chunks}
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    total_rows_processed += len(futures[future])
                    csv_buffer.extend(batch_results)
                    if len(csv_buffer) >= csv_buffer_size:
                        writer.writerows(csv_buffer)
                        rows_written = len(csv_buffer)
                        total_rows_written += rows_written
                        csvfile.flush()  # Flush the CSV file buffer
                        print(f"GCID's processed so far: {total_rows_processed}")
                        csv_buffer = []  # Clear buffer
                except Exception as e:
                    print(f"Error processing chunk: {e}")

        # Write any remaining data in the buffer
        if csv_buffer:
            writer.writerows(csv_buffer)
            rows_written = len(csv_buffer)
            total_rows_written += rows_written
            csvfile.flush()
            print(f"Rows written for this buffer: {rows_written}")

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time

    print(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"Data exported to {output_file}")
    print(f"Total count of GCID's provided: {total_input_lines}")
    print(f"Total count of GCID's processed: {total_rows_processed}")
    print(f"Total count of retention_policy_map entries written: {total_rows_written}")
    print(f"Time taken to complete the script: {elapsed_time:.2f} seconds")
    print(f"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
