import csv
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError, RequestError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Read configuration from JSON file
with open('config/fetchAllFromEs.json', 'r') as config_file:
    config = json.load(config_file)

# Extract Elasticsearch configuration
es_host = config['Elasticsearch']['host']
request_timeout = config['Elasticsearch']['timeout']
index_pattern = config['Elasticsearch']['index_pattern']

# Extract pagination parameters
page_size = config['Pagination']['page_size']

# Extract retry parameters
max_retries = config['Retry']['max_retries']
retry_delay = config['Retry']['retry_delay']

# Extract output file path (CSV file now)
output_file = config['Output']['file'].replace(".txt", ".csv")

# Extract fields to export
fields_to_export = config['Query']['fields_to_export']

# Connect to the Elasticsearch cluster
es = Elasticsearch([es_host], request_timeout=request_timeout)

def initialize_scroll(slice_id, total_slices):
    """
    Initializes a scroll context for a specific slice in Elasticsearch.
    Returns the scroll_id and the first batch of hits for that slice.
    """
    query = {
        "slice": {
            "id": slice_id,
            "max": total_slices
        },
        "track_total_hits": True,
        "query": {
            "match_all": {}  # Match all documents in the index
        },
        "_source": fields_to_export,  # Fetch only the fields we want to export
        "sort": ["_doc"]
    }

    for attempt in range(max_retries):
        try:
            response = es.search(index=index_pattern, body=query, scroll='5m', size=page_size)
            return response['_scroll_id'], response['hits']['hits'], response['hits']['total']['value']
        except (ConnectionError, TransportError, RequestError, NotFoundError) as e:
            print(f"Slice {slice_id}: Attempt {attempt + 1} to initialize scroll failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print(f"Slice {slice_id}: Max retries reached during initialization. Exiting.")
                return None, [], 0


def scroll_slice(slice_id, total_slices):
    """
    Fetches all results for a given slice using the scroll API.
    This runs in parallel for each slice.
    """
    scroll_id, hits, total_hits = initialize_scroll(slice_id, total_slices)
    if not scroll_id:
        return 0

    fetched_records = 0

    with open(f"{output_file}_{slice_id}.csv", 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields_to_export)  # Write the header for each slice's file

        while hits:
            for hit in hits:
                row = [hit['_source'].get(field, "") for field in fields_to_export]
                csvwriter.writerow(row)
                fetched_records += 1

                # Log progress every 100,000 records
                if fetched_records % 100000 == 0:
                    print(f"Slice {slice_id}: Fetched {fetched_records} / {total_hits} records")

            scroll_id, hits = scroll_results(slice_id, scroll_id)

    print(f"Slice {slice_id}: Finished fetching {fetched_records} records.")
    return fetched_records


def scroll_results(slice_id, scroll_id):
    """
    Fetches the next batch of results for a slice using the scroll API.
    """
    for attempt in range(max_retries):
        try:
            response = es.scroll(scroll_id=scroll_id, scroll='5m')
            return response['_scroll_id'], response['hits']['hits']
        except (ConnectionError, TransportError, RequestError, NotFoundError) as e:
            print(f"Slice {slice_id}: Attempt {attempt + 1} to scroll failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print(f"Slice {slice_id}: Max retries reached during scrolling. Exiting.")
                return None, []


if __name__ == "__main__":
    start_time = time.time()

    # Number of slices (adjust based on your environment and number of threads)
    total_slices = 10  # Can be increased depending on cluster size and workload
    max_threads = total_slices

    # Use ThreadPoolExecutor to parallelize fetching from multiple slices
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(scroll_slice, slice_id, total_slices) for slice_id in range(total_slices)]

        # Wait for all futures to complete
        total_fetched_records = 0
        for future in as_completed(futures):
            total_fetched_records += future.result()

    # Merge the individual slice files into a single CSV (optional)
    with open(output_file, 'w', newline='') as final_csvfile:
        csvwriter = csv.writer(final_csvfile)
        csvwriter.writerow(fields_to_export)  # Write header once in the final file

        for slice_id in range(total_slices):
            slice_file = f"{output_file}_{slice_id}.csv"
            with open(slice_file, 'r') as slice_csvfile:
                reader = csv.reader(slice_csvfile)
                next(reader)  # Skip header in each slice file
                for row in reader:
                    csvwriter.writerow(row)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"All records have been written to {output_file}.")
    print(f"Total fetched records: {total_fetched_records}")
    print(f"Time taken to complete the script: {elapsed_time:.2f} seconds")
