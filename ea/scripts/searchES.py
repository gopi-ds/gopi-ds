"""
    To search ES via scroll. Below program fetches gcid from index given snapshot_id

    Sample json
    {
      "Elasticsearch": {
        "host": "http://127.1.2.3:9200",
        "timeout": 3000,
        "index_pattern": "prod_data*"
      },
      "Pagination": {
        "page_size": 100
      },
      "Retry": {
        "max_retries": 5,
        "retry_delay": 5
      },
      "Output": {
        "file": "es_results.txt"
      },
      "Query": {
        "terms": {
          "_id": [
            "oUAEbpeFCXmLHkPUs6+NDYUm/FE=",
            "BlpxHO4/K9EhM32ZIDIEygKMyu4=",
            "YshVqT9oFU2N1Z5p5BHXvcjmg4U="
          ]
        },
        "fields_to_export": ["gcid"]
      }
    }
"""
from elasticsearch import Elasticsearch
import time
import json

# Read configuration from JSON file
with open('mongoConfig/elasticConfig.json', 'r') as config_file:
    config = json.load(config_file)

# Extract Elasticsearch configuration
es_host = config['Elasticsearch']['host']
timeout = config['Elasticsearch']['timeout']
index_pattern = config['Elasticsearch']['index_pattern']

# Extract pagination parameters
page_size = config['Pagination']['page_size']

# Extract retry parameters
max_retries = config['Retry']['max_retries']
retry_delay = config['Retry']['retry_delay']

# Extract output file path
output_file = config['Output']['file']

# Extract query parameters
terms = config['Query']['terms']
fields_to_export = config['Query']['fields_to_export']

# Connect to the Elasticsearch cluster
es = Elasticsearch([es_host], timeout=timeout)


def initialize_scroll(page_size):
    query = {
        "size": page_size,
        "track_total_hits": True,  # Ensures total hits are tracked for pagination purposes
        "query": {
            "terms": terms
        },
        "_source": fields_to_export,  # Adjust the fields as needed
        "sort": ["_doc"]  # Sorting by _doc for efficient scrolling
    }
    response = es.search(index=index_pattern, body=query, scroll='2m')
    return response['_scroll_id'], response['hits']['hits']


def scroll_results(scroll_id):
    response = es.scroll(scroll_id=scroll_id, scroll='2m')
    return response['_scroll_id'], response['hits']['hits']


if __name__ == "__main__":
    start_time = time.time()  # Record the start time

    # Open the file in append mode
    with open(output_file, 'a') as f:
        # Initialize the scroll
        scroll_id, hits = initialize_scroll(page_size)

        while hits:
            for hit in hits:
                f.write(str(hit['_source']) + "\n")

            for attempt in range(max_retries):
                try:
                    scroll_id, hits = scroll_results(scroll_id)
                    break  # Exit retry loop if successful
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    else:
                        print("Max retries reached. Exiting.")
                        exit(1)

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time

    print(f"All results have been written to {output_file}.")
    print(f"Time taken to complete the script: {elapsed_time:.2f} seconds")
