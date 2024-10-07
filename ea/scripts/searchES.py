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
from elasticsearch.exceptions import ConnectionError, TransportError, NotFoundError, RequestError
import time
import json

# Read configuration from JSON file
with open('config/esConfig_bnymnamuat.json', 'r') as config_file:
    config = json.load(config_file)

# Extract Elasticsearch configuration
es_host = config['Elasticsearch']['host']
request_timeout = config['Elasticsearch']['request_timeout']
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
es = Elasticsearch([es_host], request_timeout=request_timeout)

def initialize_scroll():
    query = {
        "track_total_hits": True,
        "query": {
            "terms": terms
        },
        "_source": fields_to_export,
        "sort": ["_doc"]
    }

    for attempt in range(max_retries):
        try:
            response = es.search(index=index_pattern, body=query, scroll='2m')
            return response['_scroll_id'], response['hits']['hits']
        except (ConnectionError, TransportError, RequestError, NotFoundError) as e:
            print(f"Attempt {attempt + 1} to initialize scroll failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("Max retries reached during initialization. Exiting.")
                exit(1)


def scroll_results(scroll_id):
    for attempt in range(max_retries):
        try:
            response = es.scroll(scroll_id=scroll_id, scroll='2m')
            return response['_scroll_id'], response['hits']['hits']
        except (ConnectionError, TransportError, RequestError, NotFoundError) as e:
            print(f"Attempt {attempt + 1} to scroll failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("Max retries reached during scrolling. Exiting.")
                exit(1)


if __name__ == "__main__":
    start_time = time.time()

    with open(output_file, 'w') as jsonfile:
        jsonfile.write("[")  # Start the JSON array

        # Initialize the scroll
        scroll_id, hits = initialize_scroll()

        first_record = True
        while hits:
            for hit in hits:
                if not first_record:
                    jsonfile.write(",\n")
                else:
                    first_record = False
                json.dump(hit['_source'], jsonfile)

            scroll_id, hits = scroll_results(scroll_id)

        jsonfile.write("]")  # End the JSON array

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"All results have been written to {output_file}.")
    print(f"Time taken to complete the script: {elapsed_time:.2f} seconds")
