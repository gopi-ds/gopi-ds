from elasticsearch import Elasticsearch
import json

# Function to load gcid values from a JSON file
def load_gcids_from_json(file_path):
    with open(file_path, 'r') as file:
        gcid_list = json.load(file)
    return gcid_list

# File paths
gcid_file = "./config/gcid_es_list.json"  # Update the path to your file

# Connect to Elasticsearch
try:
    es = Elasticsearch(['http://10.32.17.214:9200'], request_timeout=30)

    if not es.ping():
        raise ValueError("Connection failed")

    # Load gcid values from file
    id_list = load_gcids_from_json(gcid_file)

    # Function to batch and query Elasticsearch
    def batch_query(es, index, ids, batch_size=1000):
        results = []
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            response = es.search(index=index, body={
                "query": {
                    "terms": {
                        "gcid": batch_ids
                    }
                }
            })
            results.extend(response['hits']['hits'])
        return results

    # Query in batches
    all_results = batch_query(es, "dtccprod*", id_list, batch_size=1000)

    # Write the combined results to a file
    with open('./results/gcid_es_results.json', 'w') as file:
        json.dump(all_results, file, indent=2)

    print("Results written to gcid_es_results.json")

except Exception as e:
    print(f"An error occurred: {e}")
