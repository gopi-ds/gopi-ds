"""
    fetch contents from Mongo

    Sample json
    {
        "mongodb_uri": "mongodb://",
        "database": "natwestprod",
        "collection": "retention_policies"
    }
"""
import json
from jsmin import jsmin
from pymongo import MongoClient
import csv

# Step 1: Read the configuration file
def read_config(file_path):
    with open(file_path, 'r') as file:
        minified_json = jsmin(file.read())
        config = json.loads(minified_json)
    return config

# Step 2: Connect to MongoDB using details from the config file
config = read_config(r'./config/old-us-east-1-bnym-nam.json')  # Use raw string or double backslashes

client = MongoClient(config['MongoDB']['uri'])
db = client[config['MongoDB']['database']]
#collection = db[config['MongoDB']['collection']]
collection = db['retention_policies']

# Step 3: Define the query and projection
query = {}
#query = {'enabled': True}
projection = {
    'policyId': 1,
    'name': 1,
    'sourceType': 1,
    'category': 1,
    'storeType': 1,
    'retentionPeriod': 1,
    'unitOfPeriod': 1,
    'expiryAction': 1,
    'scope': 1,
    'createdDate': 1,
    'lastUpdated': 1,
    '_id': 0  # Exclude the _id field
}

# Step 4: Perform the query and sort the results
#sort_criteria = [('unitOfPeriod', 1), ('retentionPeriod', 1)]
#results = collection.find(query, projection).sort(sort_criteria)
results = collection.find(query, projection)

# Step 5: Write the results to a CSV file
def write_to_csv(results, output_file):
    # Initialize variables to keep track of fieldnames and writer
    fieldnames = set()
    documents = []

    # Collect all documents and determine the full set of fieldnames
    for document in results:
        fieldnames.update(document.keys())
        documents.append(document)

    # Convert the set of fieldnames to a sorted list for consistent ordering
    fieldnames = sorted(fieldnames)

    # Open the CSV file for writing
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Write each document, ensuring all fields are included
        for document in documents:
            writer.writerow(document)

# Specify the output CSV file
output_csv = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\BNY\\ISSv2\\BNY_NAM_SINC_16852\\bnymprodnam_rps.csv"

# Write the results to the CSV file
write_to_csv(results, output_csv)

#print(f"Results written to {output_csv}")
print(f"Results written to {output_csv}")
