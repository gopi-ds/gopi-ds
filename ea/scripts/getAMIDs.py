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
config = read_config(r'./config/us-east-1-jpmc-prodnam.json')  # Use raw string or double backslashes

client = MongoClient(config['MongoDB']['uri'])
db = client[config['MongoDB']['database']]
collection = db[config['MongoDB']['collection']]  # Use `db` to reference the collection

# Step 3: Define the query and projection
query = {'processing_state': {'$nin': ["Archived", "Duplicate", "Disposed", "Obsolete", "Unrecoverable", "Rejected"]}}
projection = {
    '_id': 1
}

# Step 4: Perform the query and retrieve results in batches of 1000
batch_size = 1000  # Set batch size to 1000
cursor = collection.find(query, projection).batch_size(batch_size)

# Step 5: Write the results to multiple CSV files with 100K entries per file
def write_to_csv(cursor, base_output_file, max_entries_per_file=5000):
    # Initialize variables to keep track of fieldnames and writer
    fieldnames = set()
    documents = []
    file_count = 1
    entry_count = 0

    # Function to open a new CSV file and return a writer object
    def open_new_csv(file_count):
        output_file = f"{base_output_file}_{file_count}.csv"
        csvfile = open(output_file, 'w', newline='')
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        return csvfile, writer

    # Open the first CSV file for writing
    csvfile, writer = None, None

    try:
        first_batch = True

        for document in cursor:
            documents.append(document)
            entry_count += 1

            # Determine the full set of fieldnames only from the first batch
            if first_batch:
                fieldnames.update(document.keys())
                fieldnames = sorted(fieldnames)
                csvfile, writer = open_new_csv(file_count)
                first_batch = False

            # Write the current batch of documents to the CSV file
            writer.writerow(document)

            # If the max entries per file are reached, create a new CSV file
            if entry_count >= max_entries_per_file:
                csvfile.flush()
                csvfile.close()  # Close the current file
                file_count += 1
                entry_count = 0  # Reset the entry count
                csvfile, writer = open_new_csv(file_count)  # Open a new file

    finally:
        # Flush and close the last file
        if csvfile:
            csvfile.flush()
            csvfile.close()
        # Ensure the cursor is always closed
        cursor.close()

# Specify the base output CSV file name (without file count)
base_output_csv = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\us-east-1_jpmc_migratedprodnam\\amids"

# Write the results to multiple CSV files, with 100,000 entries per file
write_to_csv(cursor, base_output_csv)

print(f"Results written to {base_output_csv}_*.csv")
