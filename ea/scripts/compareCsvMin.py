"""

    This script is written while working on SINC-16012/SINC-16410/SINC-16327
    to compare retention_policy_map collection before and after reingestion

    This script compares ~1-3million retention_policy_map collection entries of 2 files,
    - sorts it
    - reads and holds a dictionary of gcid, policyId as key-value pair in-memory
        for both before and after files
    - from a chunk of chunks of gcid's, starts comparing key-value pairs in both dictionaries
    - holds differences in a collection (in-memory)
    - replaces policyId values with corresponding names in the collection
    - as a final step, writes the differences into a CSV with these columns
        [gcId]
        [policies-before-reingestion]
        [policies-after-reingestion]
        [net-new-policies-assigned]
        [count-of-net-new-policies-assigned]

    Approximate completion time ~0.5-1hr based on the available machine memory.

"""

import csv
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import shutil

# Input file from pre-re-ingestion
before_file = "before_retention_policy_map.csv"

# Reference file from post-re-ingestion
after_file = "after_retention_policy_map.csv"

# File to write the differences
differences_file = "all_differences.csv"

retention_policies_csv = 'rps.csv'
retention_policies_columns = ['policyId', 'name']  # Replace with your actual column names

# Fields to sort by
sort_fields = ["gcId", "policyId", "channel", "clusterId", "network"]

# Field to compare
key_field = "gcId"
value_field = "policyId"

# Batch size for processing
batch_size = 1000

# Maximum number of threads
max_threads = 10

def sort_csv_file_in_place(file_path, sort_fields):
    start_time = time.time()
    print(f"Sorting {file_path} based on {sort_fields}...")
    # Create a temporary file to store the sorted data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='') as tmpfile:
        with open(file_path, 'r') as infile:
            reader = csv.DictReader(infile)
            sorted_list = sorted(reader, key=lambda row: tuple(row[field] for field in sort_fields))
            writer = csv.DictWriter(tmpfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(sorted_list)
    # Replace the original file with the sorted file
    shutil.move(tmpfile.name, file_path)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Sorting {file_path} completed in {elapsed_time:.2f} seconds.")

def read_csv_to_dict(file_path, key_field, value_field):
    start_time = time.time()
    print(f"Reading {file_path} into dictionary...")
    data = defaultdict(list)
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            key = row[key_field]
            value = row[value_field]
            data[key].append(value)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Reading {file_path} completed in {elapsed_time:.2f} seconds.")
    return data

def batch_keys(data, batch_size):
    keys = list(data.keys())
    for i in range(0, len(keys), batch_size):
        yield keys[i:i + batch_size]

def compare_batches(batch_keys, data1, data2):
    differences = []

    for key in batch_keys:
        values1 = sorted(data1.get(key, []))
        values2 = sorted(data2.get(key, []))
        differing_values = []
        count = 0

        # Find values in values1 not in values2
        #for value1 in values1:
        #    if value1 not in values2:
        #        differing_values.append(value1)
        #        count += 1

        # Find values in values2 not in values1
        for value2 in values2:
            if value2 not in values1:
                differing_values.append(value2)
                count += 1

        if differing_values:
            differences.append({
                "gcId": key,
                "policies-before-reingestion": values1,
                "policies-after-reingestion": values2,
                "net-new-policies-assigned": differing_values,
                "count-of-net-new-policies-assigned": count
            })

    return differences

def write_differences_to_csv(differences, differences_file):
    start_time = time.time()
    print(f"Writing differences to {differences_file}...")
    with open(differences_file, 'w', newline='') as csvfile:
        fieldnames = ["gcId", "policies-before-reingestion", "policies-after-reingestion", "net-new-policies-assigned", "count-of-net-new-policies-assigned"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for diff in differences:
            diff["net-new-policies-assigned"] = str(diff["net-new-policies-assigned"])  # Convert list to string for CSV
            writer.writerow(diff)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Differences written to {differences_file} in {elapsed_time:.2f} seconds.")

# Function to read policyId and name into a dictionary
def read_policies_csv_to_dict(file_path):
    start_time = time.time()
    print(f"Reading {file_path} into dictionary...")
    result = {}
    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            result[row['policyId']] = row['name']
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Reading {file_path} completed in {elapsed_time:.2f} seconds.")
    return result

# Function to replace policyId with corresponding names in the differences list
def replace_policy_ids_with_names(differences, policy_dict):
    fields_to_replace = ["policies-before-reingestion", "policies-after-reingestion", "net-new-policies-assigned"]
    for diff in differences:
        for field in fields_to_replace:
            diff[field] = [policy_dict.get(pid, pid) for pid in diff[field]]
    return differences

if __name__ == "__main__":
    try:
        # Sort the files before processing
        sort_csv_file_in_place(before_file, sort_fields)
        sort_csv_file_in_place(after_file, sort_fields)
        retentionPoliciesDict = read_policies_csv_to_dict(retention_policies_csv)
        beforeDict = read_csv_to_dict(before_file, key_field, value_field)
        afterDict = read_csv_to_dict(after_file, key_field, value_field)

        all_differences = []

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            print("Starting comparison in batches...")
            start_time = time.time()
            # Submit batches to the executor
            futures = {executor.submit(compare_batches, batch, beforeDict, afterDict): batch for batch in batch_keys(beforeDict, batch_size)}
            for future in as_completed(futures):
                try:
                    batch_differences = future.result()
                    all_differences.extend(batch_differences)
                except Exception as e:
                    print(f"Error processing batch: {e}")
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Batch comparison completed in {elapsed_time:.2f} seconds.")

        if all_differences:
            # Replace policyId with corresponding names
            all_differences = replace_policy_ids_with_names(all_differences, retentionPoliciesDict)
            write_differences_to_csv(all_differences, differences_file)
            print("Differences found and written to", differences_file)
        else:
            print(f"The files are identical for the field '{key_field}' and '{value_field}'.")
    finally:
        # Ensure that all temporary files are cleaned up
        if tempfile.NamedTemporaryFile:
            tempfile.NamedTemporaryFile().close()