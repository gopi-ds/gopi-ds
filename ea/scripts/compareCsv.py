"""

    This script is written while working on SINC-16012/SINC-16410/SINC-16327
    to compare retention_policy_map collection before and after reingestion

    This script compares ~30million retention_policy_map collection entries of 2 files,
    - sorts it using merge sort technique
    - reads the sorted before and after files and breaks into chunks of dictionary,
        saves them to disk as temp files rather than holding in memory
    - iteratively, reads a chunk of before temp file and starts comparing against
        each chunk of after temp file for a batch of gcid
    - writes the differences to file on disk with these columns
        [gcId]
        [policies-before-reingestion]
        [policies-after-reingestion]
        [net-new-policies-assigned]
        [count-of-net-new-policies-assigned]
    - as a final step, replaces policyId values with corresponding name

    ## Input params ##
    before_file
        - retention_policy_map collection before reingestion
    after_file
        - retention_policy_map collection after reingestion
    retention_policies_csv
        - retention_policies collection for reference to replace
        policyId values with corresponding policy name
    differences_file
        - output CSV

    Approximate completion time ~4-6hrs based on the available machine memory

"""

import csv
import os
import heapq
import tempfile
import shutil
import time
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Input files
before_file = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\BNY\\ISSv2\\BNY_APAC_SINC_16837\\SINC-16837__BNYM_PRODAPAC__Pre_Ingestion__Retention_Report.csv"
after_file = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\BNY\\ISSv2\\BNY_APAC_SINC_16837\\SINC-16837__BNYM_PRODAPAC__Post_Ingestion__Retention_Report.csv"
retention_policies_csv = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\BNY\\ISSv2\BNY_APAC_SINC_16837\\bnym_prod_apac_rps.csv"
differences_file_base = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\BNY\\ISSv2\\BNY_APAC_SINC_16837\\SINC-16837__BNYM_PRODAPAC__Post_Ingestion__Retention_Report_Differences.csv"
differences_file_ext = ".csv"

# Fields to sort by
sort_fields = ["gcId", "policyId", "channel", "clusterId", "network"]

# Comparison fields
key_field = "gcId"
value_field = "policyId"


# Batch size for processing
batch_size = 100000  # Adjust based on available memory
chunk_size = 5000
max_workers = 50
max_lines_per_file = 1048566  # Limit on MS Excel to load CSV

# Initialize logging
logging.basicConfig(filename='compareCsv.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def log_time(message, start_time):
    elapsed_time = time.time() - start_time
    logging.info(f"{message} completed in {elapsed_time:.2f} seconds.")

def external_sort_csv(file_path, sort_fields, chunk_size, max_workers):
    temp_files = []
    futures = []
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with open(file_path, 'r', newline='') as infile:
            reader = csv.DictReader(infile)
            headers = reader.fieldnames
            current_chunk = []
            for row in reader:
                current_chunk.append(row)
                if len(current_chunk) >= chunk_size:
                    futures.append(executor.submit(sort_and_save_chunk, current_chunk, headers, sort_fields))
                    current_chunk = []
            if current_chunk:
                futures.append(executor.submit(sort_and_save_chunk, current_chunk, headers, sort_fields))

        for future in as_completed(futures):
            temp_files.append(future.result())

    log_time(f"Sorting {file_path}", start_time)

    merge_sorted_chunks(temp_files, file_path, headers, sort_fields)

    # Clean up temporary files
    for temp_file in temp_files:
        try:
            os.remove(temp_file)
        except OSError as e:
            logging.error(f"Error removing temporary file {temp_file}: {e}")

def sort_and_save_chunk(chunk, headers, sort_fields):
    chunk.sort(key=lambda row: tuple(row[field] for field in sort_fields))
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    with open(temp_file.name, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(chunk)
    return temp_file.name

def merge_sorted_chunks(temp_files, output_file, headers, sort_fields):
    start_time = time.time()
    heap = []
    file_pointers = [open(temp_file, 'r', newline='') for temp_file in temp_files]
    readers = [csv.DictReader(fp) for fp in file_pointers]
    for reader_idx, reader in enumerate(readers):
        try:
            row = next(reader)
            heapq.heappush(heap, (tuple(row[field] for field in sort_fields), reader_idx, row))
        except StopIteration:
            pass

    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        while heap:
            _, reader_idx, row = heapq.heappop(heap)
            writer.writerow(row)
            try:
                next_row = next(readers[reader_idx])
                heapq.heappush(heap, (tuple(next_row[field] for field in sort_fields), reader_idx, next_row))
            except StopIteration:
                pass

    for fp in file_pointers:
        fp.close()

    log_time(f"Merging sorted chunks for {output_file}", start_time)

def read_csv_to_temp_files(file_path, key_field, value_field, chunk_size):
    temp_files = []
    start_time = time.time()
    logging.info(f"Reading {file_path} into temporary files...")
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        current_chunk = defaultdict(list)
        count = 0
        for row in reader:
            key = row[key_field]
            value = row[value_field]
            current_chunk[key].append(value)
            count += 1
            if count >= chunk_size:
                temp_files.append(write_dict_to_temp_file(current_chunk))
                current_chunk = defaultdict(list)
                count = 0
        if current_chunk:
            temp_files.append(write_dict_to_temp_file(current_chunk))
    log_time(f"Reading {file_path} into temporary files", start_time)
    return temp_files

def write_dict_to_temp_file(data_dict):
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    with open(temp_file.name, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for key, values in data_dict.items():
            for value in values:
                writer.writerow([key, value])
    return temp_file.name

def read_temp_file_to_dict(temp_file_path):
    data = defaultdict(list)
    with open(temp_file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            key, value = row
            data[key].append(value)
    return data

def batch_keys(data, batch_size):
    keys = list(data.keys())
    for i in range(0, len(keys), batch_size):
        yield keys[i:i + batch_size]

def write_differences_to_csv(differences, file_counter, line_counter):
    file_path = f"{differences_file_base}_{file_counter}{differences_file_ext}"
    with open(file_path, 'a', newline='') as csvfile:
        fieldnames = ["gcId", "policies-before-reingestion", "policies-after-reingestion", "net-new-policies-assigned", "count-of-net-new-policies-assigned"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        for diff in differences:
            diff["net-new-policies-assigned"] = str(diff["net-new-policies-assigned"])  # Convert list to string for CSV
            writer.writerow(diff)
            line_counter[0] += 1
            if line_counter[0] >= max_lines_per_file:
                file_counter[0] += 1
                line_counter[0] = 0
    return file_counter, line_counter

def compare_batches(batch_keys, data1, data2, file_counter, line_counter):
    differences = []
    for key in batch_keys:
        values1 = sorted(data1.get(key, []))
        values2 = sorted(data2.get(key, []))
        differing_values = []
        count = 0

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

    file_counter, line_counter = write_differences_to_csv(differences, file_counter, line_counter)

def read_policies_csv_to_dict(file_path):
    start_time = time.time()
    logging.info(f"Reading {file_path} into dictionary...")
    result = {}
    with open(file_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            result[row['policyId']] = row['name']
    log_time(f"Reading {file_path}", start_time)
    return result

def replace_policy_ids_in_chunk(chunk, policy_dict):
    updated_chunk = []
    for row in chunk:
        for field in ["policies-before-reingestion", "policies-after-reingestion", "net-new-policies-assigned"]:
            row[field] = [policy_dict.get(pid, pid) for pid in eval(row[field])]
        updated_chunk.append(row)
    return updated_chunk

def replace_policy_ids_with_names(differences_file, policy_dict, chunk_size, max_workers):
    start_time = time.time()
    logging.info(f"Replacing policy IDs with names in {differences_file} in chunks...")
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    with open(differences_file, 'r', newline='') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        with open(temp_file.name, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            chunk = []
            futures = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for row in reader:
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        futures.append(executor.submit(replace_policy_ids_in_chunk, chunk, policy_dict))
                        chunk = []
                if chunk:
                    futures.append(executor.submit(replace_policy_ids_in_chunk, chunk, policy_dict))

                for future in as_completed(futures):
                    updated_chunk = future.result()
                    writer.writerows(updated_chunk)

    temp_file.close()
    infile.close()
    outfile.close()
    shutil.move(temp_file.name, differences_file)
    log_time(f"Replacing policy IDs in {differences_file}", start_time)

if __name__ == "__main__":
    before_temp_files = []
    after_temp_files = []
    file_counter = [1]
    line_counter = [0]
    try:
        start_time = time.time()
        # Sort the files before processing using external sort
        external_sort_csv(before_file, sort_fields, batch_size, max_workers)
        external_sort_csv(after_file, sort_fields, batch_size, max_workers)

        log_time("Sorting both files", start_time)

        start_time = time.time()
        retentionPoliciesDict = read_policies_csv_to_dict(retention_policies_csv)

        # Process the CSV files in chunks and store intermediate results in temp files
        before_temp_files = read_csv_to_temp_files(before_file, key_field, value_field, batch_size)
        after_temp_files = read_csv_to_temp_files(after_file, key_field, value_field, batch_size)

        # Initialize the differences file with header
        with open(f"{differences_file_base}_{file_counter[0]}{differences_file_ext}", 'w', newline='') as csvfile:
            fieldnames = ["gcId", "policies-before-reingestion", "policies-after-reingestion", "net-new-policies-assigned", "count-of-net-new-policies-assigned"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

        # Ensure the file is closed after writing the header
        csvfile.close()

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            logging.info("Starting comparison in batches...")
            start_time = time.time()
            # Submit batches to the executor
            futures = []
            for before_temp_file in before_temp_files:
                before_dict = read_temp_file_to_dict(before_temp_file)
                for after_temp_file in after_temp_files:
                    after_dict = read_temp_file_to_dict(after_temp_file)
                    for batch in batch_keys(before_dict, batch_size):
                        futures.append(executor.submit(compare_batches, batch, before_dict, after_dict, file_counter, line_counter))

            for future in as_completed(futures):
                try:
                    future.result()  # Ensure any exceptions are raised
                    logging.info("Batch comparison completed successfully.")
                except Exception as e:
                    logging.error(f"Error processing batch: {e}")
            log_time("Batch comparison", start_time)

        # Ensure the file is closed before reopening it in replace_policy_ids_with_names
        replace_policy_ids_with_names(f"{differences_file_base}_{file_counter[0]}{differences_file_ext}", retentionPoliciesDict, chunk_size, max_workers)
        logging.info(f"Differences found and written to {differences_file_base}_{file_counter[0]}{differences_file_ext}")

    finally:
        # Ensure that all temporary files are cleaned up
        start_time = time.time()
        for temp_file in before_temp_files + after_temp_files:
            try:
                os.remove(temp_file)
            except OSError as e:
                logging.error(f"Error removing temporary file {temp_file}: {e}")
        log_time("Cleaning up temporary files", start_time)
