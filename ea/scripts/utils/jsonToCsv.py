import csv
import json
import re

def clean_data(data):
    # Replace MongoDB-specific types with standard JSON types
    data = re.sub(r'NumberInt\((\d+)\)', r'\1', data)  # Replace NumberInt(5) with 5
    data = re.sub(r'ISODate\("([^"]+)"\)', r'"\1"', data)  # Replace ISODate("...") with "..."
    return data

def read_data_from_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read().strip()

    # Split the content by each JSON object
    json_objects = re.findall(r'\{[^}]+\}', content)
    parsed_objects = []

    for obj in json_objects:
        cleaned_obj = clean_data(obj)
        parsed_objects.append(json.loads(cleaned_obj))

    return parsed_objects

# Specify the input and output files
input_file = 'results/data.txt'
output_file = 'results/data_out.csv'

# Read and parse the data
data = read_data_from_file(input_file)

# Define the CSV column names
fieldnames = [
    "retentionPeriod",
    "unitOfPeriod",
    "expiryAction",
    "storeType",
    "sourceType",
    "name",
    "category",
    "createdDate",
    "scope",
    "lastUpdated",
    'enabled'
]

# Write the parsed data to a CSV file
with open(output_file, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write the data rows
    writer.writerows(data)

output_file
