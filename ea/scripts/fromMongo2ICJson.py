import os
import csv
import json

# Define the folder containing the input files
input_folder = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\us-east-1_jpmc_migratedprodnam\\fromMongo\\"
output_folder = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\us-east-1_jpmc_migratedprodnam\\toIC\\"

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def process_csv_file(file_path, output_path):
    """
    Read a CSV file containing _id values, convert them to the JSON format, and save it to the output file.
    :param file_path: Path to the input CSV file.
    :param output_path: Path to the output JSON file.
    """
    try:
        ids = []

        # Open the CSV file and read the _id column
        with open(file_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Assuming the CSV has a column named '_id'
                ids.append(row['_id'].strip())

        # Create the dictionary structure
        data = {
            "archiveMetricIds": ids
        }

        # Convert the dictionary to JSON and save it to the output file
        with open(output_path, 'w') as output_file:
            json.dump(data, output_file, indent=4)

        print(f"Processed {file_path} and saved to {output_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Iterate over all CSV files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):  # Process only CSV files
        input_file_path = os.path.join(input_folder, filename)
        output_file_path = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.json")
        process_csv_file(input_file_path, output_file_path)

print("Conversion completed for all files.")
