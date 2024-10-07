import os
import csv
import json

# Define the folder containing the input files
input_folder = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\us-east-1_jpmc_migratedprodnam\\fromMongo\\"
output_folder = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\us-east-1_jpmc_migratedprodnam\\toIC2\\"

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

def process_csv_file(file_path, base_output_path, chunk_size=1000):
    """
    Read a CSV file containing _id values, convert them to the JSON format, and save it to the output file.
    Create a new file for every 1,000 entries.

    :param file_path: Path to the input CSV file.
    :param base_output_path: Base path for the output JSON files.
    :param chunk_size: Number of entries per JSON file.
    """
    try:
        ids = []
        file_count = 1  # Counter to track the number of output files

        # Open the CSV file and read the _id column
        with open(file_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            if '_id' not in reader.fieldnames:
                raise ValueError(f"Missing '_id' column in {file_path}")

            for row in reader:
                ids.append(row['_id'].strip())

                # When we hit the chunk size (1,000 entries), write to a new JSON file
                if len(ids) >= chunk_size:
                    output_path = f"{base_output_path}_{file_count}.json"
                    save_to_json(ids, output_path)
                    print(f"Processed chunk {file_count} from {file_path} and saved to {output_path}")
                    file_count += 1
                    ids = []  # Reset the list for the next chunk

            # Write any remaining entries (if less than 1,000)
            if ids:
                output_path = f"{base_output_path}_{file_count}.json"
                save_to_json(ids, output_path)
                print(f"Processed final chunk {file_count} from {file_path} and saved to {output_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def save_to_json(ids, output_path):
    """
    Save a list of _id values to a JSON file.
    :param ids: List of _id values.
    :param output_path: Path to the output JSON file.
    """
    data = {
        "archiveMetricIds": ids
    }

    # Convert the dictionary to JSON and save it to the output file
    with open(output_path, 'w') as output_file:
        json.dump(data, output_file, indent=4)

# Iterate over all CSV files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".csv"):  # Process only CSV files
        input_file_path = os.path.join(input_folder, filename)
        output_file_base_path = os.path.join(output_folder, os.path.splitext(filename)[0])
        process_csv_file(input_file_path, output_file_base_path)

print("Conversion completed for all files.")
