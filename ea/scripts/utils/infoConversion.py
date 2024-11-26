import json
import csv
from datetime import datetime, timezone

def convert_json_file_to_csv(input_file, output_file):
    # Read the JSON-like content from the input file
    with open(input_file, 'r') as file:
        input_data = file.read()

    # Correct any invalid starting/ending braces in the JSON string
    input_data = input_data.strip().replace('{[', '[').replace(']}', ']')

    # Convert the string to a Python object (list of dictionaries)
    data = json.loads(input_data)

    # Open the output CSV file for writing
    with open(output_file, 'w', newline='') as csvfile:
        # Initialize csv writer with the fieldnames as column names
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header (column names)
        writer.writeheader()

        # Iterate over each row in the JSON data
        for row in data:
            # Convert StartDate if it exists
            if 'StartDate' in row:
                # Convert Unix timestamp (milliseconds) to UTC timezone-aware datetime
                timestamp = int(row['StartDate']) / 1000  # Convert ms to seconds
                row['StartDate'] = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            # Write the row to the CSV file
            writer.writerow(row)

    print(f"Data has been converted and saved to {output_file}")

# Usage example
input_file = 'C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\t2_bnym_nam_uat\\only_info_Sep.json'  # Path to your input file
output_file = 'C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\t2_bnym_nam_uat\\only_info_Sep.csv'  # Path to your desired output CSV file

# Run the conversion
convert_json_file_to_csv(input_file, output_file)
