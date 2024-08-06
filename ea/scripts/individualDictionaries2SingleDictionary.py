"""
To convert from (individual dictionary)

    {'gcid': 'evmigration-ffaecba5-4bab-485c-8fe8-178cb1d6faf7@VerQuMessageForge1.0_1506717413000'}
    {'gcid': 'evmigration-62280e15-2d1c-4c5d-9a02-91deee03509d@VerQuMessageForge1.0_1506741899000'}
    {'gcid': 'evmigration-f79d190b-a736-40a1-9b03-5091adbf9e09@VerQuMessageForge1.0_1508534876000'}
    {'gcid': 'evmigration-a628ab17-d392-4be9-be10-163623b8ada6@VerQuMessageForge1.0_1508506096000'}
    {'gcid': 'evmigration-cb52eba3-448c-41fb-ab62-cfa661bfb7b3@VerQuMessageForge1.0_1508498723000'}

To this (single dictionary)
    {
        "gcids": [
            "evmigration-ffaecba5-4bab-485c-8fe8-178cb1d6faf7@VerQuMessageForge1.0_1506717413000",
            "evmigration-62280e15-2d1c-4c5d-9a02-91deee03509d@VerQuMessageForge1.0_1506741899000",
            "evmigration-f79d190b-a736-40a1-9b03-5091adbf9e09@VerQuMessageForge1.0_1508534876000",
            "evmigration-a628ab17-d392-4be9-be10-163623b8ada6@VerQuMessageForge1.0_1508506096000",
            "evmigration-cb52eba3-448c-41fb-ab62-cfa661bfb7b3@VerQuMessageForge1.0_1508498723000"
        ]
    }

IYKYK
"""
import json
import re


# Function to correct JSON formatting by adding double quotes when needed
def correct_json_formatting(line):
    # Replace single quotes around keys and values with double quotes
    line = re.sub(r"(?<!\")(\b\w+\b)(?=\s*:)", r'"\1"', line)  # Add double quotes around keys
    line = re.sub(r"':\s*'([^']*)'", r'": "\1"', line)  # Replace single quotes around values with double quotes
    line = line.replace("'", '"')  # Replace any remaining single quotes with double quotes
    return line


# File paths
input_file = 'es_results.json'
output_file = 'es_results_output.json'

input_data = []

# Use with statement to ensure the file is properly closed
try:
    with open(input_file, 'r') as infile:
        for line in infile:
            try:
                corrected_line = correct_json_formatting(line.strip())
                input_data.append(json.loads(corrected_line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON on line: {line.strip()}")
                print(f"Corrected line: {corrected_line}")
                print(f"Error: {e}")
except FileNotFoundError:
    print(f"Input file {input_file} not found.")

if input_data:
    # Extract gcid values and combine into a single dictionary with a list of gcids
    output_data = {"gcid": [item["gcid"] for item in input_data]}

    # Print the result
    #print(output_data)

    # Write the output to a file using with statement
    with open(output_file, 'w') as outfile:
        json.dump(output_data, outfile, indent=4)

    print(f"All results have been written to {output_file}.")
else:
    print("No valid input data found.")
