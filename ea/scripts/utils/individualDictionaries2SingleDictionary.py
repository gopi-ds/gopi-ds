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

# Function to correct JSON formatting by adding necessary brackets
def correct_json_formatting(content):
    content = content.strip().strip(',')  # Remove any leading/trailing whitespace or stray commas
    content = "[" + content + "]"  # Wrap the entire content in brackets to form a valid JSON array
    return content

# File paths
input_file = "C:\\Users\\saigopinath.dokku\\Downloads\\gcid_es_list.txt"
output_file = 'gcid_es_list.json'

# Read the entire content of the file
try:
    with open(input_file, 'r') as infile:
        content = infile.read()
        corrected_content = correct_json_formatting(content)

        try:
            input_data = json.loads(corrected_content)  # Parse the corrected JSON
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            input_data = []
except FileNotFoundError:
    print(f"Input file {input_file} not found.")
    input_data = []

if input_data:
    # Extract gcid values and combine them into a single dictionary with a list of gcids
    output_data = {"gcids": [item.get("gcid") for item in input_data if "gcid" in item]}

    # Write the output to a file using with statement
    with open(output_file, 'w') as outfile:
        json.dump(output_data, outfile, indent=4)

    print(f"All results have been written to {output_file}.")
else:
    print("No valid input data found.")
