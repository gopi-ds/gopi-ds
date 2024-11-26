"""
    Given a folder with text files of GCID's,
    merge them all and put them in a single file and in a sorted order
"""

import os
import pandas as pd

# Path to the folder containing the CSV files
folder_path = 'folder_with_multiple_files_each_having_gcids'  # Replace with the actual folder path

# Intermediate and final output file paths
intermediateOutputFilePath = 'gcid.txt'  # Replace with your desired intermediate output file path
finalOutputFilePath = 'sorted_unique_gcid.txt'  # Replace with your desired final output file path

# List to store gcid values
gcidList = []

try:
    # Loop through each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            
            try:
                # Read the CSV file in chunks
                chunk_size = 1000  # Number of rows per chunk
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    # Add the gcid values to the list
                    gcidList.extend(chunk['gcid'])
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    try:
        # Write the gcid values to the intermediate output file
        with open(intermediateOutputFilePath, 'w') as intermediate_output_file:
            for gcid in gcidList:
                intermediate_output_file.write(gcid + '\n')
        print(f"GCID values have been written to {intermediateOutputFilePath}")
    except Exception as e:
        print(f"Error writing to {intermediateOutputFilePath}: {e}")

    try:
        # Read the gcid values from the intermediate output file
        with open(intermediateOutputFilePath, 'r') as intermediate_output_file:
            gcidList = intermediate_output_file.readlines()
        
        # Remove duplicates and sort the gcid values
        unique_sorted_gcid_list = sorted(set(gcid.strip() for gcid in gcidList))

        # Write the sorted unique gcid values to the final output file
        with open(finalOutputFilePath, 'w') as final_output_file:
            for gcid in unique_sorted_gcid_list:
                final_output_file.write(gcid + '\n')
        print(f"Sorted unique GCID values have been written to {finalOutputFilePath}")
    except Exception as e:
        print(f"Error processing the intermediate output file {intermediateOutputFilePath}: {e}")

except Exception as e:
    print(f"Error processing the folder {folder_path}: {e}")

finally:
    print("Processing complete.")
