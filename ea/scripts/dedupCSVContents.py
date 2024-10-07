import csv

def remove_duplicates(input_csv_path, output_csv_path):
    unique_rows = set()  # A set to hold unique rows

    # Read the CSV file and store only unique rows
    with open(input_csv_path, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read the header row
        for row in reader:
            row_tuple = tuple(row)  # Convert row to a tuple (hashable) for the set
            unique_rows.add(row_tuple)  # Add only unique rows

    # Write the unique rows to a new CSV file
    with open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(headers)  # Write the header row
        writer.writerows(unique_rows)  # Write the unique rows

    print(f"Duplicates removed. Output written to {output_csv_path}")

# Example usage:
input_csv = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\t2_bnym_nam_uat\\only_info_Sep.csv"
output_csv = "C:\\Users\\saigopinath.dokku\\OneDrive - Smarsh, Inc\\EA\\t2_bnym_nam_uat\\only_info_Sep_dedup.csv"
remove_duplicates(input_csv, output_csv)
