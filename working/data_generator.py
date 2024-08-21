import csv
import sys


def generate_csv(num_records, output_file_prefix='output'):
    delimiter = chr(29)  # Group Separator ASCII = 29
    num_files = 5  # Number of files to create
    records_per_file = num_records // num_files  # Divide records equally among files

    for file_num in range(1, num_files + 1):
        output_file = f"{output_file_prefix}_file{file_num}.csv"

        # Open the output file in write mode
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)

            # Write the records for this file
            for i in range(records_per_file):
                global_index = (file_num - 1) * records_per_file + i
                writer.writerow([f'{global_index}', f'{global_index * 10}'])

        print(f"Generated {records_per_file} records in {output_file}")

    # If there are any leftover records, add them to the last file
    remaining_records = num_records % num_files
    if remaining_records > 0:
        output_file = f"{output_file_prefix}_file{num_files}.csv"
        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=delimiter)
            for i in range(remaining_records):
                global_index = num_files * records_per_file + i
                writer.writerow([f'{global_index}', f'{global_index * 10}'])
        print(f"Added {remaining_records} leftover records to {output_file}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python data_generator.py <num_records> <output_file_prefix>")
        sys.exit(1)

    try:
        num_records = int(sys.argv[1])
        output_file_prefix = sys.argv[2]
    except ValueError:
        print("Error: num_records must be an integer.")
        sys.exit(1)

    generate_csv(num_records, output_file_prefix)
