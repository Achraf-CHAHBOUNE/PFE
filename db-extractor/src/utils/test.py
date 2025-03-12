import re

# Read table names from the file
def read_table_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Filter tables based on the patterns
def filter_tables(table_names, pattern):
    return [table for table in table_names if re.match(pattern, table)]

# Define the patterns for 5-minute, 15-minute, and MGW tables
pattern_5min = re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]5_S\d+_A\d{4}$')
pattern_15min = re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]15_S\d+_A\d{4}$')
pattern_mgw = re.compile(r'^([A-Za-z0-9]+)MGW_S\d+_A\d{4}$')

# Function to filter tables by year (2024 or greater)
def filter_by_year(tables, start_year):
    filtered = []
    for table in tables:
        # Extract the year from the table name
        year_match = re.search(r'_A(\d{4})$', table)
        if year_match:
            year = int(year_match.group(1))
            if year >= start_year:
                filtered.append(table)
    return filtered

# Function to sort tables by year and week
def sort_by_year_and_week(tables):
    return sorted(tables, key=lambda x: (
        int(re.search(r'_A(\d{4})$', x).group(1)),  # Extract year
        int(re.search(r'_S(\d+)_', x).group(1))    # Extract week number
    ))

# Function to save results to a file
def save_results(file_path, results):
    with open(file_path, 'w') as file:
        for result in results:
            file.write(result + '\n')

# Path to the file containing table names
input_file_path = './data/our_data/our_tables.txt'
output_5min_path = './data/our_data/result_5min.txt'
output_15min_path = './data/our_data/result_15min.txt'
output_mgw_path = './data/our_data/result_mgw.txt'

# Read table names from the file
table_names = read_table_names(input_file_path)

# Filter tables for 5-minute, 15-minute, and MGW intervals
filtered_5min = filter_tables(table_names, pattern_5min)
filtered_15min = filter_tables(table_names, pattern_15min)
filtered_mgw = filter_tables(table_names, pattern_mgw)

# Filter for year 2024 or greater
start_year = 2024
filtered_5min_by_year = filter_by_year(filtered_5min, start_year)
filtered_15min_by_year = filter_by_year(filtered_15min, start_year)
filtered_mgw_by_year = filter_by_year(filtered_mgw, start_year)

# Sort the filtered tables by year and week
sorted_5min = sort_by_year_and_week(filtered_5min_by_year)
sorted_15min = sort_by_year_and_week(filtered_15min_by_year)
sorted_mgw = sort_by_year_and_week(filtered_mgw_by_year)

# Save the sorted results to separate files
save_results(output_5min_path, sorted_5min)
save_results(output_15min_path, sorted_15min)
save_results(output_mgw_path, sorted_mgw)

print(f"Filtered 5-minute results have been saved to {output_5min_path}")
print(f"Filtered 15-minute results have been saved to {output_15min_path}")
print(f"Filtered MGW results have been saved to {output_mgw_path}")

# Print total number of tables found
total_tables = len(sorted_5min) + len(sorted_15min) + len(sorted_mgw)
print(f"Total tables found: {total_tables}")
