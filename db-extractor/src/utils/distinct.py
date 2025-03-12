import re

# Function to extract the base name (before the week number part)
def extract_base_name(table):
    match = re.match(r'^(.*)_S\d+_A\d{4}$', table)
    if match:
        return match.group(1)
    return None

# Function to get distinct base names from the table names
def get_distinct_base_names(tables):
    base_names = set(extract_base_name(table) for table in tables)
    return list(base_names)

table_path = './data/our_data/result_mgw.txt'

# List of example table names
with open(table_path, 'r') as file:
    table_names = file.read().splitlines()

# Get distinct base names
distinct_base_names = get_distinct_base_names(table_names)

print("Distinct base names:", distinct_base_names)
