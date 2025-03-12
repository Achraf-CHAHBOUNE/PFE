import mysql.connector
import csv
from config import SOURCE_MYSQL_HOST, SOURCE_MYSQL_USER, SOURCE_MYSQL_PASSWORD, SECOND_MYSQL_DB


# Database connection details
DB_HOST = "your_host"
DB_USER = "your_username"
DB_PASSWORD = "your_password"
DB_NAME = "your_database_name"

# File paths
input_files = {
    "5min": "./data/our_data/result_5min.txt",
    "15min": "./data/our_data/result_15min.txt",
    "mgw": "./data/our_data/result_mgw.txt"
}

output_files = {
    "5min": "./data/our_data/table_sizes_5min.csv",
    "15min": "./data/our_data/table_sizes_15min.csv",
    "mgw": "./data/our_data/table_sizes_mgw.csv"
}

def get_table_size(table_name):
    """Returns the size of a given table in MB from MySQL."""
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()

    query = f"""
    SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
    FROM information_schema.tables
    WHERE table_schema = '{DB_NAME}' AND table_name = '{table_name}';
    """

    cursor.execute(query)
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()

    return result[0] if result else 0  # Return 0 if table doesn't exist

def process_table_sizes(input_file, output_file):
    """Reads table names, gets their sizes, and saves them to a CSV file."""
    with open(input_file, "r") as file:
        table_names = file.read().splitlines()

    table_sizes = []
    total_size = 0

    for table in table_names:
        size = get_table_size(table)
        table_sizes.append([table, size])
        total_size += size

    # Write to CSV
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Table Name", "Size (MB)"])  # Header
        writer.writerows(table_sizes)
        writer.writerow(["Total Size", total_size])  # Add total at the end

    print(f"✅ Table sizes saved to {output_file}")

# Process each file
for key in input_files:
    process_table_sizes(input_files[key], output_files[key])
