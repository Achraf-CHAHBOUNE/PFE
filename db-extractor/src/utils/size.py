import mysql.connector
import csv
from config import SOURCE_MYSQL_HOST, SOURCE_MYSQL_USER, SOURCE_MYSQL_PASSWORD, SECOND_MYSQL_DB

# Database connection details
DB_HOST = SOURCE_MYSQL_HOST
DB_USER = SOURCE_MYSQL_USER
DB_PASSWORD = SOURCE_MYSQL_PASSWORD
DB_NAME = SECOND_MYSQL_DB

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

def get_table_size(table_name, conn):
    """Fetch size of a single table in MB."""
    cursor = conn.cursor()
    
    query = """
    SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
    FROM information_schema.tables
    WHERE table_schema = %s AND table_name = %s;
    """
    
    cursor.execute(query, (DB_NAME, table_name))
    result = cursor.fetchone()
    
    cursor.close()
    
    return result[0] if result else 0  # Return 0 if table doesn't exist

def process_table_sizes(input_file, output_file, conn):
    """Reads table names, retrieves sizes, and writes to a CSV file."""
    with open(input_file, "r") as file:
        table_names = file.read().splitlines()

    table_sizes = []
    total_size = 0

    for table in table_names:
        size = get_table_size(table, conn)
        table_sizes.append([table, size])
        total_size += size

    # Write to CSV
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Table Name", "Size (MB)"])  # Header
        writer.writerows(table_sizes)
        writer.writerow(["Total Size", total_size])  # Add total at the end

    print(f"✅ Table sizes saved to {output_file}")

# Open database connection once
conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

# Process each file
for key in input_files:
    process_table_sizes(input_files[key], output_files[key], conn)

# Close the connection after all queries
conn.close()
