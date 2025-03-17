import mysql.connector
import csv
import sys
from config import SOURCE_MYSQL_HOST, SOURCE_MYSQL_USER, SOURCE_MYSQL_PASSWORD, FIRST_MYSQL_DB, SOURCE_MYSQL_PORT

# Database connection details
DB_HOST = SOURCE_MYSQL_HOST 
DB_USER = SOURCE_MYSQL_USER
DB_PASSWORD = SOURCE_MYSQL_PASSWORD
DB_NAME = FIRST_MYSQL_DB
DB_PORT = SOURCE_MYSQL_PORT

print("\U0001F680 Process started...")

# File paths
input_files = {
    "5min": "./data/our_data/result_5min.txt",
    "15min": "./data/our_data/result_15min.txt",
    "mgw": "./data/our_data/result_mgw.txt"
}

output_files = {
    "5min": "./data/our_data/extracted_data_5min.csv",
    "15min": "./data/our_data/extracted_data_15min.csv",
    "mgw": "./data/our_data/extracted_data_mgw.csv"
}

def extract_table_data(table_name, conn):
    """Extracts timestamp, indicator name, and value from the given table."""
    try:
        cursor = conn.cursor()
        query = f'''
            SELECT t.time, i.nom_indicateur, t.value
            FROM {table_name} t
            JOIN indicateur_CALIS_APG43_5 i ON t.indicator_id = i.id;
        '''
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except mysql.connector.Error as err:
        print(f"\u274C Error fetching data from table {table_name}: {err}")
        return None

def process_table_data(input_file, output_file, conn):
    """Reads table names, extracts data, and writes to a CSV file."""
    try:
        with open(input_file, "r") as file:
            table_names = file.read().splitlines()
            print(f"\U0001F4C2 Tables read from {input_file}: {table_names}")

        if not table_names:
            print(f"⚠️ Warning: No tables found in {input_file}")
            return

        extracted_data = []

        for table in table_names:
            data = extract_table_data(table, conn)
            if data:
                extracted_data.extend(data)

        # Write to CSV
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Timestamp", "Indicator Name", "Value"])  # Header
            writer.writerows(extracted_data)

        print(f"✅ Data extracted and saved to {output_file}")
    except Exception as e:
        print(f"❌ An unexpected error occurred while processing {input_file}: {e}")
        raise

def Mysql_process():
    """Establishes a database connection and processes table data extraction."""
    print("🔄 Connecting to the database...")

    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        print("✅ Connection successful!")
    except mysql.connector.Error as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

    print("🔄 Extracting data from tables...")

    for key in input_files:
        print(f"📂 Processing file: {input_files[key]}")
        process_table_data(input_files[key], output_files[key], conn)

    conn.close()
    print("✅ Process completed!")

# Run the process
Mysql_process()
