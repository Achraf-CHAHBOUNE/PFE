import mysql.connector
import json
import os
from utils.config import (
    SOURCE_MYSQL_HOST, SOURCE_MYSQL_USER, SOURCE_MYSQL_PASSWORD, SOURCE_MYSQL_PORT,
    DEST_MYSQL_HOST, DEST_MYSQL_USER, DEST_MYSQL_PASSWORD, DEST_MYSQL_PORT
)

# Define batch size
BATCH_SIZE = 5000  

# Path to store the last extracted timestamp
LAST_DATES_FILE = "data/last_dates.json"

# Load the last extracted timestamps from a file
def load_last_dates():
    if os.path.exists(LAST_DATES_FILE):
        with open(LAST_DATES_FILE, "r") as f:
            return json.load(f)
    return {}

# Save the last extracted timestamps to a file
def save_last_dates(last_dates):
    with open(LAST_DATES_FILE, "w") as f:
        json.dump(last_dates, f)

# Get connection to source MySQL
def get_source_connection(database):
    return mysql.connector.connect(
        host=SOURCE_MYSQL_HOST,
        user=SOURCE_MYSQL_USER,
        password=SOURCE_MYSQL_PASSWORD,
        database=database,
        port=SOURCE_MYSQL_PORT
    )

# Get connection to destination MySQL
def get_destination_connection():
    return mysql.connector.connect(
        host=DEST_MYSQL_HOST,
        user=DEST_MYSQL_USER,
        password=DEST_MYSQL_PASSWORD,
        database=DEST_MYSQL_DB,
        port=DEST_MYSQL_PORT
    )

# Fetch all table names from source MySQL
def get_table_names(database):
    connection = get_source_connection(database)
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    connection.close()
    return tables

# Fetch new data based on `date`
def fetch_new_data(database, table_name, last_date):
    connection = get_source_connection(database)
    cursor = connection.cursor(dictionary=True)

    query = f"""
        SELECT * FROM {table_name} 
        WHERE date > '{last_date}'
        ORDER BY date ASC
        LIMIT {BATCH_SIZE}
    """
    cursor.execute(query)

    data = cursor.fetchall()
    cursor.close()
    connection.close()

    return data, data[-1]["date"] if data else last_date

# Bulk insert into destination MySQL
def bulk_insert_into_destination(table_name, data):
    if not data:
        return

    connection = get_destination_connection()
    cursor = connection.cursor()

    columns = ", ".join(data[0].keys())
    values_placeholder = ", ".join(["%s"] * len(data[0]))

    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

    values = [tuple(record.values()) for record in data]

    cursor.executemany(insert_query, values)
    connection.commit()

    cursor.close()
    connection.close()
