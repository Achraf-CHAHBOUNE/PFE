import mysql.connector
import csv
import sys
from config import SOURCE_MYSQL_HOST, SOURCE_MYSQL_USER, SOURCE_MYSQL_PASSWORD, FIRST_MYSQL_DB,SOURCE_MYSQL_PORT

# Database connection details
DB_HOST = SOURCE_MYSQL_HOST 
DB_USER = SOURCE_MYSQL_USER
DB_PASSWORD = SOURCE_MYSQL_PASSWORD
DB_NAME = FIRST_MYSQL_DB
DB_PORT = SOURCE_MYSQL_PORT

print("🚀 Process started...")

print(f"🔹 Host: {DB_HOST}")
print(f"🔹 User: {DB_USER}")
print(f"🔹 Database: {DB_NAME}")

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
    try:
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
    except mysql.connector.Error as err:
        print(f"❌ Error fetching size for table {table_name}: {err}")
        return None

def process_table_sizes(input_file, output_file, conn):
    """Reads table names, retrieves sizes, and writes to a CSV file."""
    try:
        with open(input_file, "r") as file:
            table_names = file.read().splitlines()
            print(f"🔹 Tables read from {input_file}: {table_names}")  # Debug statement

        if not table_names:
            print(f"⚠️ Warning: No tables found in {input_file}")
            return

        table_sizes = []
        total_size = 0

        for table in table_names:
            size = get_table_size(table, conn)
            if size is not None:  # Avoid writing None values
                table_sizes.append([table, size])
                total_size += size

        # Write to CSV
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Table Name", "Size (MB)"])  # Header
            writer.writerows(table_sizes)
            writer.writerow(["Total Size", total_size])  # Add total at the end

        print(f"✅ Table sizes saved to {output_file}")
    except Exception as e:
        print(f"❌ An unexpected error occurred while processing {input_file}: {e}")
        raise  # Re-raise the exception to see the full traceback

def Mysql_process():
    """Establishes a database connection and processes table sizes."""
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

    except mysql.connector.InterfaceError as e:
        print("❌ Network or database interface error. Possible reasons:")
        print("   - MySQL server is down or unreachable.")
        print("   - Incorrect hostname or port.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.ProgrammingError as e:
        print("❌ Programming error. Possible reasons:")
        print("   - Incorrect database name.")
        print("   - Incorrect username or password.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.DatabaseError as e:
        print("❌ General database error. Possible reasons:")
        print("   - Database is corrupted or locked.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.OperationalError as e:
        print("❌ Operational error. Possible reasons:")
        print("   - Too many connections to MySQL server.")
        print("   - Authentication issues.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.IntegrityError as e:
        print("❌ Integrity error. Possible reasons:")
        print("   - Foreign key constraint violation.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.DataError as e:
        print("❌ Data error. Possible reasons:")
        print("   - Invalid data types used in the connection parameters.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except mysql.connector.NotSupportedError as e:
        print("❌ Feature not supported error. Possible reasons:")
        print("   - Unsupported MySQL feature in this version.")
        print(f"   Error details: {e}")
        sys.exit(1)

    except Exception as e:
        print("❌ An unexpected error occurred while connecting to MySQL.")
        print(f"   Error details: {e}")
        sys.exit(1)
        
    print("🔄 Processing table sizes...")
    
    # Process each file
    for key in input_files:
        print(f"📂 Processing file: {input_files[key]}")
        process_table_sizes(input_files[key], output_files[key], conn)

    # Close the connection after all queries
    conn.close()
    print("✅ Process completed!")

# Run the process
Mysql_process()
