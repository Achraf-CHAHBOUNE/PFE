import mysql.connector
import re
import sys
import json
from typing import List, Dict, Any, Optional
from config import files_paths as output_paths

def connect_database(config: Dict[str, Any]):
    """Connect to the database."""
    try:
        conn = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            port=config['port'],
            database=config['database']
        )
        return conn
    except mysql.connector.Error as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

def store_json(data: Any, filename: str):
    """Store data in a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def load_json(filename: str) -> Any:
    """Load data from a JSON file."""
    with open(filename, 'r') as f:
        return json.load(f)

def store_csv(data: List[List[str]], filename: str):
    """Store data in a CSV file."""
    with open(filename, 'w') as f:
        for row in data:
            f.write(','.join(row) + '\n')

def load_csv(filename: str) -> List[List[str]]:
    """Load data from a CSV file."""
    data = []
    with open(filename, 'r') as f:
        for line in f:
            data.append(line.strip().split(','))
    return data

def store_txt(data: List[str], filename: str):
    """Store data in a text file."""
    with open(filename, 'w') as f:
        f.write('\n'.join(data))

def load_txt(filename: str) -> List[str]:
    """Load data from a text file."""
    with open(filename, 'r') as f:
        return f.read().splitlines()

def filter_tables(table_names: List[str], pattern: re.Pattern) -> List[str]:
    """Filter table names based on a regex pattern."""
    return [table for table in table_names if re.match(pattern, table)]

def filter_by_year(tables: List[str], start_year: int) -> List[str]:
    """Filter tables by year (2024 or greater)."""
    return [table for table in tables if int(re.search(r'_A(\d{4})$', table).group(1)) >= start_year]

def sort_by_year_and_week(tables: List[str]) -> List[str]:
    """Sort tables by year and week."""
    return sorted(tables, key=lambda x: (
        int(re.search(r'_A(\d{4})$', x).group(1)),  # Extract year
        int(re.search(r'_S(\d+)_', x).group(1))    # Extract week number
    ))

def process_tables_names(table_names: List[str], patterns: Dict[str, re.Pattern], start_year: int):
    """Process table names by filtering and sorting them."""
    filtered_5min = filter_tables(table_names, patterns['5min'])
    filtered_15min = filter_tables(table_names, patterns['15min'])
    filtered_mgw = filter_tables(table_names, patterns['mgw'])

    filtered_5min_by_year = filter_by_year(filtered_5min, start_year)
    filtered_15min_by_year = filter_by_year(filtered_15min, start_year)
    filtered_mgw_by_year = filter_by_year(filtered_mgw, start_year)

    sorted_5min = sort_by_year_and_week(filtered_5min_by_year)
    sorted_15min = sort_by_year_and_week(filtered_15min_by_year)
    sorted_mgw = sort_by_year_and_week(filtered_mgw_by_year)

    store_txt(sorted_5min, output_paths['5min'])
    store_txt(sorted_15min, output_paths['15min'])
    store_txt(sorted_mgw, output_paths['mgw'])

    print(f"✅ Filtered 5-minute results saved to {output_paths['5min']}")
    print(f"✅ Filtered 15-minute results saved to {output_paths['15min']}")
    print(f"✅ Filtered MGW results saved to {output_paths['mgw']}")

    total_tables = len(sorted_5min) + len(sorted_15min) + len(sorted_mgw)
    print(f"✅ Total tables found: {total_tables}")

def join_table(table: str) -> str:
    """Derive the join table name by removing the _SXX_AXXXX suffix."""
    base_table_name = re.sub(r'_S\d+_A\d{4}$', '', table)
    return f"indicateur_{base_table_name}"

def extract_table_data(table: str, cursor, offset: int, batch_size: int = 5000) -> Optional[List[tuple]]:
    """Extract a single batch of data from the specified table."""
    query = f"""
        SELECT *
        FROM {table} t1 
        JOIN {join_table(table)} t2 
        ON t1.id_indicateur = t2.id 
        LIMIT {batch_size} 
        OFFSET {offset}
    """
    cursor.execute(query)
    batch = cursor.fetchall()
    return batch if batch else None

def load_batch_into_database(batch: List[tuple], target_db, target_table: str):
    """Load a batch of data into the target database."""
    cursor = target_db.cursor()
    try:
        cursor.execute(f"SELECT * FROM {target_table} LIMIT 0")
        columns = [col[0] for col in cursor.description]
        placeholders = ', '.join(['%s'] * len(batch[0]))
        insert_query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(insert_query, batch)
        target_db.commit()
        print(f"✅ Successfully loaded {len(batch)} rows into {target_table}")
    except Exception as e:
        print(f"❌ Error loading batch into {target_table}: {e}")
        target_db.rollback()
    finally:
        cursor.close()