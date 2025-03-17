import csv
import sys
import re

# File paths
input_files = {
    "5min": "./data/our_data/result_5min.txt",
    "15min": "./data/our_data/result_15min.txt",
    "mgw": "./data/our_data/result_mgw.txt"
}

# Path to the table.csv file
table_csv_path = "./data/our_data/tables.csv"

# Define the patterns for 5-minute, 15-minute, and MGW tables
pattern_5min = re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]5_S(\d+)_A(\d{4})$')
pattern_15min = re.compile(r'^(CALIS|MEIND|RAIND)[-_]APG43[_-]15_S(\d+)_A(\d{4})$')
pattern_mgw = re.compile(r'^([A-Za-z0-9]+)MGW_S(\d+)_A(\d{4})$')

def load_table_sizes(csv_path):
    """Load table sizes from a CSV file into a dictionary."""
    table_sizes = {}
    try:
        with open(csv_path, "r") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) == 2:  # Ensure there are exactly two columns
                    table_name, size = row
                    table_sizes[table_name] = float(size)
        print("✅ Table sizes loaded successfully!")
        return table_sizes
    except Exception as e:
        print(f"❌ Error loading table sizes from {csv_path}: {e}")
        sys.exit(1)

def get_table_size(table_name, table_sizes):
    """Fetch size of a single table from the loaded dictionary."""
    return table_sizes.get(table_name, 0)  # Return 0 if the table is not found

def extract_year_week_and_type(table_name):
    """Extract year, week, and type (5min, 15min, MGW) from the table name using regex patterns."""
    if pattern_5min.match(table_name):
        week = pattern_5min.match(table_name).group(2)
        year = pattern_5min.match(table_name).group(3)
        return year, week, "5min"
    elif pattern_15min.match(table_name):
        week = pattern_15min.match(table_name).group(2)
        year = pattern_15min.match(table_name).group(3)
        return year, week, "15min"
    elif pattern_mgw.match(table_name):
        week = pattern_mgw.match(table_name).group(2)
        year = pattern_mgw.match(table_name).group(3)
        return year, week, "mgw"
    else:
        return None, None, None

def read_input_files(input_files):
    """Read table names from input files and return a set of all tables."""
    all_tables = set()
    for key, file_path in input_files.items():
        try:
            with open(file_path, "r") as file:
                tables = file.read().splitlines()
                all_tables.update(tables)
            print(f"✅ Tables read from {file_path}")
        except Exception as e:
            print(f"❌ Error reading tables from {file_path}: {e}")
            sys.exit(1)
    return all_tables

def process_global_summary(table_sizes, input_tables):
    """Create a global summary of tables by year, week, and type, filtered by input tables."""
    global_summary = {}  # Format: {year: {week: {"5min": {"count": X, "size": Y}, ...}}}

    for table_name in input_tables:
        size = get_table_size(table_name, table_sizes)
        year, week, table_type = extract_year_week_and_type(table_name)
        if year and week and table_type:
            if year not in global_summary:
                global_summary[year] = {}
            if week not in global_summary[year]:
                global_summary[year][week] = {"5min": {"count": 0, "size": 0},
                                             "15min": {"count": 0, "size": 0},
                                             "mgw": {"count": 0, "size": 0}}
            global_summary[year][week][table_type]["count"] += 1
            global_summary[year][week][table_type]["size"] += size

    return global_summary

def write_global_summary(global_summary, output_file):
    """Write the global summary to a CSV file, sorted by year and week."""
    try:
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(["Year", "Week", "Type", "Total Tables", "Total Size (MB)"])
            
            # Sort years in ascending order
            sorted_years = sorted(global_summary.keys())
            for year in sorted_years:
                weeks = global_summary[year]
                total_tables_year = 0
                total_size_year = 0
                
                # Sort weeks in ascending order
                sorted_weeks = sorted(weeks.keys(), key=lambda x: int(x))
                for week in sorted_weeks:
                    data = weeks[week]
                    total_tables_week = 0
                    total_size_week = 0
                    
                    # Write data for each type
                    for table_type in ["5min", "15min", "mgw"]:
                        count = data[table_type]["count"]
                        size = data[table_type]["size"]
                        writer.writerow([year, week, table_type, count, size])
                        total_tables_week += count
                        total_size_week += size
                    
                    # Write the total for the week
                    writer.writerow([year, week, "Total", total_tables_week, total_size_week])
                    total_tables_year += total_tables_week
                    total_size_year += total_size_week
                
                # Write the total for the year
                writer.writerow([year, "All Weeks", "Total", total_tables_year, total_size_year])
        
        print(f"✅ Global summary saved to {output_file}")
    except Exception as e:
        print(f"❌ Error writing global summary to {output_file}: {e}")
        raise

def main():
    """Main function to load table sizes, process global summary, and write to CSV."""
    print("🚀 Process started...")

    # Load table sizes from the CSV file
    table_sizes = load_table_sizes(table_csv_path)

    # Read tables from input files
    input_tables = read_input_files(input_files)

    print("🔄 Processing global summary...")
    
    # Create global summary
    global_summary = process_global_summary(table_sizes, input_tables)

    # Write global summary to CSV
    output_file = "./data/our_data/global_summary.csv"
    write_global_summary(global_summary, output_file)

    print("✅ Process completed!")

# Run the process
if __name__ == "__main__":
    main()