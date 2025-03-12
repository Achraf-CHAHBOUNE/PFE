import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Source MySQL Configuration (Company Server)
SOURCE_MYSQL_HOST = os.getenv("SOURCE_MYSQL_HOST")
SOURCE_MYSQL_USER = os.getenv("SOURCE_MYSQL_USER")
SOURCE_MYSQL_PASSWORD = os.getenv("SOURCE_MYSQL_PASSWORD")
SOURCE_MYSQL_PORT = int(os.getenv("SOURCE_MYSQL_PORT", 3306))
FIRST_MYSQL_DB = os.getenv("FIRST_MYSQL_DB")
SECOND_MYSQL_DB = os.getenv("SECOND_MYSQL_DB")

# Destination MySQL Configuration (Target Server)
DEST_MYSQL_HOST = os.getenv("DEST_MYSQL_HOST")
DEST_MYSQL_USER = os.getenv("DEST_MYSQL_USER")
DEST_MYSQL_PASSWORD = os.getenv("DEST_MYSQL_PASSWORD")
DEST_MYSQL_PORT = int(os.getenv("DEST_MYSQL_PORT", 3306))
DEST_MYSQL_DB = os.getenv("DEST_MYSQL_DB")

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Load 5-minute tables as a list
TABLES_5MIN = os.getenv("TABLES_5MIN", "").split(",")

# Load 15-minute tables as a list
TABLES_15MIN = os.getenv("TABLES_15MIN", "").split(",")

with open("data/last_dates.txt", "w") as f:
    for i in range(0, len(TABLES_5MIN)):
       f.write(TABLES_5MIN[i] + "\n")
    for i in range(0, len(TABLES_15MIN)):
        f.write(TABLES_15MIN[i] + "\n")
