import json
from kafka import KafkaProducer
from utils.config import KAFKA_BROKER, KAFKA_TOPIC

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Send data to Kafka
def send_to_kafka(database, table, data):
    message = {"database": database, "table": table, "data": data}
    producer.send(KAFKA_TOPIC, message)
    print(f"Sent to Kafka: {message}")
