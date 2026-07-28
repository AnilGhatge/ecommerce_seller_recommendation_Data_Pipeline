import os
import csv
import sys
import json
import time
from kafka import KafkaProducer
from utils import load_config, get_logger

cfg = load_config()
logger = get_logger("KafkaStreamingProducer", cfg["log_file"])

producer = KafkaProducer(
    bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_csv_to_kafka(file_key, topic_name):
    file_path = cfg[file_key]["input_path"]
    if not os.path.exists(file_path):
        logger.error(f"Target landing file not found to stream: {file_path}")
        return
    
    logger.info(f"Starting real-time streaming for {file_key} to Kafka Topic: {topic_name}")
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(topic_name, value=row)
            time.sleep(0.01) # Simulates streaming intervals
    producer.flush()
    logger.info(f"Successfully published all landing records for {file_key}.")

if __name__ == "__main__":
    stream_csv_to_kafka("seller_catalog", cfg["kafka"]["topics"]["seller_catalog"])
    stream_csv_to_kafka("company_sales", cfg["kafka"]["topics"]["company_sales"])
    stream_csv_to_kafka("competitor_sales", cfg["kafka"]["topics"]["competitor_sales"])