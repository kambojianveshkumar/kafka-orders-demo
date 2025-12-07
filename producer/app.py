import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka broker running in docker compose
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "orders-topic"

def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(100, 10000), 2),
        "currency": "INR",
        "status": random.choice(["SUCCESS", "FAILED", "PENDING"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        order = generate_order()
        producer.send(TOPIC_NAME, order)
        producer.flush()
        print(f"Sent: {order}")

