import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer, errors

KAFKA_BROKER = "kafka:29092"
TOPIC = "orders-topic"

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
    print("Producer starting...")
    producer = None

    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10)
            )
            print("Producer connected to Kafka")
        except errors.NoBrokersAvailable:
            print("Kafka not ready yet... retrying")
            time.sleep(5)

    while True:
        order = generate_order()
        producer.send(TOPIC, order)
        producer.flush()
        print(f"Sent: {order}")
        time.sleep(5)

if __name__ == "__main__":
    main()
