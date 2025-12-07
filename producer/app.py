import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer, errors

KAFKA_BROKER = "kafka:9092"
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
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Producer connected to Kafka!")
            break
        except Exception as e:
            print("Kafka not ready... Retrying", e)
            time.sleep(3)

    while True:
        order = generate_order()
        producer.send(TOPIC, order)
        producer.flush()
        print("Sent:", order)
        time.sleep(3)

if __name__ == "__main__":
    main()
