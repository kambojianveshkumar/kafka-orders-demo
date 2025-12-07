import json
import time
from kafka import KafkaConsumer, errors

KAFKA_BROKER = "kafka:29092"
TOPIC = "orders-topic"

def main():
    print("Consumer starting...")
    consumer = None

    while consumer is None:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                api_version=(0, 10)
            )
            print("Consumer connected to Kafka")
        except errors.NoBrokersAvailable:
            print("Kafka not ready yet... retrying")
            time.sleep(5)

    for message in consumer:
        print(f"Received: {message.value}")

if __name__ == "__main__":
    main()


