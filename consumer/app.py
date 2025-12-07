import json
from kafka import KafkaConsumer

KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "orders-topic"

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Kafka Order Consumer Started...")
    for msg in consumer:
        print(f"Received Order: {msg.value}")

if __name__ == "__main__":
    main()

