import json
import time
from kafka import KafkaConsumer, errors

KAFKA_BROKER = "kafka:9092"
TOPIC = "orders-topic"

def main():
    print("Consumer starting...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print("Consumer connected to Kafka!")
            break
        except Exception as e:
            print("Kafka not ready... Retrying", e)
            time.sleep(3)

    for message in consumer:
        print("Received:", message.value)

if __name__ == "__main__":
    main()




