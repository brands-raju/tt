
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
#from kafka.errors import NoBrokersAvailable

TOPIC = "air_quality"


def connect_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda value: json.dumps(value).encode("utf-8")
            )
            print("Connected to Kafka.", flush=True)
            return producer
        except Exception as error:
            print("Waiting for Kafka...", flush=True)
            time.sleep(3)


def main():
    producer = connect_producer()
    cities = ["Mumbai", "Pune", "Nashik", "Nagpur"]

    print("Air Quality Producer Started", flush=True)

    try:
        while True:
            reading = {
                "event_time": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "city": random.choice(cities),
                "pm25": round(random.uniform(10, 180), 2),
                "pm10": round(random.uniform(20, 250), 2),
                "temperature": round(random.uniform(24, 38), 2)
            }

            producer.send(TOPIC, value=reading).get(timeout=10)

            print("Sent:", reading, flush=True)

            time.sleep(2)

    except KeyboardInterrupt:
        print("Producer stopped.", flush=True)

    finally:
        producer.close()


if __name__ == "__main__":
    main()
