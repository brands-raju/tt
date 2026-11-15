
import json
import time

import clickhouse_connect
from kafka import KafkaConsumer
#from kafka.errors import NoBrokersAvailable

def connect_clickhouse():
    while True:
        try:
            client = clickhouse_connect.get_client(
                host="clickhouse",
                port=8123,
                username="default",
                password=""
            )

            client.command("""
                CREATE TABLE IF NOT EXISTS default.air_quality
                (
                    event_time DateTime,
                    city String,
                    pm25 Float64,
                    pm10 Float64,
                    temperature Float64
                )
                ENGINE = MergeTree
                ORDER BY (city, event_time)
            """)

            print("Connected to ClickHouse.", flush=True)
            return client

        except Exception as error:
            print("Waiting for ClickHouse:", error, flush=True)
            time.sleep(3)


def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                "air_quality",
                bootstrap_servers=["kafka:9092"],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="air-quality-group",
                value_deserializer=lambda value: json.loads(
                    value.decode("utf-8")
                )
            )

            print("Connected to Kafka topic.", flush=True)
            return consumer

        except Exception as error:
            print("Waiting for Kafka...", flush=True)
            time.sleep(3)


def main():
    client = connect_clickhouse()
    consumer = connect_kafka()

    print("Kafka Consumer Started", flush=True)

    try:
        for message in consumer:
            reading = message.value

            try:
                client.insert(
                    "default.air_quality",
                    [[
                        reading["event_time"],
                        reading["city"],
                        reading["pm25"],
                        reading["pm10"],
                        reading["temperature"]
                    ]],
                    column_names=[
                        "event_time",
                        "city",
                        "pm25",
                        "pm10",
                        "temperature"
                    ]
                )

                print("Inserted:", reading, flush=True)

            except Exception as error:
                print("Insertion error:", error, flush=True)

    except KeyboardInterrupt:
        print("Consumer stopped.", flush=True)

    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
