
from kafka import KafkaConsumer
import json

# Create Kafka Consumer
consumer = KafkaConsumer(
    "student-events",
    bootstrap_servers=[
        "localhost:29092",
        "localhost:39092",
        "localhost:49092"
    ],
    auto_offset_reset="earliest",
    group_id="student-consumer-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...")
print("Waiting for messages...\n")

# Receive messages
for message in consumer:
    print(
        "Received:", message.value,
        "| Partition:", message.partition,
        "| Offset:", message.offset
    )
#docker deskstop start then
#docker ps
#notepad docker-compose.yml -docker code i this
#docker compose up -d     ,docker ps 
#Test-NetConnection localhost -Port 29092
#Test-NetConnection localhost -Port 39092
#Test-NetConnection localhost -Port 49092
#pyrhon cosumer.py,producer 2 terminals