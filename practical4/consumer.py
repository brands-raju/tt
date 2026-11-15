
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Schema Registry configuration
schema_registry_conf = {
    "url": "http://localhost:8081"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Consumer configuration
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "healthcare-consumer",
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer("utf_8")
}

# Avro deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client
)

consumer_conf["value.deserializer"] = avro_deserializer

# Create consumer
consumer = DeserializingConsumer(consumer_conf)

# Subscribe to topic
consumer.subscribe(["healthcare-patients"])

print("Consumer started...")
print("Reading healthcare records...\n")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Consumer error:", msg.error())
            continue

        patient = msg.value()

        print("Received Patient:")
        print(patient)
        print("----------------------")

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
#pip install "confluent-kafka[avro]"
#docker on then, docker compose -f docker-config.yml up -d
#docker ps
#curl.exe http://localhost:8081/subjects
#python Producer-v1.py
#curl.exe -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data "{\"compatibility\":\"BACKWARD\"}" http://localhost:8081/config
#python Producer-v2.py