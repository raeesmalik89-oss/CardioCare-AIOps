import os
import sys
import json

from kafka import KafkaProducer

from crypto import encrypt_event


producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
)
topic = os.getenv("KAFKA_TOPIC", "cardiac.vitals.stream")

for line in sys.stdin:
    if not line.strip():
        continue
    event = json.loads(line)
    producer.send(topic, key=event["patient_id"], value=encrypt_event(event))

producer.flush()
