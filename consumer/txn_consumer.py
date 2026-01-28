import json
from kafka import KafkaConsumer

TOPIC = "bank_txn_stream"
BOOTSTRAP = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Consuming messages... Ctrl+C to stop.")
try:
    for msg in consumer:
        print(msg.value)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
