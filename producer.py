import json
import uuid

from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    if err:
        print("delivery failed: {}".format(err))
    else:
        print(f"delivered successfully: {msg.value().decode('utf-8')}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "Kishore",
    "item": "pasta",
    "quantity": 99,
}

val = json.dumps(order).encode("utf-8")

producer.produce(topic="orders", value=val, callback=delivery_report)
producer.flush()