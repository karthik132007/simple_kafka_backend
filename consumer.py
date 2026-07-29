import json

from confluent_kafka import Consumer

consumer_config={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order_tracker',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

print("Consumer is running and subscribed to 'orders'")
try:
    while 1:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        val = msg.value().decode('utf-8')
        order = json.loads(val)
        print(f"recived order: {order['quantity']} x {order['item']} for {order['user']}")
except KeyboardInterrupt:
    print("Shutting down")
finally:
    consumer.close()