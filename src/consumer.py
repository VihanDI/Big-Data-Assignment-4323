from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader
from io import BytesIO
import json
import time
import os
import random

BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "orders"
DLQ_TOPIC = "orders_dlq"
AVG_TOPIC = "order_averages"
GROUP_ID = "order_consumers"

MAX_RETRIES = 3
BASE_BACKOFF = 1.0 # in seconds

def load_schema(path):
    with open(path, "r") as f:
        return parse_schema(json.load(f))

def deserialize_avro(schema, data):
    bytes_reader = BytesIO(data)
    return schemaless_reader(bytes_reader, schema)

def produce_dlq(producer, raw_bytes, reason=None):
    headers = [("error", str(reason).encode("utf-8"))] if reason else None
    producer.produce(DLQ_TOPIC, value=raw_bytes, headers=headers)
    producer.flush()
    print(" -> Sent to DLQ")

def publish_average(producer, avg, count):
    payload = json.dumps({"running_average": avg, "count": count}).encode("utf-8")
    producer.produce(AVG_TOPIC, value=payload)
    producer.flush()
    print(f" -> Published average: {avg:.4f} over {count} messages")

def main(schema_path):
    schema = load_schema(schema_path)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    consumer.subscribe([SOURCE_TOPIC])

    running_sum = 0.0
    count = 0

    try:
        print("Consumer started. Waiting for messages...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Consumer error:", msg.error())
                    continue

            raw = msg.value()

            # Retry logic
            attempt = 0
            success = False
            last_exc = None

            while attempt < MAX_RETRIES and not success:
                try:
                    order = deserialize_avro(schema, raw)

                    # simulate random failure (40% of messages fail)
                    if random.random() < 0.4:
                        raise ValueError("Simulated processing error")

                    price = float(order["price"])
                    running_sum += price
                    count += 1
                    running_avg = running_sum / count

                    print("Consumed:", order)
                    print(f"Running average price: {running_avg:.4f}")

                    publish_average(producer, running_avg, count)

                    success = True
                    consumer.commit(message=msg)

                except Exception as e:
                    last_exc = e
                    attempt += 1
                    backoff = BASE_BACKOFF * (2 ** (attempt - 1))
                    print(f"Error (attempt {attempt}/{MAX_RETRIES}): {e} | backoff {backoff}s")
                    time.sleep(backoff)

            if not success:
                print("Permanent failure → sending to DLQ")
                produce_dlq(producer, raw, reason=str(last_exc))
                consumer.commit(message=msg)

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    schema_default = os.path.join(os.path.dirname(__file__), "order.avsc")

    main(schema_default)