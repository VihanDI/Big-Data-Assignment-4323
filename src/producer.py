import json
import time
import random
import argparse
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from io import BytesIO
import os

BOOTSTRAP = "localhost:9092"
TOPIC = "orders"

def load_schema(path):
    with open(path, "r") as f:
        return parse_schema(json.load(f))

def serialize_avro(schema, record):
    buf = BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()

def main(rate=1.0, schema_path=None):
    schema = load_schema(schema_path)
    p = Producer({"bootstrap.servers": BOOTSTRAP})

    products = ["Item1", "Item2", "Item3", "Gadget", "Accessory"]
    try:
        while True:
            order = {
                "orderId": str(random.randint(1000, 99999)),
                "product": random.choice(products),
                "price": float(round(random.uniform(5.0, 500.0), 2))
            }
            payload = serialize_avro(schema, order)
            p.produce(TOPIC, value=payload)
            p.poll(0)
            print("Produced:", order)
            time.sleep(1.0 / rate)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        p.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    schema_default = os.path.join(os.path.dirname(__file__), "order.avsc")
    parser.add_argument("--rate", type=float, default=1.0)
    parser.add_argument("--schema", type=str, default=schema_default)
    args = parser.parse_args()

    main(rate=args.rate, schema_path=args.schema)