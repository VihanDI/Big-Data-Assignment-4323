# dlq_inspector_live.py
from confluent_kafka import Consumer
from fastavro import schemaless_reader
from io import BytesIO
import json
import time

BOOTSTRAP = "localhost:9092"
DLQ_TOPIC = "orders_dlq"

SCHEMA_FILE = "order.avsc"


def load_schema():
    try:
        with open(SCHEMA_FILE, "r") as f:
            return json.load(f)
    except:
        print("Schema not found, DLQ messages will be shown in raw form.\n")
        return None


def decode_avro(schema, data_bytes):
    try:
        bio = BytesIO(data_bytes)
        return schemaless_reader(bio, schema)
    except Exception as e:
        print("Failed to decode Avro:", e)
        return None


def main():
    schema = load_schema()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dlq_inspector_live",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe([DLQ_TOPIC])

    print("\nDLQ Inspector")
    print("Listening for DLQ messages...\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                time.sleep(0.3)
                continue

            if msg.error():
                print("Error:", msg.error())
                continue

            value = msg.value()

            print("\n================== DLQ MESSAGES ==================")
            print(f"Partition: {msg.partition()} Offset: {msg.offset()}")

            # decode if schema exists
            if schema:
                decoded = decode_avro(schema, value)
                if decoded:
                    print("Decoded:", decoded)
                else:
                    print("Raw Bytes:", value)
            else:
                print("Raw Bytes:", value)

            print("====================================================\n")

    except KeyboardInterrupt:
        print("\nStopping DLQ inspector...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
