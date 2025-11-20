from confluent_kafka.admin import AdminClient, NewTopic
import time

BOOTSTRAP = "localhost:9092"
TOPICS = [
    {"name": "orders", "num_partitions": 1, "replication_factor": 1},
    {"name": "orders_dlq", "num_partitions": 1, "replication_factor": 1},
    {"name": "order_averages", "num_partitions": 1, "replication_factor": 1},
]

def create_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics.keys()
    new_topics = []
    for t in TOPICS:
        if t["name"] in existing:
            print(f"Topic already exists: {t['name']}")
            continue
        new_topics.append(NewTopic(t["name"], num_partitions=t["num_partitions"], replication_factor=t["replication_factor"]))

    if new_topics:
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Created topic {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print("No new topics to create.")

if __name__ == "__main__":
    print("Waiting a moment for Kafka to be ready...")
    time.sleep(5)
    create_topics()
