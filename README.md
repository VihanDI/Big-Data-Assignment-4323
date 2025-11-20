# Kafka-based system that produces and consumes order messages (Python) — Assignment
### Reg No: EG/2020/4323

## What this implements
- Kafka producer that emits Avro-encoded `Order` messages
- Kafka consumer that:
  - Deserializes Avro
  - Retries transient errors (exponential backoff)
  - Sends permanently failed messages to `orders_dlq`
  - Computes running average of `price` and publishes it to `order_averages`
- Topic creation script
- Docker Compose to run Kafka & Zookeeper locally

## Prerequisites
- Docker & Docker Compose installed (for local Kafka)
- Python 3.8+
