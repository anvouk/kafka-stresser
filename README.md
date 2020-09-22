# Kafka Stresser

Apache kafka consumer/producer stresser with simple REST api.

## Prerequisites

Ensure the followings are present in your kafka cluster:

- `KAFKA_AUTO_CREATE_TOPICS: "true"`
- `KAFKA_NUM_PARTITIONS: <PARTITION_NUMBER_HERE>`

## Usage

set `KAFKA_SERVERS` env with the addresses of your kafka cluster before running the stresser.

To start a new stress test with 10 topics each with 100 partitions (`KAFKA_NUM_PARTITIONS` >= 100):
```bash
curl -X POST 'http://localhost:8080/api/start' \
--header 'Content-Type: application/json' \
--data-raw '{
    "topicsCount": 10,
    "partitionsPerTopic": 100
}'
```

To stop the session:
```bash
curl -X POST 'http://localhost:8080/api/stop'
```
