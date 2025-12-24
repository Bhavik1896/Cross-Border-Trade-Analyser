#!/bin/bash
# File: create-topics.sh

# Environment variables provided by Docker Compose
KAFKA_BROKERS="kafka:29092"
TOPIC_NAME="gdelt-live"

echo "Waiting for Kafka to become available..."
# Wait for 10 seconds (or until the broker is up)
sleep 10

# Run the topic creation command until it succeeds
/usr/bin/kafka-topics --create \
    --if-not-exists \
    --topic "$TOPIC_NAME" \
    --bootstrap-server "$KAFKA_BROKERS" \
    --partitions 1 \
    --replication-factor 1

echo "Kafka topic '$TOPIC_NAME' created successfully!"