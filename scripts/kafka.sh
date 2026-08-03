#!/bin/bash

# Starts a single-node Redpanda (Kafka API compatible) for local development and creates
# the two synchronizer topics. Pair it with:
#   CONFIG=./configuration/server-kafka go run cmd/server/main.go

set -e

# probed this way because `which podman` would abort the script under `set -e`
if command -v podman >/dev/null 2>&1; then
    echo "podman exists."
    cr='podman'
else
    echo "podman does not exist. using docker"
    cr='docker'
fi

# 64 MB, matching maxMessageBytes in configuration/server-kafka/config.json. SBOMs and
# profiles routinely exceed Kafka's 1 MB default, so the broker is sized up front.
MAX_MESSAGE_BYTES=67108864

# so re-running the script does not fail on a name collision with a previous run
$cr rm -f redpanda >/dev/null 2>&1 || true

# published on loopback only: this broker has no authentication, so it should not be
# reachable from the rest of the network. The image matches the one the tests use, so
# running both does not pull it twice.
$cr run --name=redpanda -d \
    -p 127.0.0.1:9092:9092 \
    -p 127.0.0.1:9644:9644 \
    redpandadata/redpanda:v24.2.7 \
    redpanda start \
    --mode dev-container \
    --smp 1 \
    --memory 1G \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr PLAINTEXT://localhost:9092 \
    --set redpanda.kafka_batch_max_bytes=$MAX_MESSAGE_BYTES

echo "waiting for redpanda to accept connections..."
for _ in $(seq 1 30); do
    if $cr exec redpanda rpk cluster info >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done

# without this the failure would surface as a confusing `rpk topic create` error
if [ -z "$ready" ]; then
    echo "redpanda did not become ready in time; check '$cr logs redpanda'" >&2
    exit 1
fi

for topic in armo.kubescape.synchronizer.out armo.kubescape.synchronizer.in; do
    # the .out topic is keyed by {account}/{cluster}: partitions preserve per-cluster
    # ordering, so a PutObject followed by a DeleteObject cannot be reordered
    $cr exec redpanda rpk topic create "$topic" \
        --partitions 3 \
        --topic-config max.message.bytes=$MAX_MESSAGE_BYTES
done

$cr exec redpanda rpk topic list
