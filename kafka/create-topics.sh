#!/bin/bash

# waiting on kafka cont to spin
sleep 20

# create topic with partitions and replicas
kafka-topics --bootstrap-server localhost:9092 \
    --create \
    --topic transactions \
    --partitions 2 \
    --replication-factor 2 \
    --if-not-exists


