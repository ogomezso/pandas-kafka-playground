#!/bin/bash

set -u -e
topicName=$1
shift

docker-compose exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic $topicName
