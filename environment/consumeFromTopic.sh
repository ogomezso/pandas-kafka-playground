#!/bin/bash

set -u -e
topicName=$1
shift

docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic $topicName --property print.key=true --from-beginning

