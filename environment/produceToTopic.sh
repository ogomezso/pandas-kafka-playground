#!/bin/bash

set -u -e
topicName=$1
shift

docker-compose exec kafka kafka-console-producer --bootstrap-server kafka:9092 --topic $topicName --property "parse.key=true" --property "key.separator=,"

