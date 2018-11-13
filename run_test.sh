#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

docker-compose up -d --build --force-recreate kafka
rc=$?
if [[ $rc != 0 ]]
  then exit $rc
fi

echo "Waiting for Kafka to start..."
sleep 15

docker ps -a

docker-compose up --exit-code-from go-agg-framer
rc=$?
if [[ $rc != 0 ]]
  docker ps -a
  then exit $rc
fi
