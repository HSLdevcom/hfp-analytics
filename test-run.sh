#!/bin/bash

set -euo pipefail

function cleanup {
  docker-compose -f docker-compose.test.yml down
}

trap cleanup EXIT

docker-compose -f docker-compose.test.yml build

mkdir -p qgis/out
mkdir -p results

source .env.test && docker-compose -f docker-compose.test.yml up &

echo "Waiting for db startup ..."
sleep 5

docker-compose -f docker-compose.test.yml run --rm worker bash import_all.sh
docker-compose -f docker-compose.test.yml run --rm worker python run_analysis.py
docker-compose -f docker-compose.test.yml run --rm worker python make_report.py

echo "OK, exiting"
