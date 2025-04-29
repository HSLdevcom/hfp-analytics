#!/bin/bash

set -euo pipefail

# for mac use command 'docker compose' instead of 'docker-compose'
docker-compose down

docker-compose build

docker-compose up -d

docker ps -a
