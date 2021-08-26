#!/bin/bash

set -euo pipefail

function cleanup {
  docker-compose -f docker-compose.test.server.yml down
}

trap cleanup EXIT

cp .env.test .env && \
  source .env && \
  docker-compose -f docker-compose.test.server.yml up
