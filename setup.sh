#!/bin/bash

set -euo pipefail

if [[ -e .env ]]; then
  read -p ".env already exists, overwrite? [y/n] " answer
  if [[ "$answer" != "y" ]]; then
    echo "Exiting"
    exit 0
  fi
fi

cp .env.test .env
echo ".env created with default values; adjust to your needs"
docker network rm stopcorr_nw || true > /dev/null 2>&1
docker network create --attachable stopcorr_nw
