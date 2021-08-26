#!/bin/bash

set -euo pipefail

if [[ -e .env ]]; then
  read -p ".env already exists, overwrite? [y/n] " answer
  if [[ "$answer" == "y" ]]; then
    echo "Overwriting .env"
    cp .env.test .env
  fi
else
  cp .env.test .env
  echo ".env created from .env.test; adjust to your needs"
fi

mkdir -p data
mkdir -p data/import
docker network rm stopcorr_nw || true > /dev/null 2>&1
docker network create --attachable stopcorr_nw
