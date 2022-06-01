#!/bin/bash
# Sets up the project from a newly cloned repo.

set -euo pipefail

function conditional_cp () {
  if [[ -e "$2" ]]; then
    read -p "$2 already exists, overwrite? [y/n] " answer
    if [[ "$answer" == "y" ]]; then
      cp "$1" "$2"
      echo "$2 overwritten from $1"
    fi
  else
    cp "$1" "$2"
    echo "$2 created from $1"
  fi
}

function conditional_mkdir () {
  if [[ -d "$1" ]]; then
    echo "$1 directory already exists"
  else
    mkdir -p "$1"
    echo "$1 directory created"
  fi
}

conditional_cp docker-compose.test.yml docker-compose.yml
conditional_cp .env.test .env
conditional_mkdir data/import
conditional_mkdir qgis/out
conditional_mkdir results

docker-compose build

if [ ! "$(docker network ls | grep hfp_analytics_nw)" ]; then
  docker network create --attachable hfp_analytics_nw
  echo "hfp_analytics_nw docker network created"
else
  echo "hfp_analytics_nw docker network already exists"
fi
