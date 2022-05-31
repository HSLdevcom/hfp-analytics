#!/bin/bash

docker-compose down

# We don't want to rebuild db (so that we don't lose data)
docker-compose build api
docker-compose build importer

docker-compose up -d

docker ps -a
