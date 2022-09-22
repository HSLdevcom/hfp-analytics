#!/bin/bash

set -eu

az acr login --name hfpanalyticsregistry

docker build -t hfpanalyticsregistry.azurecr.io/hfp-analytics/importer:test -f ../Dockerfile.importer_deploy ../

docker push hfpanalyticsregistry.azurecr.io/hfp-analytics/importer:test
