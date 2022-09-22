#!/bin/bash

set -eu

az acr login --name hfpanalyticsregistry

docker build -t hfpanalyticsregistry.azurecr.io/hfp-analytics/api:test -f ../Dockerfile.api_deploy ../

docker push hfpanalyticsregistry.azurecr.io/hfp-analytics/api:test
