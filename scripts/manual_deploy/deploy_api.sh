#!/bin/bash

# Allow deploying from commandline to dev environment so that you can deploy
# e.g. code from existing pull requests to dev environment for testing.

set -eu

az acr login --name hfpanalyticsregistry

docker build -t hfpanalyticsregistry.azurecr.io/hfp-analytics/api:dev -f ../../Dockerfile.api_deploy ../../

docker push hfpanalyticsregistry.azurecr.io/hfp-analytics/api:dev