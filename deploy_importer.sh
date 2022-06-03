#!/bin/bash

az acr login --name hfpanalyticsregistry

docker build -t hfpanalyticsregistry.azurecr.io/hfp-analytics/importer:dev -f Dockerfile.importer_deploy .

docker push hfpanalyticsregistry.azurecr.io/hfp-analytics/importer:dev
