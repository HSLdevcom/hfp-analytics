#!/bin/bash

# We need to get azurite docker image to run locally.
# Azurite is an open-source emulator for testing Azure blob, queue storage, and table storage applications: https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub

docker pull mcr.microsoft.com/azure-storage/azurite

# Importer uses Azure Function type "timerTrigger" which seem to require having Azurite running when testing locally.
# Check if Azurite container is already running
if [ ! "$(docker ps -q -f name=azurite)" ]; then
    # check if Azurite container has been exited, clean it up first
    if [ "$(docker ps -aq -f status=exited -f name=azurite)" ]; then
        docker rm azurite
    fi
    # run Azurite container
    docker run -d --name azurite -p 10000:10000 mcr.microsoft.com/azure-storage/azurite     azurite-blob --blobHost 0.0.0.0 --blobPort 10000
fi

# Now we can start the function.

func start
