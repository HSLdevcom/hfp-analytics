#!/bin/bash
# Sends a Post request to trigger importer locally.

set -euo pipefail

local_api_key=MSrah7gr4eGE1x8wWAlX2uO6A3mT54NWG6FaO121ViAC7xOfd2net9==

curl -X POST -H "Content-Type: application/json" -H "x-functions-key: $local_api_key" -d {} http://localhost:7072/admin/functions/importer
