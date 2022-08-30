#!/bin/bash
# Sends a Post request to trigger importer locally.

set -euo pipefail

local_api_key=MSrah7gr4eGE1x8wWAlX2uO6A3mT54NWG6FaO121ViAC7xOfd2net9==

response=$(curl --write-out "%{http_code}" -H "Content-Type: application/json" \
-H "x-functions-key: $local_api_key" -d "{ }" \
-X POST http://localhost:7072/admin/functions/importer)

echo "Response was:"
echo "$response"