#!/bin/bash
# Sends a Post request to trigger dev environment's importer.

set -euo pipefail

echo -n "Enter dev importer (master) api key:"
read dev_api_key

response=$(curl --write-out "%{http_code}" -H "Content-Type: application/json" \
-H "x-functions-key: $dev_api_key" -d "{ }" \
-X POST https://hfp-analytics-importer.azurewebsites.net/admin/functions/importer)

echo "Response was:"
echo "$response"