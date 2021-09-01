#!/bin/bash

set -euo pipefail

python hfp_import.py
python digitransit_import.py
