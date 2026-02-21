#!/bin/bash
export VERTXWEB_ENVIRONMENT="dev"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
#VXPORT=5000 DATA_DIR="${SCRIPT_DIR}/data/" TILE_SIZE=256 java -jar "${SCRIPT_DIR}/build/libs/hict_server-1.0.45-93ca650-webui_12c3391-fat.jar"
VXPORT=5000 DATA_DIR="/mnt/Models/HiCT/data/" TILE_SIZE=256 java -jar "${SCRIPT_DIR}/build/libs/hict_server-1.0.49-cfba91c-webui_12c3391-fat.jar"
