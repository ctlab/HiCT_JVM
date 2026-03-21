#!/bin/bash
export VERTXWEB_ENVIRONMENT="dev"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VXPORT=5000 DATA_DIR="/mnt/Models/HiCT/data/" TILE_SIZE=256 java -jar ${SCRIPT_DIR}/build/libs/hict_server-*-fat.jar $@
