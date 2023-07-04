#!/bin/bash
HDF5_VERSION="1.14.0"
HDF5_DIR="/home/${USER}/hdf/HDF5-${HDF5_VERSION}-Linux/HDF5/${HDF5_VERSION}/"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:${HDF5_DIR}/lib:${HDF5_DIR}/lib/plugin"
export HDF5_PLUGIN_PATH="${HDF5_DIR}/lib/plugin"
export VERTXWEB_ENVIRONMENT="production"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
VXPORT=5000 DATA_DIR=. TILE_SIZE=256 java -jar "${SCRIPT_DIR}/hict_server-1.0.0-SNAPSHOT-fat.jar"
