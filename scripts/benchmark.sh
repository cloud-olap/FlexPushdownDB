#!/usr/bin/env bash

# Get the script path
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd`
popd > /dev/null

# Check if venv activated
if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Need to activate virtualenv"
    exit 1
fi

# Set the package path
PACKAGE_PATH=`cd ${SCRIPT_PATH}/.. ; pwd `

BENCHMARK_FILE=${PACKAGE_PATH}/s3filter/benchmark/main.py

echo "Running benchmark at '${BENCHMARK_FILE}'. Python path is '${PACKAGE_PATH}'"

PYTHONPATH=${PACKAGE_PATH} python -O ${BENCHMARK_FILE}
