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

TESTS_PATH=${PACKAGE_PATH}/s3filter/tests

echo "Running tests under '${TESTS_PATH}'. Python path is '${PACKAGE_PATH}'"

PYTHONPATH=${PACKAGE_PATH} pytest ${TESTS_PATH}