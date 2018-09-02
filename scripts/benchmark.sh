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

LOG_FILE=${PACKAGE_PATH}/benchmark.log

CPU_SAR_FILE=${PACKAGE_PATH}/benchmark-output/sar.cpu.txt
MEM_SAR_FILE=${PACKAGE_PATH}/benchmark-output/sar.mem.txt
NET_SAR_FILE=${PACKAGE_PATH}/benchmark-output/sar.net.txt

echo "Running benchmark at '${BENCHMARK_FILE}'. Python path is '${PACKAGE_PATH}'. Logging to '${LOG_FILE}'."

nohup sar -P ALL 1 > ${CPU_SAR_FILE} 2>&1 & CPU_SAR_PID=$!
nohup sar -r 1 > ${MEM_SAR_FILE} 2>&1  & MEM_SAR_PID=$!
nohup sar -n DEV 1 > ${NET_SAR_FILE} 2>&1  & NET_SAR_PID=$!

nohup sh -c `PYTHONPATH=${PACKAGE_PATH} python -u ${BENCHMARK_FILE} > ${LOG_FILE} 2>&1 && kill ${CPU_SAR_PID} && kill ${MEM_SAR_PID} && kill ${NET_SAR_PID}` > /dev/null 2>&1 &

