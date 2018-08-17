#!/usr/bin/env bash

# Get the script path
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd`
popd > /dev/null

clean=false

# Get build args
while getopts ":c" opt; do
  case $opt in
    c)
      clean=true
      ;;
    \?)
      echo "Invalid: -$OPTARG" >&2
      ;;
  esac
done

# Make build dir
mkdir -p ${SCRIPT_PATH}/build/thirdparty

# Build AWS C++ SDK
mkdir -p ${SCRIPT_PATH}/build/thirdparty/aws-cpp-sdk
cd ${SCRIPT_PATH}/build/thirdparty/aws-cpp-sdk
git clone https://github.com/aws/aws-sdk-cpp.git ${SCRIPT_PATH}/build/thirdparty/aws-cpp-sdk
git pull
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="core;s3" .
if [ $clean = true ] ; then
    make clean
fi
make -j 12
sudo make install

# Build AWS C Common Lib
mkdir -p ${SCRIPT_PATH}/build/thirdparty/aws-c-common
cd ${SCRIPT_PATH}/build/thirdparty/aws-c-common
git clone https://github.com/mattyouill/aws-c-common.git ${SCRIPT_PATH}/build/thirdparty/aws-c-common
git pull
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON .
if [ $clean = true ] ; then
    make clean
fi
make -j 12
sudo make install

# Build AWS Checksums Lib
mkdir -p ${SCRIPT_PATH}/build/thirdparty/aws-checksums
cd ${SCRIPT_PATH}/build/thirdparty/aws-checksums
git clone https://github.com/awslabs/aws-checksums.git ${SCRIPT_PATH}/build/thirdparty/aws-checksums
git pull
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON .
if [ $clean = true ] ; then
    make clean
fi
make -j 12
sudo make install

# Build AWS C Event Stream Lib
mkdir -p ${SCRIPT_PATH}/build/thirdparty/aws-c-event-stream
cd ${SCRIPT_PATH}/build/thirdparty/aws-c-event-stream
git clone https://github.com/mattyouill/aws-c-event-stream.git ${SCRIPT_PATH}/build/thirdparty/aws-c-event-stream
git pull
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON .
if [ $clean = true ] ; then
    make clean
fi
make j 12
sudo make install

# Get CSV Parser Lib
mkdir -p ${SCRIPT_PATH}/build/thirdparty/csv-parser
cd ${SCRIPT_PATH}/build/thirdparty/csv-parser
git clone https://github.com/AriaFallah/csv-parser.git ${SCRIPT_PATH}/build/thirdparty/csv-parser
git pull


cd ${SCRIPT_PATH}

# Check if venv activated
if [ -z "${VIRTUAL_ENV}" ]; then
    echo "Need to activate virtualenv"
    exit 1
fi

if [ $clean = true ] ; then
    python ./setup.py clean --all
fi
python ./setup.py install
