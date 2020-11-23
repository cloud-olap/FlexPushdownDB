#!/usr/bin/env bash

set -e

# Get the script path
pushd "$(dirname "$0")" > /dev/null
SCRIPT_PATH=$(pwd)
popd > /dev/null

PROJECT_DIR=$(readlink -f "${SCRIPT_PATH}/../../..")

BUILD_TYPE="RelWithDebInfo"

cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DCMAKE_C_COMPILER=/usr/bin/clang-7 \
      -DCMAKE_CXX_COMPILER=/usr/bin/clang++-7 \
      -GNinja \
      -S "${PROJECT_DIR}" \
      -B "${PROJECT_DIR}/cmake-build-${BUILD_TYPE,,}-llvm-7-ninja/"

cmake --build \
      "${PROJECT_DIR}/cmake-build-${BUILD_TYPE,,}-llvm-7-ninja/" \
      --target all  \
      -- -j32
