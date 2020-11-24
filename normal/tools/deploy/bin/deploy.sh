#!/usr/bin/env bash

set -e

# Get the script path
pushd "$(dirname "$0")" >/dev/null
SCRIPT_PATH=$(pwd)
popd >/dev/null

PROJECT_DIR=$(readlink -f "${SCRIPT_PATH}/../../..")

AWS_EC2_HOST="ec2-18-144-67-176.us-west-1.compute.amazonaws.com"

rsync -avzP \
  -e "ssh -i ${PROJECT_DIR}/private/aws-keypair.pem" \
  --exclude "cmake-build*" \
  --exclude "normal-ssb/data" \
  --exclude "normal-ssb/private" \
  "${PROJECT_DIR}/" \
  "ubuntu@${AWS_EC2_HOST}:/home/ubuntu/normal"
