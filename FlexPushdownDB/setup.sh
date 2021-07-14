#!/usr/bin/env bash

set -e

# Get the script path
pushd "$(dirname "$0")" > /dev/null
SCRIPT_PATH=$(pwd)
popd > /dev/null

PROJECT_DIR=$(readlink -f "${SCRIPT_PATH}")

PROJECT_TOOLS_DIR="${PROJECT_DIR}/tools/project"


#-----------------------------------------------------------------------------------------------------------------------
# Development prerequisities
#-----------------------------------------------------------------------------------------------------------------------

# shellcheck disable=SC1090
source "${PROJECT_TOOLS_DIR}/bin/ubuntu-prerequisites.sh"

# shellcheck disable=SC1090
source "${PROJECT_TOOLS_DIR}/bin/darwin-prerequisites.sh"
