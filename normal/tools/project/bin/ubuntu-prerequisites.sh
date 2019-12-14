#!/usr/bin/env bash

set -e

# shellcheck disable=SC1090
source "${PROJECT_TOOLS_DIR}/etc/env.sh"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
  if [ -f /etc/debian_version ]; then

    # Check if running with required privileges
    if [ "$EUID" -ne 0 ]; then
      echo "Please run with sudo"
      exit
    fi

    # Install development requirements
    apt-get install \
      build-essential \
      libssl-dev \
      curl \
      libcurl4-openssl-dev \
      qmake \
      flex \
      bison
  fi
fi
