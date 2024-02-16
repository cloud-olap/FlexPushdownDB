#!/usr/bin/env bash

set -e

# shellcheck disable=SC1090
#source "${PROJECT_TOOLS_DIR}/etc/env.sh"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
  if [ -f /etc/debian_version ]; then

    # Check if running with required privileges
    if [ "$EUID" -ne 0 ]; then
      echo "Please run with sudo"
      exit
    fi

    apt-get update

    # Install development requirements
    apt-get install -y \
      build-essential \
      llvm-14 \
      clang-14 \
      cmake \
      ninja-build \
      libcurl4-openssl-dev \
      libssl-dev \
      uuid-dev \
      zlib1g-dev \
      libpulse-dev \
      binutils-dev \
      bison \
      flex \
      libtool \
      tcl \
      libdeflate-dev \
      openjdk-11-jre-headless \
      maven

  fi
fi

sudo ln -s /usr/bin/clang-14 /usr/bin/clang
sudo ln -s /usr/bin/clang++-14 /usr/bin/clang++
