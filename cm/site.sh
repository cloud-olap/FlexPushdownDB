#!/usr/bin/env bash

# Get the script path
pushd `dirname $0` > /dev/null
SCRIPT_PATH=`pwd`
popd > /dev/null

# Run the playbook
ansible-playbook \
    -i "${SCRIPT_PATH}/inventory/tst/inventory.ini" \
    "${SCRIPT_PATH}/playbook/site.yml"
