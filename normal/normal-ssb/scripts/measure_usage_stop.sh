#!/usr/bin/env bash

if [ $# -lt 2 ]; then
    echo 1>&2 "Usage: script <ssh-credentials> <ip-list>"
    exit 1
fi

CREDENTIALS=$1
IP_LIST=$2
PIDFILE=usage_cpu.pid

if ! [ -e "$CREDENTIALS" ]; then
    echo 1>&2 "Error: No credentials file '$CREDENTIALS'"
    exit 1
fi

if ! [ -e "$IP_LIST" ]; then
    echo 1>&2 "Error: No IP list file '$IP_LIST'"
    exit 1
fi

# stop measuring CPU usage of the compute node
echo "Stop measuring local CPU usage"
kill $(pgrep -f vmstat)

# stop measuring incoming network of the compute node
echo "Stop measuring network usage"
kill $(pgrep -f sar)

## stop measuring CPU usage of all storage nodes
#CURRENT_NODE=0
#for TARGET_IP in $(cat $IP_LIST); do
#	run_command() {
#        ssh -i $CREDENTIALS ubuntu@$TARGET_IP $*
#    }
#    echo "Stop measuring CPU usage on: $TARGET_IP"
#    run_command "kill $(run_command pgrep -f vmstat)"
#	echo "Get metrics file from: $TARGET_IP"
#    scp -i $CREDENTIALS ubuntu@$TARGET_IP:/home/ubuntu/usage_cpu usage_cpu_$CURRENT_NODE
#    ((CURRENT_NODE=CURRENT_NODE+1))
#done