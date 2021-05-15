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

# measure CPU usage of the compute node
echo "Start measuring local CPU usage"
vmstat 1 > usage_cpu_local &

# measure incoming network of the compute node
echo "Start measuring network usage"
sar -n DEV 1 > usage_network &

## measure CPU usage of all storage nodes
#for TARGET_IP in $(cat $IP_LIST); do
#	run_command() {
#        ssh -i $CREDENTIALS ubuntu@$TARGET_IP $*
#    }
#    echo "Start measuring CPU usage on: $TARGET_IP"
#    run_command "nohup vmstat 1 > usage_cpu &"
#done