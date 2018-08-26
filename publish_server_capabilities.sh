#!/bin/bash

# This is a very simple script for writing capabilities to the filesystem.
# DTN-RPyC will publish these.

FILESYSTEM="/dev/sd"

host=$(hostname)
host=$(echo "$host.xy")
if [ ! -f ../$host ]
then
    gps_coord="gps=50.805996776:8.76916359"
    echo "GPS not found, setting dummy values"
else
    gps_coord=$(cat ../$host | tr " " ":" | awk -vOFS='=' '{print "gps", $1}')
fi
disk_space=$(df -h | grep $FILESYSTEM | awk -F' ' -vOFS='=' '{print "disk_space", $4}')
avg_load=$(cat /proc/loadavg | awk -F' ' -vOFS='=' '{print "cpu_load", $3}')
cpu_cores=$(lscpu | grep "CPU(s)" | head -1 | awk -F' ' -vOFS='=' '{print "cpu_cores", $2}')
echo "$disk_space $avg_load $cpu_cores" | tr " " "\n" > /tmp/dtnrpc/rpc.caps
