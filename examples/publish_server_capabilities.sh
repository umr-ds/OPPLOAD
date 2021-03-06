#!/bin/bash

# This is a very simple script for writing capabilities to the filesystem.
# DTN-RPyC will publish these.

FILESYSTEM="/dev/sd"

if [ ! -f coords.xy ]
then
    gps_coord="gps=50.805996776:8.76916359"
    echo "GPS not found, setting dummy values"
else
    gps_coord=$(cat coords.xy | tr " " ":" | awk -vOFS='=' '{print "gps_coords", $1}')
fi
disk_space=$(df | grep $FILESYSTEM | awk -F' ' -vOFS='=' '{print "disk_space", $4}')
avg_load=$(cat /proc/loadavg | awk -F' ' -vOFS='=' '{print "cpu_load", $3}')
memory=$(free -t | tail -1 | rev | cut -d" " -f1 | rev | awk '{ printf "memory=";gsub(/^[ \t]+|[ \t]+$/, "");print}')
echo "$disk_space $avg_load $memory $gps_coord" | tr " " "\n" > /tmp/rpc.caps
