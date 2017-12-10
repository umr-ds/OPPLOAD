#!/bin/bash
reset
serval_running=$(servald status | grep "status" | awk '{split($0,out,":"); print out[2]}')
if [ "$serval_running" = "stopped" ]; then
    echo "Start serval first"
    #exit 1
fi

host=$(hostname)
host=$(echo "$host.xy")
if [ ! -f ../$host ]
then
    gps_coord="gps=50.805996776:8.76916359"
    echo "GPS not found, setting dummy values"
else
    gps_coord=$(cat ../$host | tr " " ":" | awk -vOFS='=' '{print "gps", $1}')
fi
disk_space=$(df -h | grep /dev/sd | awk -F' ' -vOFS='=' '{print "disk_space", $4}')
avg_load=$(cat /proc/loadavg | awk -F' ' -vOFS='=' '{print "cpu_load", $3}')
cpu_cores=$(cat /proc/cpuinfo | grep "cpu cores" | head -1 | awk -F' ' -vOFS='=' '{print "cpu_cores", $4}')
power_state=$(upower -i /org/freedesktop/UPower/devices/battery_BAT0 | egrep "state" | tr -s [:space:] | awk -F' ' -vOFS="=" '{print "power_state", $2}')
power_percent=$(upower -i /org/freedesktop/UPower/devices/battery_BAT0 | egrep "percentage" | tr -s [:space:] | awk -F' ' -vOFS="=" '{print "power_percentage", $2}')

echo $servald_running
echo $disk_space
echo $avg_load
echo $cpu_cores
echo $power_state
echo $power_percent
echo $gps_coord

echo "$disk_space $avg_load $cpu_cores $power_state $power_percent" | tr " " "\n" > $(hostname).info

# bundle id
bundle_id=$(rhizome list | grep $(hostname).info -1 | tail -1 | awk '{split($0, out, " "); print out[2]}')
if [ "$bundle_id" = "" ]; then
    echo "Bundle does not exist, creating a new one."
    rhizome put $(hostname).info
fi
rhizome update server.info $bundle_id
journal append SENSORLOG gps "$gps_coord" 
exit 1

