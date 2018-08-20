#!/bin/bash

### check your filesystem if its /dev/sd* or /dev/root or sth else
FILESYSTEM="/dev/sd"

# copied from Lars's script
USER="pum"
PASS="pum123"
RESTAUTH="$USER:$PASS"
function get_first_identity {
    curl -H "Expect:" --silent --dump-header http.header \
        --basic --user $RESTAUTH \
        "http://127.0.0.1:4110/restful/keyring/identities.json" \
        | grep -A 1 rows | tail -n1 | cut -d "\"" -f 2
}

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
disk_space=$(df -h | grep $FILESYSTEM | awk -F' ' -vOFS='=' '{print "disk_space", $4}')
avg_load=$(cat /proc/loadavg | awk -F' ' -vOFS='=' '{print "cpu_load", $3}')
cpu_cores=$(lscpu | grep "CPU(s)" | head -1 | awk -F' ' -vOFS='=' '{print "cpu_cores", $2}')
power_state=$(upower -i /org/freedesktop/UPower/devices/battery_BAT0 | egrep "state" | tr -s [:space:] | awk -F' ' -vOFS="=" '{print "power_state", $2}')
power_percent=$(upower -i /org/freedesktop/UPower/devices/battery_BAT0 | egrep "percentage" | tr -s [:space:] | awk -F' ' -vOFS="=" '{print "power_percentage", $2}')
id_self=$(get_first_identity)
echo $servald_running
echo $disk_space
echo $avg_load
echo $cpu_cores
echo $power_state
echo $power_percent
echo $gps_coord
echo $id_self
echo "$disk_space $avg_load $cpu_cores $power_state $power_percent" | tr " " "\n" > $id_self.info

# bundle id
bundle_id=$(rhizome list | grep $id_self.info -1 | tail -1 | awk '{split($0, out, " "); print out[2]}')
if [ "$bundle_id" = "" ]; then
    echo "Bundle does not exist, creating a new one."
    rhizome put $id_self.info
fi
rhizome update $id_self.info $bundle_id
journal append SENSORLOG gps "$gps_coord" 
exit 1

