#!/bin/bash

p_log=$NYCTRAFFICLOG/prediction
mkdir --parents $p_log

#
# Create the output directory
#
while true; do
    base=$p_log/`date +%Y_%m-%d_%H%M`.`hostname`
    if [ ! -e $base ]; then
        mkdir --parents $base
        break
    fi
    # sleep until this minute is finished
    sleep `expr 60 - $(expr $(date +%s) % 60)`
done

#
# Create the configuration files
#
python3 $NYCTRAFFIC/src/prediction/mkconfigs.py --top-level $base

echo $base
