#!/bin/bash

while getopts "a:" OPTION; do
    case $OPTION in
        a) activity=$OPTARG ;;
        *) exit 1 ;;
    esac
done

bin=$NYCTRAFFIC/src/characterise
log=$NYCTRAFFICLOG/characterise/$activity

dat=$log/data
ini=$log/ini

cp $NYCTRAFFIC/etc/opts/prediction.ini $ini
cat <<EOF >> $ini
[window]
observation = 1,10
prediction = 0,10

[parameters]
intra-reporting = 75
acceleration = -0.002
activity = $activity

[output]
destination = $dat
EOF

rm --recursive --force $dat
python3 $bin/classify.py --config $ini &> $bin/$activity.out
