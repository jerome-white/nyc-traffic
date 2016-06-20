#!/bin/bash

while getopts "p:" OPTION; do
    case $OPTION in
        p) program=$OPTARG ;;
        *) exit 1 ;;
    esac
done

ini=`mktemp`
bin=$NYCTRAFFIC/src/characterise
out=$NYCTRAFFICLOG/characterise/$program/data

cat $NYCTRAFFIC/etc/opts/prediction.ini > $ini
cat <<EOF >> $ini
[window]
observation = 1,10
prediction = 1,10

[parameters]
intra-reporting = 120
acceleration = -0.002
activity = $program

[output]
destination = $out
EOF

rm --recursive --force $out/*
python3 $bin/classify.py --config $ini &> $bin/$program.out
rm $ini
