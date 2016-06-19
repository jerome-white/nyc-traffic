#!/bin/bash

ini=`mktemp`
fname=`basename $0 .sh`
out=$NYCTRAFFICLOG/characterise/$fname
classify=$NYCTRAFFIC/src/characterise/$fname

cat $NYCTRAFFIC/etc/opts/prediction.ini > $ini
cat <<EOF >> $ini
[window]
observation = 1,10
prediction = 1,10

[parameters]
intra-reporting = 120
acceleration = -0.002

[output]
root = $out
EOF

rm --recursive --force $out/*
python3 $classify.py --config $ini &> $classify.out
rm $ini
