#!/bin/bash

src=$NYCTRAFFIC/src/characterise
dest=$NYCTRAFFICLOG/characterise/speed
ini=`mktemp`

cp $NYCTRAFFIC/etc/opts/prediction.ini $ini
cat <<EOF >> $ini
[parameters]
intra-reporting = 75

[output]
destination = $dest
EOF

rm --recursive --force $dest
python3 $src/ts-plot.py --config $ini # &> $src/ts-plot.out
rm $ini
