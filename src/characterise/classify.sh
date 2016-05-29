#!/bin/bash

if [ $# -eq 0 ]; then
    exit 1
fi

ini=$NYCTRAFFICLOG/characterise/$1
rm --recursive --force $ini
mkdir --parents $ini

python3 $NYCTRAFFIC/src/prediction/mkconfigs.py \
    --reporting-threshold 120 \
    --output-directory $ini \
    --skeleton $NYCTRAFFIC/etc/opts/prediction.ini \
    --parallel \
    --quiet

for i in $ini/*; do
    echo $i
    python3 $NYCTRAFFIC/src/characterise/$1.py --config $i || break
done
