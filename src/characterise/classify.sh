#!/bin/bash

ini=$NYCTRAFFICLOG/characterise/classify
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
    python3 $NYCTRAFFIC/src/characterise/classify.py --config $i || break
done
