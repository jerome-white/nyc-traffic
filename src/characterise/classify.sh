#!/bin/bash

ini=$NYCTRAFFICLOG/characterise/classify
mkdir --parents $ini

# python3 $NYCTRAFFIC/src/prediction/mkconfigs.py \
#     --reporting-threshold 120 \
#     --output-directory $ini \
#     --skeleton $NYCTRAFFIC/etc/opts/prediction.ini \
#     --parallel \
#     --quiet

for i in $ini/*; do
    grep --silent 'depth = 0' $i || continue
    echo $i
    python3 $NYCTRAFFIC/src/characterise/classify.py --config $i || break
done
