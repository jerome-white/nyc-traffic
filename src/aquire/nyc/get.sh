#!/bin/bash

out=$HOME/data/traffic/nyc/`date +'%Y-%j/%s'`
mkdir --parents `dirname $out`

wget \
    --random-wait \
    --quiet \
    --wait=3 \
    --tries=3 \
    --timeout=13 \
    --output-document=$out \
    http://207.251.86.229/nyc-links-cams/LinkSpeedQuery.txt
