#!/bin/bash

url=http://207.251.86.229/nyc-links-cams/LinkSpeedQuery.txt
out=/data/jsw7/traffic/nyc/`date +'%Y-%j/%s'`

mkdir --parents `dirname $out`
wget --quiet --output-document=$out $url
