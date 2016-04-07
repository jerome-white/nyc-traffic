#!/bin/bash
#
# https://data.gov.ie/dataset/national-roads-travel-times
#

URL=http://data.tii.ie/Datasets/Its/DatexII/TravelTimeData/Content.xml

tmp=`mktemp`
wget --quiet --output-document=$tmp $URL
if [ $? -eq 0 ]; then
    out=$HOME/data/traffic/ie/`date +'%Y-%j/%H'`
    mkdir --parents `dirname $out`

    exchange=`grep --line-number '<exchange>' $tmp | \
	cut --delimiter=':' --fields=1`
    tail --lines=+$exchange $tmp | head --lines=-1 >> $out
fi

rm $tmp
