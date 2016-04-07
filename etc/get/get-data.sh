#!/bin/bash

while getopts "t:l:" OPTION; do
    case $OPTION in
        t) table=$OPTARG ;;
	l) location=$OPTARG ;;
        *) exit 1 ;;
    esac
done

if [ ! $NYCTRAFFIC ]; then
    source $HOME/.bash_exports
fi

case $location in
    nyc) # http://www.nyc.gov/html/dot/html/about/datafeeds.shtml
	src=nyc
	rnd=Speed
	url=http://207.251.86.229/nyc-links-cams/TrafficSpeed.php
	;;
    mass) # http://www.massdot.state.ma.us/DevelopersData.aspx
	src=mass
	rnd=TRAVELDATA
	url=https://www.massdot.state.ma.us/feeds/traveltimes/RTTM_feed.aspx
	;;
    *) exit 1 ;;
esac


out=$HOME/data/traffic/$src/`date +'%Y-%j/%H'`
mkdir --parents `dirname $out`

python3 $NYCTRAFFIC/etc/get/get-data.py \
    --table $table \
    --source $src \
    --url $url \
    --root-node $rnd \
    --output $out
