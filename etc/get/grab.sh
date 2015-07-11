#!/bin/bash
#
# http://207.251.86.229/nyc-links-cams/TrafficSpeed.php

while getopts "u:o:" OPTION; do
    case $OPTION in
        u) url=$OPTARG ;;
        o)
            mkdir --parents $OPTARG
            output=$OPTARG/`date +%j`
            ;;
        *) exit 1 ;;
    esac
done

if [ ! $url ] || [ ! $output ]; then
    exit 1
fi

wget --quiet --output-document=- $url | \
    tail --lines=+3 | \
    head --lines=-1 >> \
    $output
