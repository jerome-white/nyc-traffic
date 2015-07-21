#!/bin/bash

logs=(
    2015-0715-113928,6
)
traffic=$NYCTRAFFIC/src
prediction=$traffic/prediction
cluster=$traffic/cluster

for i in ${logs[@]}; do
    l=( `sed -e's/,/ /g' <<< $i` )
    dat=$prediction/log/${l[0]}/dat

    cls=( `ls $cluster/log/${l[1]}/dat-*` )
    output=`dirname $dat`/fig/$k
    mkdir --parents $output

    python3 $prediction/plot.py \
	    --data $dat \
	    --output $output \
    	    --clusters `sed -e's/ / --clusters /g' <<< ${cls[@]}`
done
