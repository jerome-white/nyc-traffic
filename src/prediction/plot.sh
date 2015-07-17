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

    for j in $cluster/log/${l[1]}/dat-*; do
	k=`cut --delimiter='-' --fields=2 <<< $(basename $j)`
	output=`dirname $dat`/fig/$k
	mkdir --parents $output

	echo ${l[@]} $output
	python3 $prediction/plot.py --data $dat --clusters $j --output $output
    done
done
