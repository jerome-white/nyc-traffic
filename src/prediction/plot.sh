#!/bin/bash

# 2015-0611-152910 3,6 1
# 2015-0612-140613   6 0,1
# 2015-0614-001927 3   0,1
# 2015-0615-112906   6 0,1 (granger?)

logs=(
    # 2015-0612-140613,6
    # 2015-0614-001927,3
    # 2015-0615-112906,6
    # 2015-0619-110948,3
    2015-0622-173700,6
)
traffic=$HOME/data/traffic/fig

for i in ${logs[@]}; do
    l=( `sed -e's/,/ /g' <<< $i` )
    dat=$HOME/traffic/log/${l[0]}/dat

    for j in $traffic/clusters/${l[1]}/dat-*; do
	k=`cut --delimiter='-' --fields=2 <<< $(basename $j)`
	d=$traffic/plots/${l[0]}/$k
	mkdir --parents $d

	echo ${l[@]} $d
	./pandas/main-plot.py --data $dat --clusters $j --output $d
    done
done
