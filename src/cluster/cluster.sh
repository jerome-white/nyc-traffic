#!/bin/bash

pth=$NYCTRAFFIC/src/cluster
mkdir --parents $pth/log

for pwindow in `seq 3 3 12`; do
    dir=$pth/log/$pwindow
    mkdir --parents $dir

    pkl=$dir/observations.pkl

    python3 $pth/cluster.py \
    	--observation-window 10 \
    	--prediction-window $pwindow \
    	--target-window 5 \
    	--speed-threshold -0.002 \
    	--clusters 0 \
    	--fig-directory $dir \
    	--pickle $pkl

    for j in `seq 2 9`; do
    	python3 $pth/cluster.py --resume $pkl --clusters $j > $dir/dat-$j
    done
done &> $pth/log/log
