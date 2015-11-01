#!/bin/bash

dir=$NYCTRAFFICLOG/characterise
mkdir --parents $dir
pkl=$dir/observations.pkl

python3 $NYCTRAFFIC/src/characterise/characterise.py \
    	--prediction-window 10 \
    	--target-window 10 \
    	--speed-threshold -0.5 \
	--reporting-threshold 120 \
	--figure-directory $dir \
	--pickle $pkl 2> $dir/log
