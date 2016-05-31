#!/bin/bash

mkcurrent() {
    cd $1
    rm --force current
    ln --symbolic `basename $out` current
}

mklogdir() {
    out=/
    while [ -e $out ]; do
	out=$1/`date +%Y_%m-%d_%H%M`.`hostname`
	sleep 1
    done
    mkdir --parents $out
    ( mkcurrent `dirname $out` )
    
    echo $out
}

dir=`mklogdir $NYCTRAFFICLOG/characterise/frequency`
python3 $NYCTRAFFIC/src/characterise/frequency.py \
    	--prediction-window 10 \
    	--target-window 10 \
    	--speed-threshold -0.002 \
	--reporting-threshold 90 \
        --figure-directory $dir \
        --pickle $dir/observations.pkl &> \
        $dir/log
