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

dir=`mklogdir $NYCTRAFFICLOG/characterise`
# dir=$NYCTRAFFICLOG/characterise/2015_11-06_0340.samjam.local
pkl=$dir/observations.pkl

python3 $NYCTRAFFIC/src/characterise/characterise.py \
    	--prediction-window 11 \
    	--target-window 11 \
    	--speed-threshold -0.002 \
	--reporting-threshold 120 \
	--figure-directory $dir \
 	--pickle $pkl &> $dir/log
#	--resume $pkl

