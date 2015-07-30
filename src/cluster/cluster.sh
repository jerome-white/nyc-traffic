#!/bin/bash

sep=:
pth=$NYCTRAFFIC/src/cluster
mkdir --parents $pth/var

# build an array of cluster combinations beforehand (since some
# combinations won't make sense if blindly added to a loop)
for owindow in 10; do
    for pwindow in `seq 3 3 12`; do
	for twindow in 5; do
	    for thresh in -0.002; do
		c="$owindow $pwindow $twindow $thresh"
		combo=( ${combo[@]}  `sed -e"s/ /$sep/g" <<< $c` )
	    done
	done
    done
done

# cluster for each combination
for i in ${combo[@]}; do
    c=( `sed -e"s/$sep/ /g" <<< $i` )
    echo ${c[@]}
    
    dir=$pth/var/`sed -e's/ /\//g' <<< ${c[@]}`
    pkl=$dir/observations.pkl
    
    mkdir --parents $dir

    python3 $pth/cluster.py \
    	    --observation-window ${c[0]} \
    	    --prediction-window ${c[1]} \
    	    --target-window ${c[2]} \
    	    --speed-threshold ${c[3]} \
    	    --pickle $pkl

    for j in `seq 2 9`; do
    	python3 $pth/cluster.py \
		--resume $pkl \
		--fig-directory $dir \
		--clusters $j > \
		$dir/dat-$j
    done
done 2> $pth/log
