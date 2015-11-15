#!/bin/bash

sep=:
pth=$NYCTRAFFIC/src/cluster

while getopts "xp" OPTION; do
    case $OPTION in
        x) execute=1 ;;
	p) plot=1 ;;
        *) exit 1 ;;
    esac
done

# build an array of cluster combinations beforehand (since some
# combinations won't make sense if blindly added to a loop)
# for owindow in 10; do
#     for pwindow in `seq 3 3 12`; do
# 	for twindow in 5; do
# 	    for thresh in -0.002; do
# 		c="$owindow $pwindow $twindow $thresh"
# 		combo=( ${combo[@]}  `sed -e"s/ /$sep/g" <<< $c` )
# 	    done
# 	done
#     done
# done
combo=(
    10:6:5:-0.002:120
    # 15:15:3:-0.002:120
    )

# cluster for each combination
for i in ${combo[@]}; do
    c=( `sed -e"s/$sep/ /g" <<< $i` )
    echo "[`date`] ${c[@]}"
    
    dir=$NYCTRAFFICLOG/cluster/`sed -e's/ /\//g' <<< ${c[@]}`
    mkdir --parents $dir
    
    pkl=$dir/observations.pkl
    
    if [ $execute ]; then
	python3 $pth/cluster.py \
    	    --observation-window ${c[0]} \
    	    --prediction-window ${c[1]} \
    	    --target-window ${c[2]} \
    	    --speed-threshold ${c[3]} \
	    --reporting-threshold ${c[4]} \
    	    --pickle $pkl
    fi

    if [ $plot ]; then
	# for j in `seq 2 9`; do
	for j in 0; do
    	    python3 $pth/cluster.py \
		    --observation-window ${c[0]} \
    		    --prediction-window ${c[1]} \
    		    --target-window ${c[2]} \
    		    --speed-threshold ${c[3]} \
		    --reporting-threshold ${c[4]} \
		    --resume $pkl \
		    --figure-directory $dir \
		    --clusters $j > \
		    $dir/cluster-$j.csv || \
		exit
	done
    fi
done 2> $pth/log
