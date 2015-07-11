#!/bin/bash

for i in 3; do
    dir=data/fig/clusters/$i
    pkl=$dir/observations.pkl

    mkdir --parents $dir
    ./pandas/cluster.py \
	--observation-window 10 \
	--prediction-window $i \
	--target-window 5 \
	--speed-threshold -0.002 \
	--clusters 3 \
	--fig-directory $dir \
	--pickle $pkl > \
	/dev/null

    for j in `seq 2 7`; do
	./pandas/cluster.py --resume $pkl --clusters $j > $dir/dat-$j
    done
done
