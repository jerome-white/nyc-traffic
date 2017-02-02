#!/bin/bash

#PBS -V
#PBS -l nodes=1:ppn=20,mem=60GB,walltime=2:00:00
#PBS -m abe
#PBS -M jsw7@nyu.edu
#PBS -N heatmap
#PBS -j oe

python $HOME/src/nyc-traffic/src/visualization/plots/heatmap.py \
    --data $SCRATCH/nyc/classify/2017_0122_071500 \
    --output $TMPDIR/hm.png

# leave a blank line at the end
