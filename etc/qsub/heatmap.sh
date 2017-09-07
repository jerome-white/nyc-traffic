#!/bin/bash

output=$SCRATCH/nyc/heatmap
rm --recursive --force $output
mkdir $output

for i in $SCRATCH/nyc/classify/*; do
    j=`basename $i`
    tmp=`mktemp`

    cat <<EOF > $tmp
python $HOME/src/nyc-traffic/src/visualization/plots/heatmap.py \
       --data $i \
       --output $output/`$j`.png \
       --save-data $output/`$j`.csv
EOF
    sbatch \
	--mem=60GB \
	--time=2:00:00 \
	--nodes=1 \
	--cpus-per-task=20 \
	--job-name heatmap-$j \
	$tmp
done > jobs
