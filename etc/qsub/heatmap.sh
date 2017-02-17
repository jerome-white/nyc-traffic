#!/bin/bash

output=$TMPDIR/heatmap
rm --recursive --force $output
mkdir $output

for i in $SCRATCH/nyc/classify/*; do
    qsub=`mktemp`
    cat <<EOF > $qsub
python $HOME/src/nyc-traffic/src/visualization/plots/heatmap.py \
       --data $i \
       --output $output/`basename $i`.png \
       --save-data $output/`basename $i`.csv
EOF
    qsub \
	-j oe \
	-l nodes=1:ppn=20,mem=60GB,walltime=2:00:00 \
	-N heatmap \
	-V \
	$qsub
done > jobs

# leave a blank line at the end
