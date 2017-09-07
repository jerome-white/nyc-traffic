#!/bin/bash

while getopts "a:n:d:w:o:l:e:f:h" OPTION; do
    case $OPTION in
        n) nodes=$OPTARG ;;
	d) duration=$OPTARG ;;
	w) observations=$OPTARG ;;
	o) offset=$OPTARG ;;
	e) output=$OPTARG ;;
	a) alpha=$OPTARG ;;
	f) frequency="--frequency $OPTARG" ;;
	h)
	    cat<<EOF
$0 [options]
     -n nodes
     -d duration (hours)
EOF
	    exit
	    ;;
        *) exit 1 ;;
    esac
done

if [ ! $output ]; then
    output=$SCRATCH/nyc/classify/`date +'%Y_%m%d_%H%M%S'`
fi

#
#
#
if [ $alpha ]; then
    output=$output-$alpha
    alpha="--alpha $alpha"
fi
ledger=$output/.ledger
mkdir --parents $ledger

#
#
#
for i in `seq $nodes`; do
    tmp=`mktemp`
    ( >&2 echo "[ `date` ] $i $tmp" )

    echo -n "$i "
    cat <<EOF > $tmp
python $HOME/src/nyc-traffic/src/characterise/classify.py $frequency $alpha \
  --data $SCRATCH/nyc/data \
  --output $output \
  --ledger $ledger \
  --max-observations $observations \
  --max-offset $offset \
  --node `expr $i - 1` \
  --total-nodes $nodes
EOF
    sbatch \
	--mem=60GB \
	--time=${duration}:00:00 \
	--nodes=1 \
	--cpus-per-task=20 \
	--job-name=classify-$i \
	$tmp
done > jobs
