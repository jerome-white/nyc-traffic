#!/bin/bash

output=$SCRATCH/nyc/classify/`date +'%Y_%m%d_%H%M%S'`
while getopts "a:n:d:w:o:l:e:f:h" OPTION; do
    case $OPTION in
        n) nodes=$OPTARG ;;
	d) duration=$OPTARG ;;
	w) observations=$OPTARG ;;
	o) offset=$OPTARG ;;
	l) ledger=$OPTARG ;;
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

if [ $alpha ]; then
    output=$output-$alpha
    alpha="--alpha $alpha"
fi

if [ ! $ledger ]; then
    ledger=`mktemp --directory`
fi

for i in `seq $nodes`; do
    tmp=`mktemp`
    ( >&2 echo "[ `date` ] $i $tmp" )
cat <<EOF > $tmp
python $HOME/src/nyc-traffic/src/characterise/frequency.py $frequency $alpha \
  --data $SCRATCH/nyc/data \
  --output $output \
  --ledger $ledger \
  --max-observations $observations \
  --max-offset $offset \
  --node `expr $i - 1` \
  --total-nodes $nodes
EOF
    qsub \
	-j oe \
	-l nodes=1:ppn=20,mem=60GB,walltime=${duration}:00:00 \
	-N nyc-classify \
	-V \
	$tmp
done > jobs

# leave a blank line at the end
