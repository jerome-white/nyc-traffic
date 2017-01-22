#!/bin/bash

while getopts "n:d:w:o:l:h" OPTION; do
    case $OPTION in
        n) nodes=$OPTARG ;;
	d) duration=$OPTARG ;;
	w) observations=$OPTARG ;;
	o) offset=$OPTARG ;;
	l) ledger="--ledger $OPTARG" ;;
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

for i in `seq $nodes`; do
    tmp=`mktemp`
    ( >&2 echo "[ `date` ] $i $tmp" )
cat <<EOF > $tmp
python $HOME/src/nyc-traffic/src/characterise/classify.py $ledger \
  --data $SCRATCH/nyc/data \
  --output $SCRATCH/nyc/classify \
  --max-observations $observations \
  --max-offset $offset \
  --node `expr $i - 1` \
  --total-nodes $nodes
EOF
    qsub \
	-j oe \
	-l nodes=1:ppn=20,mem=60GB,walltime=${duration}:00:00 \
	-m abe \
	-M jsw7@nyu.edu \
	-N nyc-classify \
	-V \
	$tmp
done > jobs

# leave a blank line at the end
